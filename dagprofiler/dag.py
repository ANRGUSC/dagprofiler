"""
DAG executor with automatic profiling.

Provides a generic, declarative approach to DAG execution
with built-in instrumentation and metric collection.
"""

import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import numpy as np

from dagprofiler.config import Config
from dagprofiler.metrics import DAGMetrics, EdgeMetrics, TaskMetrics
from dagprofiler.profile import Profile
from dagprofiler.task import Task


class DAG:
    """
    DAG executor with automatic profiling.

    Executes tasks in topological order and automatically measures:
    - Node weights (instruction estimates from task.estimate_instructions())
    - Edge weights (actual bytes transferred between tasks)
    - Execution metrics (time, bytes in/out per task)

    Example:
        dag = DAG(
            tasks={
                "T0": CollectState(),
                "T1": Predict(),
                "T2": Schedule(),
            },
            edges=[
                ("T0", "T1"),  # Auto-routes matching outputs->inputs
                ("T0", "T2"),
                ("T1", "T2"),
            ],
            config=Config(N_UE=10000, N_CELL=7)
        )

        profile = dag.run()
        profile.save("output.json")
    """

    def __init__(
        self,
        tasks: Dict[str, Task],
        edges: List[Union[Tuple[str, str], Tuple[str, str, List[str]]]],
        config: Optional[Config] = None,
    ):
        """
        Initialize the DAG.

        Args:
            tasks: Dictionary mapping task names to Task instances
            edges: List of edges, each as:
                   - (source, target): auto-route matching outputs->inputs
                   - (source, target, fields): explicit data fields to route
            config: Global configuration for all tasks
        """
        self.tasks = tasks
        self.config = config or Config()

        # Parse edges
        self.edges: Dict[str, List[str]] = defaultdict(list)  # src -> [dst, ...]
        self.edge_fields: Dict[Tuple[str, str], List[str]] = {}  # (src, dst) -> [fields]

        for edge in edges:
            if len(edge) == 2:
                src, dst = edge
                fields = None  # Auto-detect
            else:
                src, dst, fields = edge

            unknown = [node for node in (src, dst) if node not in tasks]
            if unknown:
                raise ValueError(
                    f"Edge references unknown task(s): {sorted(unknown)}. "
                    f"Known tasks: {sorted(tasks.keys())}"
                )

            self.edges[src].append(dst)
            if fields:
                self.edge_fields[(src, dst)] = fields

        # Compute reverse edges
        self.reverse_edges: Dict[str, List[str]] = defaultdict(list)
        for src, dsts in self.edges.items():
            for dst in dsts:
                self.reverse_edges[dst].append(src)

        # Compute execution order
        self.execution_order = self._topological_sort()

        # Auto-detect edge fields if not specified
        self._resolve_edge_fields()

        # Set config on all tasks
        for task in self.tasks.values():
            task.config = self.config

    def _topological_sort(self) -> List[str]:
        """Compute topological order of tasks using Kahn's algorithm."""
        in_degree = {task: 0 for task in self.tasks}
        for dsts in self.edges.values():
            for dst in dsts:
                in_degree[dst] += 1

        queue = [task for task, degree in in_degree.items() if degree == 0]
        order = []

        while queue:
            queue.sort()  # Deterministic ordering
            task = queue.pop(0)
            order.append(task)

            for downstream in self.edges.get(task, []):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(order) != len(self.tasks):
            raise ValueError("DAG contains a cycle!")

        return order

    def _resolve_edge_fields(self) -> None:
        """Auto-detect which fields to route on each edge."""
        for src, dsts in self.edges.items():
            src_task = self.tasks[src]
            src_outputs = set(src_task.outputs)

            for dst in dsts:
                if (src, dst) in self.edge_fields:
                    continue  # Already specified

                dst_task = self.tasks[dst]
                dst_inputs = set(dst_task.inputs)

                # Find matching fields
                matching = src_outputs & dst_inputs
                if matching:
                    self.edge_fields[(src, dst)] = sorted(matching)
                else:
                    # No matching names - route all outputs
                    self.edge_fields[(src, dst)] = sorted(src_outputs)

    def _compute_instruction_estimates(self) -> Dict[str, int]:
        """Compute instruction estimates for all tasks."""
        estimates = {}
        for name, task in self.tasks.items():
            estimates[name] = task.estimate_instructions(self.config)
        return estimates

    def _route_inputs(
        self,
        task_name: str,
        outputs: Dict[str, Dict[str, Any]],
    ) -> Dict[str, bytes]:
        """
        Route upstream outputs to task inputs.

        Args:
            task_name: Target task name
            outputs: Dict of all upstream task raw outputs

        Returns:
            Serialized inputs for the task
        """
        task = self.tasks[task_name]
        routed = {}

        for upstream in self.reverse_edges.get(task_name, []):
            fields = self.edge_fields.get((upstream, task_name), [])
            upstream_out = outputs.get(upstream, {})

            for field in fields:
                if field in upstream_out:
                    routed[field] = task.serialize(upstream_out[field])

        return routed

    def _compute_edge_sizes(
        self,
        task_name: str,
        outputs: Dict[str, Dict[str, Any]],
    ) -> List[EdgeMetrics]:
        """Compute edge data sizes for incoming edges."""
        edge_metrics = []

        for upstream in self.reverse_edges.get(task_name, []):
            fields = self.edge_fields.get((upstream, task_name), [])
            upstream_out = outputs.get(upstream, {})

            total_bytes = 0
            for field in fields:
                if field in upstream_out:
                    data = upstream_out[field]
                    if isinstance(data, np.ndarray):
                        total_bytes += data.nbytes
                    else:
                        total_bytes += len(Task.serialize(data))

            edge_metrics.append(EdgeMetrics(
                source=upstream,
                target=task_name,
                data_fields=fields,
                size_bytes=total_bytes,
            ))

        return edge_metrics

    def run(self, seed: Optional[int] = None) -> Profile:
        """
        Execute the full DAG with profiling.

        Args:
            seed: Optional random seed for reproducibility

        Returns:
            Profile object containing all execution metrics
        """
        if seed is not None:
            np.random.seed(seed)

        start_time = time.perf_counter()

        # Store raw outputs from each task (for edge sizing)
        outputs: Dict[str, Dict[str, Any]] = {}

        # Collect metrics
        task_metrics: List[TaskMetrics] = []
        edge_metrics: List[EdgeMetrics] = []

        # Execute tasks in topological order
        for task_name in self.execution_order:
            task = self.tasks[task_name]

            # Compute edge sizes before routing
            incoming_edges = self._compute_edge_sizes(task_name, outputs)
            edge_metrics.extend(incoming_edges)

            # Route inputs from upstream tasks
            inputs = self._route_inputs(task_name, outputs)

            # Execute task
            result = task.run(inputs, self.config)

            # Store metrics
            task_metrics.append(result["metrics"])

            # Store raw outputs for downstream tasks
            outputs[task_name] = result["outputs_raw"]

        end_time = time.perf_counter()
        total_time_ms = (end_time - start_time) * 1000.0

        # Compute node weights (instruction estimates)
        node_weights = self._compute_instruction_estimates()

        # Convert edge metrics to edge_weights dict
        edge_weights = {
            f"{e.source}->{e.target}": e.size_bits
            for e in edge_metrics
        }

        # Build DAGMetrics
        dag_metrics = DAGMetrics(
            task_metrics=task_metrics,
            edge_metrics=edge_metrics,
            node_weights=node_weights,
            edge_weights=edge_weights,
            total_time_ms=total_time_ms,
        )

        # Build Profile
        return Profile(
            config=self.config,
            dag_structure={
                "nodes": list(self.tasks.keys()),
                "edges": [
                    {"source": src, "target": dst}
                    for src, dsts in self.edges.items()
                    for dst in dsts
                ],
                "execution_order": self.execution_order,
            },
            metrics=dag_metrics,
            final_outputs=outputs.get(self.execution_order[-1], {}),
            seed=seed,
        )

    @classmethod
    def from_yaml(cls, path: str) -> "DAG":
        """
        Load DAG definition from a YAML file.

        Args:
            path: Path to YAML configuration file

        Returns:
            Configured DAG instance
        """
        import importlib
        import yaml

        with open(path) as f:
            spec = yaml.safe_load(f)

        # Load config
        config = Config(**spec.get("config", {}))

        # Load tasks
        tasks = {}
        for name, task_spec in spec.get("tasks", {}).items():
            module = importlib.import_module(task_spec["module"])
            task_class = getattr(module, task_spec["class"])
            tasks[name] = task_class()

        # Load edges
        edges = []
        for edge in spec.get("edges", []):
            if isinstance(edge, list):
                edges.append(tuple(edge))
            elif isinstance(edge, dict):
                src, dst = edge["source"], edge["target"]
                if "data" in edge:
                    edges.append((src, dst, edge["data"]))
                else:
                    edges.append((src, dst))

        return cls(tasks=tasks, edges=edges, config=config)

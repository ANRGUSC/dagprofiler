"""
Decorator-based task definition for simple use cases.

Allows defining tasks with minimal boilerplate:

    @task(inputs=["x"], outputs=["y"])
    def my_task(x, cfg):
        return {"y": process(x)}
"""

from functools import wraps
from typing import Any, Callable, Dict, List, Optional

from dagprofiler.config import Config
from dagprofiler.task import Task


def task(
    inputs: List[str],
    outputs: List[str],
    config_deps: Optional[List[str]] = None,
    estimate_fn: Optional[Callable[[Config], int]] = None,
):
    """
    Decorator to create a Task from a function.

    Args:
        inputs: List of input field names
        outputs: List of output field names
        config_deps: Optional list of config parameters used in estimation
        estimate_fn: Optional function to estimate instruction count

    Returns:
        Decorated function wrapped as a Task class

    Example:
        @task(inputs=["data"], outputs=["result"])
        def process(data, cfg):
            return {"result": data * 2}

        # Creates a Task subclass named "process"
        # that can be used in a DAG
    """
    _inputs = list(inputs)
    _outputs = list(outputs)
    _config_deps = config_deps or []
    _estimate_fn = estimate_fn

    def decorator(fn: Callable) -> type:
        class FunctionTask(Task):
            __doc__ = fn.__doc__

            # Set class attributes
            inputs = _inputs
            outputs = _outputs
            config_deps = _config_deps

            def __init__(self, name: Optional[str] = None, config: Optional[Config] = None):
                super().__init__(name=name or fn.__name__, config=config)
                self._fn = fn

            def compute(self, cfg: Optional[Config] = None, **kwargs: Any) -> Dict[str, Any]:
                return self._fn(cfg=cfg, **kwargs)

            @staticmethod
            def estimate_instructions(cfg: Config) -> int:
                if _estimate_fn is not None:
                    return _estimate_fn(cfg)
                return 0

        # Name the class after the function
        FunctionTask.__name__ = fn.__name__
        FunctionTask.__qualname__ = fn.__name__

        return FunctionTask

    return decorator


def dag(
    tasks: List[type],
    config: Optional[Config] = None,
    edges: Optional[List] = None,
):
    """
    Create a DAG from a list of task classes with automatic edge inference.

    Args:
        tasks: List of Task classes (from @task decorator or subclasses)
        config: Configuration object
        edges: Optional explicit edges; if None, inferred from I/O names

    Returns:
        DAG instance ready to run

    Example:
        @task(inputs=[], outputs=["data"])
        def source(cfg):
            return {"data": generate()}

        @task(inputs=["data"], outputs=["result"])
        def process(data, cfg):
            return {"result": transform(data)}

        my_dag = dag(tasks=[source, process], config=Config(N=100))
        profile = my_dag.run()
    """
    from dagprofiler.dag import DAG

    # Instantiate tasks
    task_instances = {}
    for task_class in tasks:
        name = task_class.__name__
        task_instances[name] = task_class(name=name, config=config)

    # Infer edges if not provided
    if edges is None:
        edges = []

        # Build output -> task name mapping
        output_providers: Dict[str, str] = {}
        for name, t in task_instances.items():
            for output in t.outputs:
                output_providers[output] = name

        # Find edges by matching inputs to outputs
        for name, t in task_instances.items():
            for input_name in t.inputs:
                if input_name in output_providers:
                    src = output_providers[input_name]
                    if src != name:  # No self-loops
                        edge = (src, name)
                        if edge not in edges:
                            edges.append(edge)

    return DAG(tasks=task_instances, edges=edges, config=config)

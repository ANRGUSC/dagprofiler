"""
Profile storage and comparison.

Handles saving/loading execution profiles to JSON format.
"""

import json
import socket
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dagprofiler.config import Config
from dagprofiler.metrics import DAGMetrics


@dataclass
class Profile:
    """
    Execution profile for a DAG run.

    Contains all information needed for scheduler consumption:
    - Configuration parameters
    - DAG structure
    - Node weights (instruction estimates)
    - Edge weights (data transfer sizes)
    - Execution metrics
    """

    config: Config
    dag_structure: Dict[str, Any]
    metrics: DAGMetrics
    final_outputs: Dict[str, Any]
    seed: Optional[int] = None
    timestamp: Optional[str] = None
    hostname: Optional[str] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
        if self.hostname is None:
            self.hostname = socket.gethostname()

    def save(self, path: Union[str, Path]) -> str:
        """
        Save profile to JSON file.

        Args:
            path: Output file path

        Returns:
            Path to saved file
        """
        path = Path(path)

        # Build JSON structure
        data = {
            "metadata": {
                "timestamp": self.timestamp,
                "dagprofiler_version": "0.1.0",
                "seed": self.seed,
                "hostname": self.hostname,
            },
            "configuration": self.config.to_dict(),
            "dag_structure": self.dag_structure,
            "node_weights": self.metrics.node_weights,
            "edge_weights": self.metrics.edge_weights,
            "execution_metrics": [m.to_dict() for m in self.metrics.task_metrics],
            "total_time_ms": self.metrics.total_time_ms,
        }

        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=self._json_default)

        return str(path)

    @staticmethod
    def _json_default(obj: Any) -> Any:
        """Handle non-JSON-serializable types."""
        import numpy as np

        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif hasattr(obj, "to_dict"):
            return obj.to_dict()
        else:
            return str(obj)

    @classmethod
    def load(cls, path: Union[str, Path]) -> "Profile":
        """
        Load profile from JSON file.

        Args:
            path: Input file path

        Returns:
            Profile instance
        """
        from dagprofiler.metrics import EdgeMetrics, TaskMetrics

        with open(path) as f:
            data = json.load(f)

        config = Config(**data["configuration"])

        # Reconstruct task metrics
        task_metrics = [
            TaskMetrics(**m) for m in data.get("execution_metrics", [])
        ]

        # Reconstruct edge metrics from edge_weights
        edge_metrics = []
        for edge_key, bits in data.get("edge_weights", {}).items():
            src, dst = edge_key.split("->")
            edge_metrics.append(EdgeMetrics(
                source=src,
                target=dst,
                data_fields=[],  # Unknown from saved profile
                size_bytes=bits // 8,
            ))

        dag_metrics = DAGMetrics(
            task_metrics=task_metrics,
            edge_metrics=edge_metrics,
            node_weights=data.get("node_weights", {}),
            edge_weights=data.get("edge_weights", {}),
            total_time_ms=data.get("total_time_ms", 0),
        )

        return cls(
            config=config,
            dag_structure=data.get("dag_structure", {}),
            metrics=dag_metrics,
            final_outputs={},
            seed=data.get("metadata", {}).get("seed"),
            timestamp=data.get("metadata", {}).get("timestamp"),
            hostname=data.get("metadata", {}).get("hostname"),
        )

    def compare(self, other: "Profile") -> Dict[str, Any]:
        """
        Compare this profile with another.

        Args:
            other: Another Profile to compare against

        Returns:
            Dictionary with comparison results
        """
        comparison = {
            "config_diff": {},
            "node_weight_diff": {},
            "edge_weight_diff": {},
            "time_diff_ms": self.metrics.total_time_ms - other.metrics.total_time_ms,
        }

        # Compare configs
        for key in set(self.config) | set(other.config):
            v1 = self.config.get(key)
            v2 = other.config.get(key)
            if v1 != v2:
                comparison["config_diff"][key] = {"self": v1, "other": v2}

        # Compare node weights
        for task in set(self.metrics.node_weights) | set(other.metrics.node_weights):
            w1 = self.metrics.node_weights.get(task, 0)
            w2 = other.metrics.node_weights.get(task, 0)
            if w1 != w2:
                comparison["node_weight_diff"][task] = {
                    "self": w1,
                    "other": w2,
                    "diff": w1 - w2,
                    "pct": (w1 - w2) / w2 * 100 if w2 else float("inf"),
                }

        # Compare edge weights
        for edge in set(self.metrics.edge_weights) | set(other.metrics.edge_weights):
            w1 = self.metrics.edge_weights.get(edge, 0)
            w2 = other.metrics.edge_weights.get(edge, 0)
            if w1 != w2:
                comparison["edge_weight_diff"][edge] = {
                    "self": w1,
                    "other": w2,
                    "diff": w1 - w2,
                    "pct": (w1 - w2) / w2 * 100 if w2 else float("inf"),
                }

        return comparison

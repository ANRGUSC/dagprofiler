"""
Dataclasses for profiling metrics.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TaskMetrics:
    """Metrics for a single task execution."""

    task: str
    compute_time_ms: float
    serialize_time_ms: float
    deserialize_time_ms: float
    bytes_in: int
    bytes_out: int
    loop_idx: int = 0

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "task": self.task,
            "compute_time_ms": self.compute_time_ms,
            "serialize_time_ms": self.serialize_time_ms,
            "deserialize_time_ms": self.deserialize_time_ms,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
        }


@dataclass
class EdgeMetrics:
    """Metrics for data transferred on a DAG edge."""

    source: str
    target: str
    data_fields: List[str]  # Which output fields are transferred
    size_bytes: int
    size_bits: int = field(init=False)

    def __post_init__(self):
        self.size_bits = self.size_bytes * 8

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "source": self.source,
            "target": self.target,
            "data_fields": self.data_fields,
            "size_bytes": self.size_bytes,
            "size_bits": self.size_bits,
        }


@dataclass
class DAGMetrics:
    """Aggregate metrics for a DAG execution."""

    task_metrics: List[TaskMetrics]
    edge_metrics: List[EdgeMetrics]
    node_weights: Dict[str, int]  # task -> instruction estimate
    edge_weights: Dict[str, int]  # "src->dst" -> bits
    total_time_ms: float

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "task_metrics": [m.to_dict() for m in self.task_metrics],
            "edge_metrics": [e.to_dict() for e in self.edge_metrics],
            "node_weights": self.node_weights,
            "edge_weights": self.edge_weights,
            "total_time_ms": self.total_time_ms,
        }

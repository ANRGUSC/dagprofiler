"""
Abstract base class for DAG tasks.

Provides a generic task framework with:
- Explicit inputs/outputs declaration
- Automatic serialization and timing
- Optional instruction estimation hook
"""

import pickle
import time
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, List, Optional, Type

import numpy as np

from dagprofiler.config import Config
from dagprofiler.metrics import TaskMetrics


class Task(ABC):
    """
    Abstract base class for all DAG tasks.

    Subclasses must:
    1. Declare `inputs` and `outputs` class attributes
    2. Implement `compute()` method

    Subclasses may optionally:
    - Override `estimate_instructions()` for cost modeling
    - Override `serialize()` / `deserialize()` for custom serialization

    Example:
        class MyTask(Task):
            inputs = ["data_in"]
            outputs = ["data_out"]
            config_deps = ["N"]  # Optional: config params used in estimation

            def compute(self, data_in: np.ndarray, cfg: Config) -> Dict[str, Any]:
                result = process(data_in)
                return {"data_out": result}

            @staticmethod
            def estimate_instructions(cfg: Config) -> int:
                return cfg.N * 100
    """

    # Class attributes to be overridden by subclasses
    inputs: ClassVar[List[str]] = []
    outputs: ClassVar[List[str]] = []
    config_deps: ClassVar[List[str]] = []  # Config params used in estimation

    def __init__(self, name: Optional[str] = None, config: Optional[Config] = None):
        """
        Initialize the task.

        Args:
            name: Optional task name (defaults to class name)
            config: Optional configuration object
        """
        self.name = name or self.__class__.__name__
        self.config = config

    def run(self, inputs: Dict[str, bytes], config: Optional[Config] = None) -> Dict[str, Any]:
        """
        Execute the task with full instrumentation.

        Steps (in order):
        1. Deserialize inputs
        2. Perform computation
        3. Serialize outputs

        Args:
            inputs: Dictionary with serialized input data (bytes)
            config: Optional config override for this run

        Returns:
            {
                "outputs": dict mapping output names to serialized bytes,
                "outputs_raw": dict mapping output names to raw data (for edge sizing),
                "metrics": TaskMetrics dataclass
            }
        """
        cfg = config or self.config

        # Measure bytes in
        bytes_in = self._count_bytes(inputs)

        # Step 1: Deserialize inputs
        t0 = time.perf_counter()
        deserialized = self._deserialize_all(inputs)
        t1 = time.perf_counter()
        deserialize_time_ms = (t1 - t0) * 1000.0

        # Step 2: Compute
        t0 = time.perf_counter()
        computed = self.compute(**deserialized, cfg=cfg)
        t1 = time.perf_counter()
        compute_time_ms = (t1 - t0) * 1000.0

        # Validate outputs
        self._validate_outputs(computed)

        # Step 3: Serialize outputs
        t0 = time.perf_counter()
        serialized = self._serialize_all(computed)
        t1 = time.perf_counter()
        serialize_time_ms = (t1 - t0) * 1000.0

        # Measure bytes out
        bytes_out = self._count_bytes(serialized)

        metrics = TaskMetrics(
            task=self.name,
            compute_time_ms=compute_time_ms,
            serialize_time_ms=serialize_time_ms,
            deserialize_time_ms=deserialize_time_ms,
            bytes_in=bytes_in,
            bytes_out=bytes_out,
        )

        return {
            "outputs": serialized,
            "outputs_raw": computed,
            "metrics": metrics,
        }

    @abstractmethod
    def compute(self, cfg: Optional[Config] = None, **inputs: Any) -> Dict[str, Any]:
        """
        Perform the actual task computation.

        Args:
            cfg: Configuration object
            **inputs: Deserialized input data (keyword args match `inputs` declaration)

        Returns:
            Dictionary mapping output names to computed data
        """
        pass

    @staticmethod
    def estimate_instructions(cfg: Config) -> int:
        """
        Estimate instruction count for this task.

        Override this method to provide a parameterized cost model.

        Args:
            cfg: Configuration object with scale parameters

        Returns:
            Estimated instruction count (integer)
        """
        return 0  # Default: unknown cost

    def _deserialize_all(self, inputs: Dict[str, bytes]) -> Dict[str, Any]:
        """Deserialize all inputs."""
        result = {}
        for name, data in inputs.items():
            if isinstance(data, bytes):
                result[name] = self.deserialize(data)
            else:
                result[name] = data  # Already deserialized
        return result

    def _serialize_all(self, outputs: Dict[str, Any]) -> Dict[str, bytes]:
        """Serialize all outputs."""
        return {name: self.serialize(data) for name, data in outputs.items()}

    def _validate_outputs(self, outputs: Dict[str, Any]) -> None:
        """Validate that all declared outputs are present."""
        missing = set(self.outputs) - set(outputs.keys())
        if missing:
            raise ValueError(
                f"Task {self.name} missing outputs: {missing}. "
                f"Expected: {self.outputs}, got: {list(outputs.keys())}"
            )

    @staticmethod
    def serialize(data: Any) -> bytes:
        """
        Serialize data to bytes.

        Override for custom serialization (e.g., protobuf, msgpack).

        Args:
            data: Data to serialize (numpy array, dict, etc.)

        Returns:
            Serialized bytes
        """
        return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize(data: bytes) -> Any:
        """
        Deserialize bytes to data.

        Override for custom deserialization.

        Args:
            data: Serialized bytes

        Returns:
            Deserialized data
        """
        return pickle.loads(data)

    @staticmethod
    def _count_bytes(data: Dict[str, Any]) -> int:
        """Count total bytes in a dictionary of data."""
        total = 0
        for value in data.values():
            if isinstance(value, bytes):
                total += len(value)
            elif isinstance(value, np.ndarray):
                total += value.nbytes
        return total

"""Tests for the Task base class."""

import numpy as np
import pytest

from dagprofiler import Config, Task


class SimpleTask(Task):
    """A simple test task."""

    inputs = ["x"]
    outputs = ["y"]
    config_deps = ["scale"]

    def compute(self, x, cfg=None):
        scale = cfg.scale if cfg else 1.0
        return {"y": x * scale}

    @staticmethod
    def estimate_instructions(cfg):
        return cfg.get("N", 100) * 10


class TestTask:
    def test_task_creation(self):
        task = SimpleTask()
        assert task.name == "SimpleTask"
        assert task.inputs == ["x"]
        assert task.outputs == ["y"]

    def test_task_run(self):
        task = SimpleTask()
        cfg = Config(scale=2.0)

        x = np.array([1.0, 2.0, 3.0])
        inputs = {"x": Task.serialize(x)}

        result = task.run(inputs, cfg)

        assert "outputs" in result
        assert "metrics" in result

        y = Task.deserialize(result["outputs"]["y"])
        np.testing.assert_array_equal(y, x * 2.0)

    def test_task_metrics(self):
        task = SimpleTask()
        cfg = Config(scale=1.0)

        x = np.array([1.0, 2.0, 3.0])
        inputs = {"x": Task.serialize(x)}

        result = task.run(inputs, cfg)
        metrics = result["metrics"]

        assert metrics.task == "SimpleTask"
        assert metrics.compute_time_ms >= 0
        assert metrics.bytes_in > 0
        assert metrics.bytes_out > 0

    def test_estimate_instructions(self):
        cfg = Config(N=1000)
        estimate = SimpleTask.estimate_instructions(cfg)
        assert estimate == 10000


class SourceTask(Task):
    """Task with no inputs."""

    inputs = []
    outputs = ["data"]

    def compute(self, cfg=None):
        n = cfg.N if cfg else 10
        return {"data": np.random.randn(n)}


class TestSourceTask:
    def test_no_inputs(self):
        task = SourceTask()
        cfg = Config(N=100)

        result = task.run({}, cfg)

        data = Task.deserialize(result["outputs"]["data"])
        assert data.shape == (100,)

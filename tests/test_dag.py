"""Tests for DAG execution."""

import numpy as np
import pytest

from dagprofiler import Config, DAG, Task


class T0(Task):
    inputs = []
    outputs = ["x", "y"]

    def compute(self, cfg=None):
        n = cfg.N if cfg else 10
        return {
            "x": np.random.randn(n),
            "y": np.random.randn(n),
        }

    @staticmethod
    def estimate_instructions(cfg):
        return cfg.N * 10


class T1(Task):
    inputs = ["x"]
    outputs = ["z"]

    def compute(self, x, cfg=None):
        return {"z": x * 2}

    @staticmethod
    def estimate_instructions(cfg):
        return cfg.N * 5


class T2(Task):
    inputs = ["y", "z"]
    outputs = ["result"]

    def compute(self, y, z, cfg=None):
        return {"result": y + z}

    @staticmethod
    def estimate_instructions(cfg):
        return cfg.N * 3


class TestDAG:
    def test_simple_dag(self):
        dag = DAG(
            tasks={
                "T0": T0(),
                "T1": T1(),
                "T2": T2(),
            },
            edges=[
                ("T0", "T1"),
                ("T0", "T2"),
                ("T1", "T2"),
            ],
            config=Config(N=100),
        )

        assert dag.execution_order == ["T0", "T1", "T2"]

    def test_dag_run(self):
        dag = DAG(
            tasks={
                "T0": T0(),
                "T1": T1(),
                "T2": T2(),
            },
            edges=[
                ("T0", "T1"),
                ("T0", "T2"),
                ("T1", "T2"),
            ],
            config=Config(N=100),
        )

        profile = dag.run(seed=42)

        assert profile.metrics.total_time_ms > 0
        assert len(profile.metrics.task_metrics) == 3
        assert "T0" in profile.metrics.node_weights
        assert profile.metrics.node_weights["T0"] == 1000  # N * 10

    def test_edge_weights(self):
        dag = DAG(
            tasks={
                "T0": T0(),
                "T1": T1(),
            },
            edges=[("T0", "T1")],
            config=Config(N=100),
        )

        profile = dag.run()

        assert "T0->T1" in profile.metrics.edge_weights
        assert profile.metrics.edge_weights["T0->T1"] > 0


class TestDAGTopology:
    def test_cycle_detection(self):
        with pytest.raises(ValueError, match="cycle"):
            DAG(
                tasks={
                    "A": T0(),
                    "B": T1(),
                },
                edges=[("A", "B"), ("B", "A")],
            )

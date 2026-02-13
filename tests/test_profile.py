"""Tests for profile save/load."""

import json
import tempfile
from pathlib import Path

import numpy as np
import pytest

from dagprofiler import Config, DAG, Profile, Task


class SourceTask(Task):
    inputs = []
    outputs = ["data"]

    def compute(self, cfg=None):
        return {"data": np.array([1, 2, 3])}


class SinkTask(Task):
    inputs = ["data"]
    outputs = ["result"]

    def compute(self, data, cfg=None):
        return {"result": data.sum()}


class TestProfile:
    def test_save_load(self):
        dag = DAG(
            tasks={"source": SourceTask(), "sink": SinkTask()},
            edges=[("source", "sink")],
            config=Config(N=100),
        )

        profile = dag.run(seed=42)

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name

        try:
            profile.save(path)

            # Verify JSON is valid
            with open(path) as f:
                data = json.load(f)

            assert "metadata" in data
            assert "configuration" in data
            assert "node_weights" in data
            assert data["configuration"]["N"] == 100

            # Load and compare
            loaded = Profile.load(path)
            assert loaded.config.N == 100
            assert loaded.metrics.node_weights == profile.metrics.node_weights
        finally:
            Path(path).unlink()

    def test_compare(self):
        dag1 = DAG(
            tasks={"source": SourceTask(), "sink": SinkTask()},
            edges=[("source", "sink")],
            config=Config(N=100),
        )

        dag2 = DAG(
            tasks={"source": SourceTask(), "sink": SinkTask()},
            edges=[("source", "sink")],
            config=Config(N=200),
        )

        profile1 = dag1.run()
        profile2 = dag2.run()

        comparison = profile1.compare(profile2)

        assert "config_diff" in comparison
        assert "N" in comparison["config_diff"]

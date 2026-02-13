# dagprofiler

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18628153.svg)](https://doi.org/10.5281/zenodo.18628153)

A standard framework for authoring profiler-compatible DAG (Directed Acyclic Graph) tasks with automatic compute and communication profiling.

**[Read the full specification (PDF)](dagprofiler-specification.pdf)**

## Overview

`dagprofiler` is a standalone, pip-installable Python package that defines the **DAG Task Standard** — a contract between application developers, the profiler, and schedulers. It provides:

1. **A standard for authoring profiler-compatible DAG tasks** (the "DAG Task Standard")
2. **Per-task compute cost profiling** (instruction estimates)
3. **Per-edge communication volume measurement** (bytes/bits transferred)
4. **Reproducible profile storage** (JSON format with metadata)

Any application developer who follows the DAG Task Standard can automatically get profiling data that feeds into schedulers like SAGA — for any network type, not just 5G.

## Installation

`dagprofiler` is available on [PyPI](https://pypi.org/project/dagprofiler/):

```bash
pip install dagprofiler
```

Or install from source:

```bash
git clone https://github.com/ANRGUSC/dagprofiler.git
cd dagprofiler
pip install -e ".[dev]"
```

## Quick Start

### Define Tasks

```python
import numpy as np
from dagprofiler import Task, Config, DAG

class CollectData(Task):
    inputs = []
    outputs = ["data", "labels"]
    config_deps = ["N"]

    def compute(self, cfg=None):
        N = cfg.N
        return {
            "data": np.random.randn(N, 10),
            "labels": np.random.randint(0, 3, N),
        }

    @staticmethod
    def estimate_instructions(cfg):
        return cfg.N * 10

class ProcessData(Task):
    inputs = ["data"]
    outputs = ["features"]
    config_deps = ["N"]

    def compute(self, data, cfg=None):
        return {"features": data ** 2}

    @staticmethod
    def estimate_instructions(cfg):
        return cfg.N * 100

class Classify(Task):
    inputs = ["features", "labels"]
    outputs = ["predictions"]

    def compute(self, features, labels, cfg=None):
        return {"predictions": labels}  # Placeholder
```

### Build and Run a DAG

```python
dag = DAG(
    tasks={
        "collect": CollectData(),
        "process": ProcessData(),
        "classify": Classify(),
    },
    edges=[
        ("collect", "process"),   # Auto-routes matching outputs->inputs
        ("collect", "classify"),
        ("process", "classify"),
    ],
    config=Config(N=10000),
)

profile = dag.run(seed=42)
profile.save("profile.json")

print(f"Total time: {profile.metrics.total_time_ms:.2f} ms")
print(f"Node weights: {profile.metrics.node_weights}")
print(f"Edge weights: {profile.metrics.edge_weights}")
```

### Using the Decorator API

For simpler tasks, use the `@task` decorator:

```python
from dagprofiler import task
from dagprofiler.decorators import dag

@task(inputs=[], outputs=["data"])
def source(cfg):
    return {"data": np.random.randn(cfg.N)}

@task(inputs=["data"], outputs=["result"], estimate_fn=lambda cfg: cfg.N * 10)
def process(data, cfg):
    return {"result": data * 2}

my_dag = dag(tasks=[source, process], config=Config(N=100))
profile = my_dag.run()
```

## The DAG Task Standard

Any task following the standard must satisfy three requirements:

### 1. Declare I/O Schema

```python
class MyTask(Task):
    inputs = ["data_in"]          # Expected input field names
    outputs = ["data_out"]         # Produced output field names
    config_deps = ["N", "scale"]   # Config parameters used in estimation
```

### 2. Implement the Compute Interface

```python
def compute(self, data_in, cfg=None):
    result = process(data_in)
    return {"data_out": result}
```

### 3. Optionally Provide a Cost Model

```python
@staticmethod
def estimate_instructions(cfg):
    return cfg.N * 100  # O(N) cost model
```

## Profile JSON Format

The output is a scheduler-agnostic JSON file:

```json
{
  "metadata": {
    "timestamp": "2025-01-30T17:45:00Z",
    "dagprofiler_version": "0.1.0",
    "seed": 42,
    "hostname": "worker-01"
  },
  "configuration": {
    "N": 10000
  },
  "dag_structure": {
    "nodes": ["collect", "process", "classify"],
    "edges": [
      {"source": "collect", "target": "process"},
      {"source": "collect", "target": "classify"},
      {"source": "process", "target": "classify"}
    ],
    "execution_order": ["collect", "process", "classify"]
  },
  "node_weights": {
    "collect": 100000,
    "process": 1000000,
    "classify": 0
  },
  "edge_weights": {
    "collect->process": 6400000,
    "collect->classify": 640080,
    "process->classify": 6400000
  },
  "execution_metrics": [
    {
      "task": "collect",
      "compute_time_ms": 5.627,
      "serialize_time_ms": 0.129,
      "deserialize_time_ms": 0.0,
      "bytes_in": 0,
      "bytes_out": 600395
    }
  ],
  "total_time_ms": 45.2
}
```

## API Reference

### Core Classes

| Class | Description |
|-------|-------------|
| `Task` | Abstract base class for all DAG tasks |
| `Config` | Dynamic configuration container |
| `DAG` | DAG executor with automatic profiling |
| `Profile` | Profile storage, loading, and comparison |
| `TaskMetrics` | Per-task execution metrics |
| `EdgeMetrics` | Per-edge data transfer metrics |

### Config

```python
cfg = Config(N_UE=10000, N_CELL=7, N_SLICE=5)
cfg.N_UE                 # Attribute access
cfg["N_CELL"]            # Dictionary access
cfg.get("N_SLICE", 5)    # With default
cfg.to_dict()            # Export as dict
cfg.copy(N_UE=20000)     # Create modified copy
```

### Profile

```python
profile.save("output.json")          # Save to JSON
loaded = Profile.load("output.json") # Load from JSON
diff = profile.compare(other)        # Compare two profiles
```

## Testing

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## Design Principles

1. **Declarative over Procedural** — I/O declarations eliminate manual routing logic
2. **Generic not Domain-Specific** — Works for any DAG, any network, any domain
3. **Automatic Profiling** — Metrics collected without modifying application code
4. **Reproducible** — Profiles include seed, config, and timestamp for exact replay
5. **Scheduler-Agnostic** — Output format consumable by any scheduler
6. **Extensible** — Custom serialization, cost models, and task types supported
7. **Testable** — Comprehensive test suite included

Contributed by Yoonjae Hwang, Bhaskar Krishnamachari (USC), February 12, 2026

## License

MIT License. See [LICENSE](LICENSE) for details.

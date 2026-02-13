"""
dagprofiler - A standard for authoring profiler-compatible DAG tasks.

This package provides:
- Task: Abstract base class for DAG tasks
- Config: Dynamic configuration container
- DAG: DAG executor with automatic profiling
- Profile: JSON profile storage and loading
- @task: Decorator for simple task definition
"""

from dagprofiler.config import Config
from dagprofiler.dag import DAG
from dagprofiler.decorators import task
from dagprofiler.metrics import EdgeMetrics, TaskMetrics
from dagprofiler.profile import Profile
from dagprofiler.task import Task

__version__ = "0.1.0"

__all__ = [
    "Task",
    "Config",
    "DAG",
    "Profile",
    "TaskMetrics",
    "EdgeMetrics",
    "task",
    "__version__",
]

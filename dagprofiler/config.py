"""
Generic configuration container for DAG parameters.

Accepts arbitrary key-value pairs for flexible parameterization.
"""

from typing import Any, Dict, Iterator


class Config:
    """
    Dynamic configuration container.

    Accepts arbitrary parameters that can be used by tasks
    for instruction estimation and computation scaling.

    Example:
        cfg = Config(N_UE=10000, N_CELL=7, N_SLICE=5)
        print(cfg.N_UE)  # 10000
        print(cfg["N_CELL"])  # 7
    """

    def __init__(self, **kwargs: Any):
        """
        Initialize configuration with arbitrary parameters.

        Args:
            **kwargs: Configuration parameters (e.g., N_UE=10000, N_CELL=7)
        """
        self._params: Dict[str, Any] = dict(kwargs)

    def __getattr__(self, name: str) -> Any:
        """Get parameter by attribute access."""
        if name.startswith("_"):
            raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")
        try:
            return self._params[name]
        except KeyError:
            raise AttributeError(
                f"'{type(self).__name__}' has no parameter '{name}'. "
                f"Available: {list(self._params.keys())}"
            )

    def __getitem__(self, name: str) -> Any:
        """Get parameter by key access."""
        return self._params[name]

    def __contains__(self, name: str) -> bool:
        """Check if parameter exists."""
        return name in self._params

    def __iter__(self) -> Iterator[str]:
        """Iterate over parameter names."""
        return iter(self._params)

    def __len__(self) -> int:
        """Return number of parameters."""
        return len(self._params)

    def get(self, name: str, default: Any = None) -> Any:
        """Get parameter with optional default."""
        return self._params.get(name, default)

    def to_dict(self) -> Dict[str, Any]:
        """Return configuration as a dictionary."""
        return dict(self._params)

    def __repr__(self) -> str:
        params_str = ", ".join(f"{k}={v!r}" for k, v in self._params.items())
        return f"Config({params_str})"

    def copy(self, **overrides: Any) -> "Config":
        """Create a copy with optional parameter overrides."""
        new_params = dict(self._params)
        new_params.update(overrides)
        return Config(**new_params)

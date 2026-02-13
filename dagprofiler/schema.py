"""
JSON Schema definitions for profile validation.
"""

PROFILE_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "DAG Execution Profile",
    "type": "object",
    "required": ["metadata", "configuration", "dag_structure", "node_weights", "edge_weights"],
    "properties": {
        "metadata": {
            "type": "object",
            "required": ["timestamp", "dagprofiler_version"],
            "properties": {
                "timestamp": {"type": "string", "format": "date-time"},
                "dagprofiler_version": {"type": "string"},
                "seed": {"type": ["integer", "null"]},
                "hostname": {"type": "string"},
            },
        },
        "configuration": {
            "type": "object",
            "description": "User-defined scale parameters",
            "additionalProperties": True,
        },
        "dag_structure": {
            "type": "object",
            "required": ["nodes", "edges", "execution_order"],
            "properties": {
                "nodes": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "edges": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["source", "target"],
                        "properties": {
                            "source": {"type": "string"},
                            "target": {"type": "string"},
                        },
                    },
                },
                "execution_order": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
        },
        "node_weights": {
            "type": "object",
            "description": "Task name -> estimated instruction count",
            "additionalProperties": {"type": "integer"},
        },
        "edge_weights": {
            "type": "object",
            "description": "Edge key (src->dst) -> data size in bits",
            "additionalProperties": {"type": "integer"},
        },
        "execution_metrics": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["task", "compute_time_ms", "bytes_in", "bytes_out"],
                "properties": {
                    "task": {"type": "string"},
                    "compute_time_ms": {"type": "number"},
                    "serialize_time_ms": {"type": "number"},
                    "deserialize_time_ms": {"type": "number"},
                    "bytes_in": {"type": "integer"},
                    "bytes_out": {"type": "integer"},
                },
            },
        },
        "total_time_ms": {"type": "number"},
    },
}


def validate_profile(data: dict) -> bool:
    """
    Validate profile data against the JSON schema.

    Args:
        data: Profile data dictionary

    Returns:
        True if valid

    Raises:
        jsonschema.ValidationError: If validation fails
    """
    try:
        import jsonschema
        jsonschema.validate(data, PROFILE_SCHEMA)
        return True
    except ImportError:
        # jsonschema not installed - skip validation
        return True

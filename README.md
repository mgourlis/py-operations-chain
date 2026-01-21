# Operations Chain

A configurable, AI-agent friendly pipeline execution engine for Python.

[![Tests](https://img.shields.io/badge/tests-110%20passing-brightgreen)](tests/)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](pyproject.toml)

## Features

- **Modular Operations**: Chain discrete, reusable operations (transformations, validations, side effects)
- **AI-Agent Friendly**: Self-documenting schemas with examples, fuzzy-matched error suggestions
- **Framework Agnostic**: No framework dependencies (pure Python)
- **Async First**: Built for modern async Python applications
- **Introspection API**: Discover operations and their schemas programmatically

## Installation

```bash
pip install operations-chain
```

For HTTP side effects:
```bash
pip install operations-chain[http]
```

For development:
```bash
pip install operations-chain[dev]
```

---

## Quick Start

```python
import asyncio
from operations_chain import PipelineExecutor, OperationSpec

async def main():
    # Define a pipeline
    pipeline = [
        OperationSpec(
            operation="extract_field",
            operation_config={"field": "user.name"}
        ),
        OperationSpec(operation="strip"),
        OperationSpec(operation="uppercase"),
    ]
    
    # Execute it
    executor = PipelineExecutor(shared_data={})
    result = await executor.execute_pipeline(
        pipeline,
        {"user": {"name": "  alice  "}}
    )
    
    print(result)  # "ALICE"

asyncio.run(main())
```

---

## For AI Agents

The library is designed for AI agent discovery:

```python
from operations_chain import get_registry

registry = get_registry()

# List all available operations
for op in registry.list_operations():
    print(f"{op['name']}: {op['description']}")

# Get detailed documentation for an operation
info = registry.describe_operation("extract_field")
print(info['config_schema'])
print(info['example'])
```

Error messages include suggestions:

```python
>>> registry.get_operation("extrct_field")
OperationNotFoundError: Unknown operation: 'extrct_field'.
Did you mean: extract_field, extract?
```

---

## Core Concepts

### Operations

Operations are discrete units of logic. There are four types:

| Type | Description | Returns |
|------|-------------|---------|
| **Transformation** | Modifies the input value | New value |
| **Validation** | Checks constraints | Original value (or raises) |
| **Side Effect** | Performs external actions | Original value |
| **Control Flow** | Directs execution flow | Result from branch |

### Pipeline

A pipeline is an ordered sequence of operations:

```python
pipeline = [
    OperationSpec(operation="required"),
    OperationSpec(operation="strip"),
    OperationSpec(operation="uppercase"),
]
```

### Context

Shared state passed through the pipeline. Operations can read/write to `context.shared_data`:

```python
executor = PipelineExecutor(shared_data={"user_id": 123})
# Operations can access context.shared_data["user_id"]
```

---

## Operation Reference

### Transformation Operations

#### `extract_field` / `extract`
Extract a field from a dictionary using dot notation.

```python
{
    "operation": "extract_field",
    "operation_config": {
        "field": "user.profile.name",  # required: dot-separated path
        "default": "Unknown"            # optional: default if not found
    }
}
```

#### `concatenate` / `concat`
Join values with a separator.

```python
{
    "operation": "concat",
    "operation_config": {
        "separator": ", ",              # optional: join string (default: "")
        "fields": ["first", "last"]     # optional: fields to extract from dict
    }
}
# Input: ["a", "b", "c"] → Output: "a, b, c"
# Input: {"first": "John", "last": "Doe"} with fields → Output: "John Doe"
```

#### `format_string` / `format`
Format a string with placeholders.

```python
{
    "operation": "format",
    "operation_config": {
        "template": "Hello, {value}!",  # optional: template with placeholders
        "fields": {"greeting": "Hi"}    # optional: additional placeholders
    }
}
# Input: "Alice" → Output: "Hello, Alice!"
```

#### `type_cast` / `cast`
Convert value to a specific type.

```python
{
    "operation": "cast",
    "operation_config": {
        "target_type": "int",           # optional: int, float, str, bool (default: str)
        "on_error": "return_default",   # optional: raise, return_default, return_none
        "default": 0                    # optional: default on error
    }
}
# Input: "42" → Output: 42
# Input: "true" (bool) → Output: True
```

#### `default_value` / `default`
Provide fallback for None or empty values.

```python
{
    "operation": "default",
    "operation_config": {
        "default": "N/A",               # required: fallback value
        "check_empty": true             # optional: also check empty strings/lists
    }
}
```

#### `map_values` / `map`
Map values using a lookup dictionary.

```python
{
    "operation": "map",
    "operation_config": {
        "mapping": {"A": "Active", "I": "Inactive"},  # required
        "default": "Unknown",           # optional: if not found
        "case_sensitive": false         # optional: for string keys
    }
}
# Input: "A" → Output: "Active"
```

#### `json_parse` / `parse_json`
Parse JSON string to Python object.

```python
{
    "operation": "json_parse",
    "operation_config": {
        "on_error": "return_default",   # optional: raise, return_default, return_original
        "default": {}                   # optional: default on error
    }
}
```

#### `json_serialize` / `serialize_json`
Serialize Python object to JSON string.

```python
{
    "operation": "json_serialize",
    "operation_config": {
        "indent": 2,                    # optional: indentation spaces
        "sort_keys": true               # optional: sort dict keys
    }
}
```

#### `strip_whitespace` / `strip`
Remove whitespace from strings.

```python
{
    "operation": "strip",
    "operation_config": {
        "mode": "both"                  # optional: both, left, right
    }
}
```

#### `lowercase` / `lower`
Convert string to lowercase.

```python
{"operation": "lowercase"}
```

#### `uppercase` / `upper`
Convert string to uppercase.

```python
{"operation": "uppercase"}
```

#### `replace`
Replace substrings.

```python
{
    "operation": "replace",
    "operation_config": {
        "search": "old",                # required: string to find
        "replace": "new",               # required: replacement
        "count": 1,                     # optional: max replacements (0=all)
        "use_regex": true               # optional: regex mode
    }
}
```

#### `set_value` / `set`
Return a static value, ignoring input.

```python
{
    "operation": "set",
    "operation_config": {
        "value": null                   # required: static value to return
    }
}
```

---

### Validation Operations

All validations return the input value unchanged on success, or raise `ValidationError` on failure.

#### `required` / `validate_required`
Ensure value is not None or empty.

```python
{
    "operation": "required",
    "operation_config": {
        "allow_empty_string": false,    # optional
        "allow_empty_list": false       # optional
    }
}
```

#### `range` / `validate_range`
Validate numeric range.

```python
{
    "operation": "range",
    "operation_config": {
        "min": 0,                       # optional: minimum (inclusive)
        "max": 100                      # optional: maximum (inclusive)
    }
}
```

#### `length` / `validate_length`
Validate string/list length.

```python
{
    "operation": "length",
    "operation_config": {
        "min_length": 1,                # optional
        "max_length": 255               # optional
    }
}
```

#### `regex` / `validate_regex`
Match against regex pattern.

```python
{
    "operation": "regex",
    "operation_config": {
        "pattern": "^[A-Z]{2}\\d{4}$",  # required: regex pattern
        "flags": "i"                    # optional: i=ignore case, m=multiline
    }
}
```

#### `email` / `validate_email`
Validate email format.

```python
{"operation": "email"}
```

#### `url` / `validate_url`
Validate URL format.

```python
{
    "operation": "url",
    "operation_config": {
        "schemes": ["http", "https"]    # optional: allowed schemes
    }
}
```

#### `type` / `validate_type`
Validate value type.

```python
{
    "operation": "type",
    "operation_config": {
        "expected_type": "str"          # required: str, int, float, bool, list, dict, none
    }
}
```

#### `in_list` / `validate_in_list`
Ensure value is in allowed list.

```python
{
    "operation": "in_list",
    "operation_config": {
        "allowed_values": ["a", "b"],   # required
        "case_sensitive": false         # optional
    }
}
```

#### `not_in_list` / `validate_not_in_list`
Ensure value is NOT in forbidden list.

```python
{
    "operation": "not_in_list",
    "operation_config": {
        "forbidden_values": ["admin"],  # required
        "case_sensitive": false         # optional
    }
}
```

#### `comparison` / `compare`
Compare against a value.

```python
{
    "operation": "compare",
    "operation_config": {
        "operator": "gt",               # required: eq, ne, lt, le, gt, ge
        "compare_to": 0,                # optional: static value
        "context_key": "threshold"      # optional: use context.shared_data key
    }
}
```

#### `unique` / `validate_unique`
Ensure value wasn't seen before in pipeline.

```python
{"operation": "unique"}
```

---

### Side Effect Operations

Side effects perform actions but always return the original value unchanged.

#### `log_value` / `log`
Log value for debugging.

```python
{
    "operation": "log",
    "operation_config": {
        "level": "info",                # optional: debug, info, warning, error
        "message": "Processing"         # optional: prefix
    }
}
```

#### `store_in_context` / `store`
Save value to shared context.

```python
{
    "operation": "store",
    "operation_config": {
        "context_path": "user.id",      # required: where to store (dot notation)
        "value_path": "data.user_id",   # optional: extract from input first
        "overwrite": true               # optional: overwrite existing
    }
}
```

#### `increment_counter`
Increment a counter in context.

```python
{
    "operation": "increment_counter",
    "operation_config": {
        "key": "processed_count",       # optional: counter key (default: counter)
        "increment": 1                  # optional: amount
    }
}
```

#### `http_request` / `http`
Make HTTP request. Requires `pip install operations-chain[http]`.

```python
{
    "operation": "http",
    "operation_config": {
        "url": "https://api.example.com/{value}",  # required
        "method": "POST",               # optional: GET, POST, PUT, DELETE
        "headers": {"Authorization": "Bearer token"},
        "body_template": "{\"id\": \"{value}\"}",
        "store_response_key": "api_response",
        "timeout": 30
    }
}
```

#### `notify`
Placeholder for notifications (logs only).

```python
{
    "operation": "notify",
    "operation_config": {
        "channel": "email",             # required: email, sms, webhook
        "recipient": "user@example.com",# required
        "message": "New: {value}"       # optional
    }
}
```

---

### Control Flow Operations

#### `if_else` / `if`
Conditional branching.

```python
{
    "operation": "if_else",
    "operation_config": {
        "condition": [                  # required: validation sub-pipeline
            {"operation": "required"}
        ],
        "then_branch": [                # required: if condition passes
            {"operation": "uppercase"}
        ],
        "else_branch": [                # optional: if condition fails
            {"operation": "set", "operation_config": {"value": "N/A"}}
        ]
    }
}
```

#### `execute_pipeline_on_path` / `on_path`
Run sub-pipeline on nested value.

```python
{
    "operation": "on_path",
    "operation_config": {
        "path": "user.name",            # required: dot-separated path
        "pipeline": [                   # required: sub-pipeline
            {"operation": "strip"},
            {"operation": "uppercase"}
        ]
    }
}
# Input: {"user": {"name": "  alice  "}}
# Output: {"user": {"name": "ALICE"}}
```

---

## Custom Operations

Create custom operations by extending the base classes:

```python
from operations_chain import TransformationOperation, register_operation

class DoubleTransformation(TransformationOperation):
    """Double the input value."""
    
    def get_config_schema(self):
        return {
            'required': {},
            'optional': {
                'times': {
                    'type': 'int',
                    'description': 'Multiplier',
                    'default': 2,
                    'example': 3
                }
            }
        }
    
    async def transform(self, value, context):
        multiplier = self.config.get('times', 2)
        return value * multiplier

# Register with the global registry
register_operation('double', DoubleTransformation)
```

Available base classes:
- `TransformationOperation` - implement `async def transform(self, value, context)`
- `ValidationOperation` - implement `async def validate(self, value, context) -> bool`
- `SideEffectOperation` - implement `async def perform(self, value, context)`
- `ControlFlowOperation` - implement `async def direct_flow(self, value, context)`

---

## Error Handling

### Operation-Level

Use `is_required` to control whether failures stop the pipeline:

```python
OperationSpec(
    operation="validate_email",
    is_required=False  # Continue on failure
)
```

Use `error_message` for custom error messages:

```python
OperationSpec(
    operation="required",
    error_message="Email is required"
)
```

### Transformation Error Handling

Transformations support `on_error` config:

```python
{
    "operation": "type_cast",
    "operation_config": {
        "target_type": "int",
        "on_error": "return_default",  # raise, return_none, return_original
        "default": 0
    }
}
```

### Catching Errors

```python
from operations_chain import ValidationError, PipelineExecutionError

try:
    result = await executor.execute_pipeline(pipeline, data)
except ValidationError as e:
    print(e.to_dict())  # Structured error for APIs
except PipelineExecutionError as e:
    print(f"Step {e.step_index} failed: {e.message}")
```

---

## Debugging

### Execution Log

```python
executor = PipelineExecutor()
await executor.execute_pipeline(pipeline, data)

# Get step-by-step log
log = executor.get_execution_log()
for step in log:
    print(f"{step['operation_name']}: {step['success']} ({step['execution_time_ms']}ms)")
```

### Pipeline Validation

Validate before execution:

```python
from operations_chain import PipelineParser

errors = PipelineParser.validate([
    {"operation": "unknown"},
    {"operation": "extract_field"}  # Missing required field
])

if errors:
    for e in errors:
        print(e)
```

---

## API Reference

### PipelineExecutor

```python
executor = PipelineExecutor(shared_data={"key": "value"})
result = await executor.execute_pipeline(operations, initial_value)

executor.get_execution_log()  # List of step results
executor.get_context_data()   # Shared data dict
executor.get_full_log()       # Complete debug info
```

### OperationRegistry

```python
from operations_chain import get_registry

registry = get_registry()
registry.list_operations()                    # All operations
registry.list_operations(category="validation")  # By type
registry.describe_operation("extract_field")  # Full docs
registry.get_operation("extract_field", config)  # Instantiate
registry.has_operation("name")                # Check existence
registry.register("name", MyOperation)        # Add custom
```

### OperationSpec

```python
spec = OperationSpec(
    operation="extract_field",
    operation_config={"field": "name"},
    order_index=0,          # Execution order
    is_required=True,       # Fail pipeline on error
    error_message="Custom"  # Override error message
)
```

---

## License

MIT

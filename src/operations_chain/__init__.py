"""
Operations Chain - A configurable pipeline execution engine.

This library provides a framework for building and executing processing pipelines
composed of discrete, reusable operations. It is designed to be AI-agent friendly
with self-documenting schemas and helpful error messages.

Basic Usage:
    >>> from operations_chain import PipelineExecutor, get_registry, OperationSpec
    >>> 
    >>> # Define a pipeline
    >>> pipeline = [
    ...     OperationSpec(operation="extract_field", operation_config={"field": "name"}),
    ...     OperationSpec(operation="uppercase"),
    ... ]
    >>> 
    >>> # Execute it
    >>> executor = PipelineExecutor(shared_data={})
    >>> result = await executor.execute_pipeline(pipeline, {"name": "alice"})
    >>> print(result)  # "ALICE"

Key Concepts:
    - **Operations**: Discrete units of logic (transform, validate, side-effect, control-flow)
    - **Pipeline**: An ordered sequence of operations
    - **Registry**: Central catalog of available operations with introspection
    - **Context**: Shared state passed through the pipeline

For AI Agents:
    >>> from operations_chain import get_registry
    >>> registry = get_registry()
    >>> 
    >>> # Discover available operations
    >>> registry.list_operations()
    >>> 
    >>> # Get detailed docs for an operation
    >>> registry.describe_operation("extract_field")
"""

from .base import (
    BaseOperation,
    OperationType,
    OperationResult,
    PipelineContext,
)
from .operation import OperationSpec
from .executor import PipelineExecutor
from .parser import PipelineParser
from .registry import OperationRegistry, get_registry, register_operation
from .exceptions import (
    OperationError,
    ValidationError,
    OperationNotFoundError,
    ConfigurationError,
    PipelineExecutionError,
)

# Re-export operation base classes for custom operations
from .transformations import TransformationOperation
from .validations import ValidationOperation
from .side_effects import SideEffectOperation
from .control_flow import ControlFlowOperation

__version__ = "0.1.0"

__all__ = [
    # Core types
    "BaseOperation",
    "OperationType",
    "OperationResult",
    "PipelineContext",
    "OperationSpec",
    # Execution
    "PipelineExecutor",
    "PipelineParser",
    # Registry
    "OperationRegistry",
    "get_registry",
    "register_operation",
    # Exceptions
    "OperationError",
    "ValidationError",
    "OperationNotFoundError",
    "ConfigurationError",
    "PipelineExecutionError",
    # Base classes for custom operations
    "TransformationOperation",
    "ValidationOperation",
    "SideEffectOperation",
    "ControlFlowOperation",
]

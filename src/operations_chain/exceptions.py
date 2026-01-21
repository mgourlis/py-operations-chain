"""
Exception classes for Operations Chain.

All exceptions include:
- Descriptive messages with context
- `to_dict()` method for structured JSON output (AI agent friendly)
- Fuzzy-matched suggestions where applicable
"""

from difflib import get_close_matches
from typing import Any, Dict, List, Optional


class OperationError(Exception):
    """
    Base exception for all Operations Chain errors.
    
    All exceptions in this library inherit from OperationError,
    making it easy to catch all pipeline-related errors.
    
    Example:
        >>> try:
        ...     await executor.execute_pipeline(ops, data)
        ... except OperationError as e:
        ...     print(e.to_dict())
    """
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to JSON-serializable dict for API responses."""
        return {
            "error": self.__class__.__name__,
            "message": str(self),
        }


class ValidationError(OperationError):
    """
    Raised when a validation operation fails.
    
    Attributes:
        message: Human-readable error message
        operation_name: Name of the operation that failed
        path: Location in the pipeline (e.g., "step[2]")
    
    Example:
        >>> raise ValidationError(
        ...     "Value must be positive",
        ...     operation_name="range",
        ...     path="step[3]"
        ... )
    """
    
    def __init__(
        self,
        message: str,
        operation_name: Optional[str] = None,
        path: Optional[str] = None,
    ):
        self.message = message
        self.operation_name = operation_name
        self.path = path
        super().__init__(message)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": "VALIDATION_ERROR",
            "message": self.message,
            "operation": self.operation_name,
            "path": self.path,
        }


class OperationNotFoundError(OperationError):
    """
    Raised when an unknown operation is requested.
    
    Includes fuzzy-matched suggestions to help identify typos.
    
    Attributes:
        operation: The unknown operation name that was requested
        valid_operations: List of all valid operation names
        suggestions: Fuzzy-matched similar operation names
    
    Example:
        >>> registry.get_operation("extrct_field")
        OperationNotFoundError: Unknown operation: 'extrct_field'.
        Did you mean: extract_field, extract?
        Available operations: concat, default, extract_field, format, ...
    """
    
    def __init__(self, operation: str, valid_operations: List[str]):
        self.operation = operation
        self.valid_operations = valid_operations
        self.suggestions = get_close_matches(
            operation.lower(),
            [op.lower() for op in valid_operations],
            n=3,
            cutoff=0.5,
        )
        # Map back to original case
        self.suggestions = [
            op for op in valid_operations
            if op.lower() in self.suggestions
        ]
        
        message = f"Unknown operation: '{operation}'."
        if self.suggestions:
            message += f" Did you mean: {', '.join(self.suggestions)}?"
        
        # Show available operations (truncated)
        sorted_ops = sorted(valid_operations)[:10]
        message += f"\nAvailable operations: {', '.join(sorted_ops)}"
        if len(valid_operations) > 10:
            message += f" ... ({len(valid_operations) - 10} more)"
        
        super().__init__(message)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": "OPERATION_NOT_FOUND",
            "operation": self.operation,
            "suggestions": self.suggestions,
            "valid_operations": sorted(self.valid_operations),
        }


class ConfigurationError(OperationError):
    """
    Raised when an operation's configuration is invalid.
    
    Includes the schema showing required and optional parameters.
    
    Attributes:
        message: Error description
        operation_name: Name of the misconfigured operation
        config_schema: The expected configuration schema
        provided_config: The invalid configuration that was provided
    
    Example:
        >>> raise ConfigurationError(
        ...     "Missing required parameter: 'field'",
        ...     operation_name="extract_field",
        ...     config_schema={"required": {"field": {...}}},
        ...     provided_config={}
        ... )
    """
    
    def __init__(
        self,
        message: str,
        operation_name: Optional[str] = None,
        config_schema: Optional[Dict[str, Any]] = None,
        provided_config: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.operation_name = operation_name
        self.config_schema = config_schema
        self.provided_config = provided_config
        
        full_message = message
        if operation_name:
            full_message = f"[{operation_name}] {message}"
        
        super().__init__(full_message)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": "CONFIGURATION_ERROR",
            "message": self.message,
            "operation": self.operation_name,
            "config_schema": self.config_schema,
            "provided_config": self.provided_config,
        }


class PipelineExecutionError(OperationError):
    """
    Raised when pipeline execution fails at a specific step.
    
    Wraps the original exception with pipeline context (which step failed).
    
    Attributes:
        message: Error description
        step_index: Index of the step that failed (0-based)
        operation_name: Name of the operation that failed
        original_error: The underlying exception
    
    Example:
        >>> raise PipelineExecutionError(
        ...     "Type conversion failed",
        ...     step_index=2,
        ...     operation_name="type_cast",
        ...     original_error=ValueError("invalid literal")
        ... )
    """
    
    def __init__(
        self,
        message: str,
        step_index: Optional[int] = None,
        operation_name: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        self.message = message
        self.step_index = step_index
        self.operation_name = operation_name
        self.original_error = original_error
        
        full_message = message
        if step_index is not None:
            full_message = f"Step {step_index}: {message}"
        
        super().__init__(full_message)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "error": "PIPELINE_EXECUTION_ERROR",
            "message": self.message,
            "step_index": self.step_index,
            "operation": self.operation_name,
        }
        if self.original_error:
            result["original_error"] = str(self.original_error)
        return result

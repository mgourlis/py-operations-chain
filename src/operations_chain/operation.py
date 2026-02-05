"""
Operation specification value object.

Represents a single operation in a pipeline definition.
This is the data structure used to define pipelines in JSON/dict format.
"""

from typing import Optional, Dict, Any


class OperationSpec:
    """
    Value object representing a single operation in a pipeline.

    Operations are executed in order (order_index) within the pipeline.
    This class is the bridge between JSON pipeline definitions and
    executable operation instances.

    Attributes:
        operation: Operation name (e.g., 'extract_field', 'required')
        operation_config: Configuration dict for the operation
        order_index: Execution order within the pipeline
        is_required: If False, operation failure won't stop pipeline
        error_message: Custom error message if operation fails

    Example (creating manually):
        >>> spec = OperationSpec(
        ...     operation="extract_field",
        ...     operation_config={"field": "user.name", "default": "Anonymous"},
        ...     is_required=True
        ... )

    Example (from JSON):
        >>> {
        ...     "operation": "extract_field",
        ...     "operation_config": {"field": "user.name"},
        ...     "is_required": true
        ... }
    """

    def __init__(
        self,
        operation: str,
        operation_config: Optional[Dict[str, Any]] = None,
        order_index: int = 0,
        is_required: bool = True,
        error_message: Optional[str] = None,
    ):
        """
        Initialize an operation specification.

        Args:
            operation: Operation name (e.g., 'extract_field', 'validate_range').
                      Must match a registered operation in the registry.
            operation_config: Configuration dict for the operation.
                             Keys depend on the specific operation's schema.
            order_index: Execution order within the pipeline (lower = earlier).
            is_required: If False, operation failure won't stop pipeline execution.
                        The pipeline will continue with the previous value.
            error_message: Custom error message if operation fails.
                          Overrides the default error message.
        """
        self._operation = operation
        self._operation_config = operation_config or {}
        self._order_index = order_index
        self._is_required = is_required
        self._error_message = error_message

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the operation specification to a dictionary.

        Useful for serialization to JSON.
        """
        return {
            "operation": self._operation,
            "operation_config": self._operation_config,
            "order_index": self._order_index,
            "is_required": self._is_required,
            "error_message": self._error_message,
        }

    def __repr__(self):
        return f"OperationSpec(operation={self._operation}, order={self._order_index})"

    # Properties (read-only)
    @property
    def operation(self) -> str:
        """The operation name."""
        return self._operation

    @property
    def operation_config(self) -> Dict[str, Any]:
        """Configuration dictionary for the operation."""
        return self._operation_config

    @property
    def order_index(self) -> int:
        """Execution order (lower = earlier)."""
        return self._order_index

    @property
    def is_required(self) -> bool:
        """Whether failure should stop the pipeline."""
        return self._is_required

    @property
    def error_message(self) -> Optional[str]:
        """Custom error message for failures."""
        return self._error_message

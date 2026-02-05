"""
Pipeline executor for running ordered operation sequences.

The pipeline follows a functional approach where each operation
receives the output of the previous operation and produces output
for the next operation.
"""

from typing import Any, Dict, List, TYPE_CHECKING
import logging

from .base import PipelineContext
from .exceptions import ValidationError, PipelineExecutionError

if TYPE_CHECKING:
    from .operation import OperationSpec

logger = logging.getLogger("operations_chain")


class PipelineExecutor:
    """
    Executes a pipeline of operations in sequence.

    The executor:
    - Creates a pipeline context
    - Runs operations in order
    - Handles errors based on is_required flag
    - Collects execution metadata for debugging

    Example:
        >>> from operations_chain import PipelineExecutor, OperationSpec
        >>>
        >>> pipeline = [
        ...     OperationSpec(operation="extract_field", operation_config={"field": "name"}),
        ...     OperationSpec(operation="uppercase"),
        ... ]
        >>>
        >>> executor = PipelineExecutor(shared_data={"user_id": 123})
        >>> result = await executor.execute_pipeline(pipeline, {"name": "alice"})
        >>> print(result)  # "ALICE"
    """

    def __init__(self, shared_data: Dict[str, Any] = None):
        """
        Initialize the pipeline executor.

        Args:
            shared_data: Initial shared data for the pipeline context.
                        This is accessible to all operations via context.shared_data.
        """
        from .registry import get_registry

        self.context = PipelineContext(shared_data=shared_data or {})
        self.registry = get_registry()

    async def execute_pipeline(
        self, operations: List["OperationSpec"], initial_value: Any
    ) -> Any:
        """
        Execute a pipeline of operations.

        Args:
            operations: List of OperationSpec objects
                       (should be pre-sorted by order_index, but will be sorted here as safety)
            initial_value: Initial value to pass to first operation

        Returns:
            Final value after all operations

        Raises:
            ValidationError: If a required operation fails
            PipelineExecutionError: If an unexpected error occurs
        """
        current_value = initial_value

        # Sort operations by order_index as a safety measure
        sorted_operations = sorted(operations, key=lambda op: op.order_index)

        logger.debug(
            "Starting pipeline with %d operations, initial_value type: %s",
            len(sorted_operations),
            type(initial_value).__name__,
        )

        for step_index, operation_entity in enumerate(sorted_operations):
            operation_name = operation_entity.operation
            operation_config = operation_entity.operation_config.copy()
            is_required = operation_entity.is_required
            error_message = operation_entity.error_message

            # Add error_message to config if provided
            if error_message:
                operation_config["error_message"] = error_message

            # Get operation instance from registry
            try:
                operation = self.registry.get_operation(
                    operation_name, operation_config
                )
            except Exception as e:
                if is_required:
                    raise PipelineExecutionError(
                        str(e), step_index=step_index, operation_name=operation_name
                    ) from e
                else:
                    logger.warning("Skipping unknown operation: %s", operation_name)
                    continue

            # Execute operation
            try:
                result = await operation.execute_with_metadata(
                    current_value, self.context
                )
                current_value = result.value

                logger.debug(
                    "Operation %s completed in %.2fms",
                    operation_name,
                    result.execution_time_ms,
                )

            except ValidationError as e:
                if is_required:
                    # Add custom error message if configured
                    if error_message:
                        raise ValidationError(
                            error_message, operation_name=operation_name
                        ) from e
                    raise
                else:
                    logger.warning(
                        "Non-required operation %s failed: %s, continuing...",
                        operation_name,
                        e,
                    )

            except Exception as e:
                if is_required:
                    msg = (
                        error_message or f"Operation {operation_name} failed: {str(e)}"
                    )
                    raise PipelineExecutionError(
                        msg,
                        step_index=step_index,
                        operation_name=operation_name,
                        original_error=e,
                    ) from e
                else:
                    logger.warning(
                        "Non-required operation %s failed: %s, continuing...",
                        operation_name,
                        e,
                    )

        logger.debug(
            "Pipeline completed, final_value type: %s, total steps: %d",
            type(current_value).__name__,
            len(self.context.steps),
        )

        return current_value

    def get_execution_log(self) -> List[dict]:
        """
        Get execution log for debugging.

        Returns:
            List of operation results as dicts
        """
        return [step.to_dict() for step in self.context.steps]

    def get_context_data(self) -> dict:
        """
        Get shared context data.

        Returns:
            Context shared_data dict
        """
        return self.context.shared_data

    def get_full_log(self) -> dict:
        """
        Get full execution log with context.

        Returns:
            Complete pipeline execution log
        """
        return self.context.to_dict()

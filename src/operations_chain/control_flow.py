"""
Control Flow Operations for dynamic pipeline execution.

Control Flow operations direct the flow of the pipeline's execution
by running conditional logic or executing sub-pipelines on specific
parts of the data.
"""

from abc import abstractmethod
from typing import Any, Dict
import logging

from .base import BaseOperation, OperationType, PipelineContext
from .exceptions import ValidationError

logger = logging.getLogger("operations_chain")


class ControlFlowOperation(BaseOperation):
    """
    Abstract base class for all control flow operations.

    These operations determine the subsequent steps in a pipeline's execution
    rather than modifying the input value. The main logic is implemented in the
    `direct_flow` method.

    Example:
        >>> class RepeatOperation(ControlFlowOperation):
        ...     async def direct_flow(self, value, context):
        ...         # Execute sub-pipeline multiple times
        ...         for i in range(self.config.get('times', 1)):
        ...             value = await self._execute_sub_pipeline(value, context)
        ...         return value
    """

    def get_operation_type(self) -> OperationType:
        return OperationType.CONTROL_FLOW

    @abstractmethod
    async def direct_flow(self, value: Any, context: PipelineContext) -> Any:
        """
        Execute the control flow logic and return the result.

        Args:
            value: The current input value in the pipeline.
            context: The shared context of the pipeline execution.

        Returns:
            The result from the executed sub-pipeline or a modified value.
        """
        pass

    async def execute(self, value: Any, context: PipelineContext) -> Any:
        """Execute the `direct_flow` method for this operation."""
        return await self.direct_flow(value, context)


class IfElseOperation(ControlFlowOperation):
    """
    Execute one of two possible sub-pipelines based on a condition.

    The 'condition' is itself a pipeline of validation operations. If the condition
    pipeline executes successfully without raising a `ValidationError`, the
    'then_branch' is executed. If a `ValidationError` is caught, the
    'else_branch' is executed.
    """

    def get_config_schema(self) -> Dict[str, Any]:
        return {
            "required": {
                "condition": {
                    "type": "list",
                    "description": "Sub-pipeline of validations to evaluate",
                    "example": [{"operation": "required"}],
                },
                "then_branch": {
                    "type": "list",
                    "description": 'Sub-pipeline for the "if true" branch',
                    "example": [{"operation": "uppercase"}],
                },
            },
            "optional": {
                "else_branch": {
                    "type": "list",
                    "description": 'Sub-pipeline for the "if false" branch',
                    "default": [],
                    "example": [
                        {"operation": "set", "operation_config": {"value": "N/A"}}
                    ],
                }
            },
        }

    async def direct_flow(self, value: Any, context: PipelineContext) -> Any:
        """Evaluate the condition and execute the appropriate branch."""
        # Import here to avoid circular imports
        from .parser import PipelineParser
        from .executor import PipelineExecutor

        condition_def = self.config.get("condition", [])
        condition_ops = PipelineParser.from_json(condition_def, "ifelse_condition")

        try:
            # Attempt to execute the condition pipeline
            await PipelineExecutor(shared_data=context.shared_data).execute_pipeline(
                condition_ops, value
            )

            # If no ValidationError, condition is "true"
            logger.debug(f"{self.name}: Condition passed. Executing 'then_branch'.")
            then_branch_def = self.config.get("then_branch", [])
            then_ops = PipelineParser.from_json(then_branch_def, "ifelse_then_branch")
            return await PipelineExecutor(
                shared_data=context.shared_data
            ).execute_pipeline(then_ops, value)

        except ValidationError as e:
            # If ValidationError is caught, condition is "false"
            logger.debug(
                f"{self.name}: Condition failed. Executing 'else_branch'. Exception: {e}"
            )
            else_branch_def = self.config.get("else_branch", None)
            if not else_branch_def:
                return value

            else_ops = PipelineParser.from_json(else_branch_def, "ifelse_else_branch")
            return await PipelineExecutor(
                shared_data=context.shared_data
            ).execute_pipeline(else_ops, value)


class ExecutePipelineOnPath(ControlFlowOperation):
    """
    Execute a sub-pipeline on a value from a nested path within the input data.

    This operation retrieves a value from a dictionary using a dot-separated path,
    runs a sub-pipeline on that value, and then updates the original dictionary
    with the result.
    """

    def get_config_schema(self) -> Dict[str, Any]:
        return {
            "required": {
                "path": {
                    "type": "str",
                    "description": "Dot-separated path to the target value",
                    "example": "user.profile.name",
                },
                "pipeline": {
                    "type": "list",
                    "description": "The sub-pipeline to execute on the value at path",
                    "example": [{"operation": "strip"}, {"operation": "lowercase"}],
                },
            }
        }

    async def direct_flow(self, value: Any, context: PipelineContext) -> Any:
        """Extract data from the path, run the sub-pipeline, and update the original value."""
        # Import here to avoid circular imports
        from .parser import PipelineParser
        from .executor import PipelineExecutor

        path = self.config.get("path")
        sub_pipeline_def = self.config.get("pipeline")

        if not isinstance(value, dict):
            raise ValueError(
                f"{self.name}: Input value must be a dictionary to access path '{path}'."
            )

        if not path or not sub_pipeline_def:
            raise ValueError(
                f"{self.name}: 'path' and 'pipeline' must be provided in the config."
            )

        # Traverse the dictionary to find the target value and its parent
        keys = path.split(".")
        current_data_parent = value
        try:
            for key in keys[:-1]:
                current_data_parent = current_data_parent[key]

            initial_sub_value = current_data_parent[keys[-1]]
        except (KeyError, TypeError):
            raise ValueError(
                f"{self.name}: Path '{path}' does not exist in the input data."
            )

        # Execute the sub-pipeline on the extracted value
        logger.debug(f"{self.name}: Executing sub-pipeline on path '{path}'.")
        sub_ops = PipelineParser.from_json(
            sub_pipeline_def, f"{self.name}_sub_pipeline"
        )
        executor = PipelineExecutor(shared_data=context.shared_data)
        result_sub_value = await executor.execute_pipeline(sub_ops, initial_sub_value)

        # Update the original data structure with the result
        current_data_parent[keys[-1]] = result_sub_value

        logger.debug(f"{self.name}: Path '{path}' updated with sub-pipeline result.")
        return value

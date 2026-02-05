"""
Base operation interface for the pipeline execution engine.

This module defines the core abstractions:
- OperationType: Enum classifying operations
- OperationResult: Metadata wrapper for operation output
- PipelineContext: Shared state passed through the pipeline
- BaseOperation: Abstract base class for all operations
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from .exceptions import ValidationError


class OperationType(Enum):
    """
    Classification of operation types.

    Each type has different semantics:
    - TRANSFORMATION: Modifies the input value
    - VALIDATION: Checks constraints, returns value unchanged
    - SIDE_EFFECT: Performs external actions, returns value unchanged
    - CONTROL_FLOW: Directs execution flow (branching, sub-pipelines)
    """

    TRANSFORMATION = "transformation"
    VALIDATION = "validation"
    SIDE_EFFECT = "side_effect"
    CONTROL_FLOW = "control_flow"


@dataclass
class OperationResult:
    """
    Result of an operation execution.

    Contains the output value and metadata for debugging/logging.
    This is primarily used internally by the pipeline executor.

    Attributes:
        value: The output value from the operation
        operation_name: Name of the operation that produced this result
        operation_type: Type of the operation (transformation, validation, etc.)
        success: Whether the operation completed successfully
        error: Error message if operation failed
        metadata: Additional context (input/output types, config, etc.)
        execution_time_ms: How long the operation took to execute
        timestamp: When the operation was executed
    """

    value: Any
    operation_name: str
    operation_type: OperationType
    success: bool = True
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    execution_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging and debugging."""
        return {
            "operation_name": self.operation_name,
            "operation_type": self.operation_type.value,
            "success": self.success,
            "error": self.error,
            "metadata": self.metadata,
            "execution_time_ms": self.execution_time_ms,
            "timestamp": self.timestamp.isoformat(),
            "value_type": type(self.value).__name__
            if self.value is not None
            else "None",
        }


@dataclass
class PipelineContext:
    """
    Context passed through the pipeline.

    Accumulates results from each operation for debugging and allows
    operations to share data via shared_data.

    Attributes:
        steps: History of all operation results
        shared_data: Mutable dict for inter-operation communication

    Example:
        >>> context = PipelineContext(shared_data={"user_id": 123})
        >>> # Operations can read/write context.shared_data
        >>> context.shared_data["processed"] = True
    """

    steps: List[OperationResult] = field(default_factory=list)
    shared_data: Dict[str, Any] = field(default_factory=dict)

    def add_step(self, result: OperationResult):
        """Add an operation result to the pipeline history."""
        self.steps.append(result)

    def get_last_value(self) -> Any:
        """Get the value from the last operation."""
        if self.steps:
            return self.steps[-1].value
        return None

    def get_step_values(self) -> List[Any]:
        """Get all values from the pipeline in order."""
        return [step.value for step in self.steps]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging."""
        return {
            "total_steps": len(self.steps),
            "steps": [step.to_dict() for step in self.steps],
            "shared_data": self.shared_data,
        }


class BaseOperation(ABC):
    """
    Base abstract class for all operations in the pipeline.

    Operations follow a functional approach:
    - Receive input value(s) and context
    - Process the value
    - Return result with metadata

    Each operation is stateless and reusable.

    To create a custom operation:
        1. Inherit from one of: TransformationOperation, ValidationOperation,
           SideEffectOperation, or ControlFlowOperation
        2. Implement the required abstract method (transform, validate, perform, or direct_flow)
        3. Override get_config_schema() to document configuration options

    Example:
        >>> class DoubleTransformation(TransformationOperation):
        ...     async def transform(self, value, context):
        ...         return value * 2
        ...
        ...     def get_config_schema(self):
        ...         return {'required': {}, 'optional': {}}
    """

    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the operation.

        Args:
            name: Name of the operation (e.g., 'create_geometry', 'validate_range')
            config: Configuration dictionary for the operation
        """
        self.name = name
        self.config = config or {}

    @abstractmethod
    def get_operation_type(self) -> OperationType:
        """Return the type of this operation."""
        pass

    def get_config_schema(self) -> Dict[str, Any]:
        """
        Return the configuration schema for this operation.

        Schema defines required and optional config parameters with their types.
        This is used for validation and AI agent introspection.

        Returns:
            Dict with 'required' and 'optional' keys. Each contains param definitions
            with 'type', 'description', and optionally 'default' and 'example'.

        Example:
            {
                'required': {
                    'field': {
                        'type': 'str',
                        'description': 'Field name to extract',
                        'example': 'user.email'
                    }
                },
                'optional': {
                    'default': {
                        'type': 'any',
                        'description': 'Default if field missing',
                        'default': None,
                        'example': 'unknown'
                    }
                }
            }
        """
        return {"required": {}, "optional": {}}

    def get_description(self) -> str:
        """
        Return a brief description of what this operation does.

        Default implementation uses the class docstring's first line.
        Override to provide a custom description.
        """
        doc = self.__class__.__doc__
        if doc:
            return doc.strip().split("\n")[0]
        return f"{self.__class__.__name__} operation"

    @abstractmethod
    async def execute(self, value: Any, context: PipelineContext) -> Any:
        """
        Execute the operation.

        Args:
            value: Input value (result from previous operation)
            context: Pipeline context with shared data and history

        Returns:
            Output value to pass to next operation

        Raises:
            ValidationError: If operation fails and is required
        """
        pass

    async def execute_with_metadata(
        self, value: Any, context: PipelineContext
    ) -> OperationResult:
        """
        Execute the operation and wrap result with metadata.

        This is the main entry point that handles timing, error catching,
        and result wrapping.
        """
        import time

        start_time = time.time()

        try:
            result_value = await self.execute(value, context)
            execution_time = (time.time() - start_time) * 1000

            result = OperationResult(
                value=result_value,
                operation_name=self.name,
                operation_type=self.get_operation_type(),
                success=True,
                execution_time_ms=execution_time,
                metadata=self._get_execution_metadata(value, result_value),
            )

            context.add_step(result)
            return result

        except ValidationError as e:
            execution_time = (time.time() - start_time) * 1000
            result = OperationResult(
                value=value,  # Return original value on validation failure
                operation_name=self.name,
                operation_type=self.get_operation_type(),
                success=False,
                error=str(e),
                execution_time_ms=execution_time,
            )
            context.add_step(result)
            raise

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            result = OperationResult(
                value=value,  # Return original value on error
                operation_name=self.name,
                operation_type=self.get_operation_type(),
                success=False,
                error=str(e),
                execution_time_ms=execution_time,
            )
            context.add_step(result)
            raise

    def _get_execution_metadata(
        self, input_value: Any, output_value: Any
    ) -> Dict[str, Any]:
        """
        Generate metadata about the execution.
        Override in subclasses to add operation-specific metadata.
        """
        return {
            "input_type": type(input_value).__name__
            if input_value is not None
            else "None",
            "output_type": type(output_value).__name__
            if output_value is not None
            else "None",
            "config": self.config,
        }

    def __repr__(self):
        return f"<{self.__class__.__name__}(name={self.name}, type={self.get_operation_type().value})>"

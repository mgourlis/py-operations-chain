"""
Operation Registry for managing and instantiating operations.

Provides a centralized catalog of available operations with introspection
capabilities for AI agent discovery.
"""

from typing import Dict, Type, Optional, Any, List
import logging

from .base import BaseOperation
from .transformations import (
    TransformationOperation,
    ExtractFieldTransformation,
    ConcatenateTransformation,
    FormatStringTransformation,
    TypeCastTransformation,
    DefaultValueTransformation,
    MapValuesTransformation,
    JsonParseTransformation,
    JsonSerializeTransformation,
    StripWhitespaceTransformation,
    LowercaseTransformation,
    UppercaseTransformation,
    ReplaceTransformation,
    SetValueTransformation,
)
from .validations import (
    ValidationOperation,
    RequiredValidation,
    RangeValidation,
    LengthValidation,
    RegexValidation,
    EmailValidation,
    UrlValidation,
    TypeValidation,
    InListValidation,
    NotInListValidation,
    ComparisonValidation,
    UniqueValidation,
)
from .side_effects import (
    SideEffectOperation,
    LogValueSideEffect,
    StoreInContextSideEffect,
    IncrementCounterSideEffect,
    HttpRequestSideEffect,
    NotifySideEffect,
)
from .control_flow import (
    ControlFlowOperation,
    IfElseOperation,
    ExecutePipelineOnPath,
)
from .exceptions import OperationNotFoundError

logger = logging.getLogger("operations_chain")


class OperationRegistry:
    """
    Registry for all operation implementations.

    Operations are registered by name and can be instantiated with configuration.
    Provides introspection methods for AI agent discovery.

    Example:
        >>> from operations_chain import get_registry
        >>> registry = get_registry()
        >>>
        >>> # List all operations
        >>> registry.list_operations()
        >>>
        >>> # Get details for a specific operation
        >>> registry.describe_operation("extract_field")
        >>>
        >>> # Instantiate an operation
        >>> op = registry.get_operation("extract_field", {"field": "name"})
    """

    def __init__(self):
        self._operations: Dict[str, Type[BaseOperation]] = {}
        self._register_default_operations()

    def _register_default_operations(self):
        """Register all built-in operations."""

        # Transformations
        self.register("extract_field", ExtractFieldTransformation)
        self.register("extract", ExtractFieldTransformation)  # Alias
        self.register("concatenate", ConcatenateTransformation)
        self.register("concat", ConcatenateTransformation)  # Alias
        self.register("format_string", FormatStringTransformation)
        self.register("format", FormatStringTransformation)  # Alias
        self.register("type_cast", TypeCastTransformation)
        self.register("cast", TypeCastTransformation)  # Alias
        self.register("default_value", DefaultValueTransformation)
        self.register("default", DefaultValueTransformation)  # Alias
        self.register("map_values", MapValuesTransformation)
        self.register("map", MapValuesTransformation)  # Alias
        self.register("json_parse", JsonParseTransformation)
        self.register("parse_json", JsonParseTransformation)  # Alias
        self.register("json_serialize", JsonSerializeTransformation)
        self.register("serialize_json", JsonSerializeTransformation)  # Alias
        self.register("strip_whitespace", StripWhitespaceTransformation)
        self.register("strip", StripWhitespaceTransformation)  # Alias
        self.register("lowercase", LowercaseTransformation)
        self.register("lower", LowercaseTransformation)  # Alias
        self.register("uppercase", UppercaseTransformation)
        self.register("upper", UppercaseTransformation)  # Alias
        self.register("replace", ReplaceTransformation)
        self.register("set_value", SetValueTransformation)
        self.register("set", SetValueTransformation)  # Alias

        # Validations
        self.register("required", RequiredValidation)
        self.register("validate_required", RequiredValidation)  # Alias
        self.register("range", RangeValidation)
        self.register("validate_range", RangeValidation)  # Alias
        self.register("length", LengthValidation)
        self.register("validate_length", LengthValidation)  # Alias
        self.register("regex", RegexValidation)
        self.register("validate_regex", RegexValidation)  # Alias
        self.register("email", EmailValidation)
        self.register("validate_email", EmailValidation)  # Alias
        self.register("url", UrlValidation)
        self.register("validate_url", UrlValidation)  # Alias
        self.register("type", TypeValidation)
        self.register("validate_type", TypeValidation)  # Alias
        self.register("in_list", InListValidation)
        self.register("validate_in_list", InListValidation)  # Alias
        self.register("not_in_list", NotInListValidation)
        self.register("validate_not_in_list", NotInListValidation)  # Alias
        self.register("comparison", ComparisonValidation)
        self.register("compare", ComparisonValidation)  # Alias
        self.register("unique", UniqueValidation)
        self.register("validate_unique", UniqueValidation)  # Alias

        # Side Effects
        self.register("log_value", LogValueSideEffect)
        self.register("log", LogValueSideEffect)  # Alias
        self.register("store_in_context", StoreInContextSideEffect)
        self.register("store", StoreInContextSideEffect)  # Alias
        self.register("increment_counter", IncrementCounterSideEffect)
        self.register("http_request", HttpRequestSideEffect)
        self.register("http", HttpRequestSideEffect)  # Alias
        self.register("notify", NotifySideEffect)

        # Control Flow
        self.register("if_else", IfElseOperation)
        self.register("if", IfElseOperation)  # Alias
        self.register("execute_pipeline_on_path", ExecutePipelineOnPath)
        self.register("on_path", ExecutePipelineOnPath)  # Alias

    def register(self, name: str, operation_class: Type[BaseOperation]):
        """
        Register an operation by name.

        Args:
            name: Name to register the operation under
            operation_class: Operation class (not instance)
        """
        self._operations[name] = operation_class
        logger.debug(f"Registered operation: {name} -> {operation_class.__name__}")

    def get_operation(self, name: str, config: Optional[Dict] = None) -> BaseOperation:
        """
        Get an operation instance by name.

        Args:
            name: Operation name
            config: Configuration dict for the operation

        Returns:
            Operation instance

        Raises:
            OperationNotFoundError: If operation not found (includes suggestions)
        """
        if name not in self._operations:
            raise OperationNotFoundError(name, list(self._operations.keys()))

        operation_class = self._operations[name]
        return operation_class(name=name, config=config or {})

    def has_operation(self, name: str) -> bool:
        """Check if an operation is registered."""
        return name in self._operations

    def get_operation_config_schema(self, name: str) -> Dict[str, Any]:
        """
        Get the configuration schema for an operation.

        Args:
            name: Operation name

        Returns:
            Config schema with 'required' and 'optional' keys

        Raises:
            OperationNotFoundError: If operation not found
        """
        if name not in self._operations:
            raise OperationNotFoundError(name, list(self._operations.keys()))

        operation_class = self._operations[name]
        temp_instance = operation_class(name=name, config={})
        return temp_instance.get_config_schema()

    def describe_operation(self, name: str) -> Dict[str, Any]:
        """
        Get full documentation for an operation (for AI agents).

        Args:
            name: Operation name

        Returns:
            Dict with name, type, description, config_schema, and example

        Raises:
            OperationNotFoundError: If operation not found

        Example:
            >>> registry.describe_operation("extract_field")
            {
                "name": "extract_field",
                "type": "transformation",
                "description": "Extract a specific field from a dictionary or object.",
                "config_schema": {...},
                "example": {...}
            }
        """
        if name not in self._operations:
            raise OperationNotFoundError(name, list(self._operations.keys()))

        operation_class = self._operations[name]
        temp_instance = operation_class(name=name, config={})
        schema = temp_instance.get_config_schema()

        # Build example from schema
        example_config = {}
        for param, param_def in schema.get("required", {}).items():
            if "example" in param_def:
                example_config[param] = param_def["example"]
        for param, param_def in schema.get("optional", {}).items():
            if "example" in param_def:
                example_config[param] = param_def["example"]

        return {
            "name": name,
            "type": temp_instance.get_operation_type().value,
            "description": temp_instance.get_description(),
            "config_schema": schema,
            "example": {"operation": name, "operation_config": example_config}
            if example_config
            else {"operation": name},
        }

    def list_operations(self, category: Optional[str] = None) -> List[Dict[str, str]]:
        """
        List all operations with brief descriptions.

        Args:
            category: Optional filter by type: 'transformation', 'validation',
                     'side_effect', or 'control_flow'

        Returns:
            List of dicts with name, type, and description

        Example:
            >>> registry.list_operations(category="transformation")
            [
                {"name": "extract_field", "type": "transformation", "description": "..."},
                ...
            ]
        """
        result = []
        seen_classes = set()  # Avoid duplicates from aliases

        for name, cls in self._operations.items():
            # Skip if we've already included this class (alias)
            if cls in seen_classes:
                continue
            seen_classes.add(cls)

            temp_instance = cls(name=name, config={})
            op_type = temp_instance.get_operation_type().value

            # Filter by category if specified
            if category and op_type != category:
                continue

            result.append(
                {
                    "name": name,
                    "type": op_type,
                    "description": temp_instance.get_description(),
                }
            )

        return sorted(result, key=lambda x: (x["type"], x["name"]))

    def list_by_type(self) -> Dict[str, List[str]]:
        """
        List operations grouped by type.

        Returns:
            Dict with keys 'transformation', 'validation', 'side_effect', 'control_flow'
        """
        result: Dict[str, List[str]] = {
            "transformation": [],
            "validation": [],
            "side_effect": [],
            "control_flow": [],
        }

        seen = set()
        for name, cls in self._operations.items():
            if name in seen:
                continue
            seen.add(name)

            if issubclass(cls, TransformationOperation):
                result["transformation"].append(name)
            elif issubclass(cls, ValidationOperation):
                result["validation"].append(name)
            elif issubclass(cls, SideEffectOperation):
                result["side_effect"].append(name)
            elif issubclass(cls, ControlFlowOperation):
                result["control_flow"].append(name)

        return result

    def get_operation_type(self, name: str) -> str:
        """Get the type of an operation."""
        if name not in self._operations:
            raise OperationNotFoundError(name, list(self._operations.keys()))

        operation_class = self._operations[name]
        if issubclass(operation_class, TransformationOperation):
            return "transformation"
        elif issubclass(operation_class, ValidationOperation):
            return "validation"
        elif issubclass(operation_class, SideEffectOperation):
            return "side_effect"
        elif issubclass(operation_class, ControlFlowOperation):
            return "control_flow"
        else:
            return "unknown"


# Global registry instance
_global_registry = OperationRegistry()


def get_registry() -> OperationRegistry:
    """Get the global operation registry."""
    return _global_registry


def register_operation(name: str, operation_class: Type[BaseOperation]):
    """
    Register a custom operation with the global registry.

    Args:
        name: Operation name
        operation_class: Operation class (not instance)

    Example:
        >>> from operations_chain import register_operation, TransformationOperation
        >>>
        >>> class DoubleTransformation(TransformationOperation):
        ...     async def transform(self, value, context):
        ...         return value * 2
        >>>
        >>> register_operation('double', DoubleTransformation)
    """
    _global_registry.register(name, operation_class)

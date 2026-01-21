"""
Pipeline parser for converting JSON definitions to executable operations.

Provides utilities to parse and validate pipeline definitions.
"""

import json
from typing import Any, Dict, List, Union

from .operation import OperationSpec


class PipelineParser:
    """
    Utility class to parse and validate pipeline definitions.
    
    Transforms JSON-like pipeline definitions into OperationSpec objects
    that can be executed by PipelineExecutor.
    
    Example:
        >>> pipeline_json = [
        ...     {"operation": "extract_field", "operation_config": {"field": "name"}},
        ...     {"operation": "uppercase"}
        ... ]
        >>> 
        >>> operations = PipelineParser.from_json(pipeline_json)
        >>> # Now ready to execute with PipelineExecutor
    """

    @classmethod
    def from_json(
        cls,
        pipeline_json: Union[str, List[Dict[str, Any]]],
        request_map_name: str = "Unnamed"
    ) -> List[OperationSpec]:
        """
        Transform pipeline JSON into a list of OperationSpec objects.
        
        Ensures operations are correctly ordered and handles defaults.
        Enforces unique order indices to prevent ambiguity.

        Args:
            pipeline_json: A list of dictionaries or a JSON string defining the pipeline.
            request_map_name: Name for clearer error messages.

        Returns:
            List of OperationSpec objects sorted by order_index.
            
        Raises:
            ValueError: If JSON is invalid or operation is missing required field.
            TypeError: If input is not a string or a list.
            
        Example:
            >>> PipelineParser.from_json('[{"operation": "required"}]')
            [OperationSpec(operation=required, order=0)]
        """
        parsed_pipeline: List[Dict[str, Any]]
        if isinstance(pipeline_json, str):
            try:
                parsed_pipeline = json.loads(pipeline_json)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in pipeline for '{request_map_name}': {e}") from e
        elif isinstance(pipeline_json, list):
            parsed_pipeline = pipeline_json
        else:
            raise TypeError(f"pipeline_json must be a JSON string or a list, not {type(pipeline_json).__name__}")

        if not parsed_pipeline:
            return []
        
        operations: List[OperationSpec] = []
        used_order_indices = set()
        
        for idx, op_def in enumerate(parsed_pipeline):
            operation_name = op_def.get('operation')
            
            if not operation_name:
                raise ValueError(
                    f"Pipeline operation at index {idx} in '{request_map_name}' "
                    f"is missing required 'operation' field."
                )
            
            order_index = int(op_def.get('order_index', idx))
            
            while order_index in used_order_indices:
                order_index += 1
            
            used_order_indices.add(order_index)
            
            operations.append(
                OperationSpec(
                    operation=operation_name,
                    operation_config=op_def.get('operation_config', {}),
                    order_index=order_index,
                    is_required=bool(op_def.get('is_required', True)),
                    error_message=op_def.get('error_message')
                )
            )
        
        return sorted(operations, key=lambda x: x.order_index)

    @staticmethod
    def _validate_config_param_type(value: Any, expected_type: str) -> bool:
        """Helper to validate that a config parameter value matches the expected type."""
        type_map = {
            'str': str, 'int': int, 'bool': bool, 'float': (float, int),
            'list': list, 'dict': dict, 'any': object
        }
        expected_py_type = type_map.get(expected_type)
        return isinstance(value, expected_py_type) if expected_py_type else True

    @classmethod
    def validate(
        cls,
        pipeline_json: Union[str, List[Dict[str, Any]]],
        request_map_name: str = "Unnamed"
    ) -> List[str]:
        """
        Validate a pipeline definition without executing it.
        
        Checks:
        - JSON structure is valid
        - All required fields are present
        - Operation names are registered
        - Required config parameters are provided
        
        Args:
            pipeline_json: Pipeline definition as JSON string or list of dicts.
            request_map_name: Name for error context.
            
        Returns:
            List of validation error messages (empty if valid).
            
        Example:
            >>> errors = PipelineParser.validate([{"operation": "unknown_op"}])
            >>> if errors:
            ...     print("Validation failed:", errors)
        """
        from .registry import get_registry
        
        errors: List[str] = []
        
        # Parse JSON if needed
        if isinstance(pipeline_json, str):
            try:
                parsed_pipeline = json.loads(pipeline_json)
            except json.JSONDecodeError as e:
                return [f"Invalid JSON: {e}"]
        elif isinstance(pipeline_json, list):
            parsed_pipeline = pipeline_json
        else:
            return [f"Expected JSON string or list, got {type(pipeline_json).__name__}"]
        
        if not parsed_pipeline:
            return ["Pipeline cannot be empty"]
        
        registry = get_registry()
        
        for idx, op_def in enumerate(parsed_pipeline):
            prefix = f"Step {idx}"
            
            if not isinstance(op_def, dict):
                errors.append(f"{prefix}: Expected dict, got {type(op_def).__name__}")
                continue
            
            op_name = op_def.get('operation')
            if not op_name:
                errors.append(f"{prefix}: Missing required 'operation' field")
                continue
            
            # Check if operation exists
            if not registry.has_operation(op_name):
                errors.append(f"{prefix}: Unknown operation '{op_name}'")
                continue
            
            # Validate config against schema
            try:
                schema = registry.get_operation_config_schema(op_name)
                op_config = op_def.get('operation_config', {})
                
                # Check required params
                for param, param_def in schema.get('required', {}).items():
                    if param not in op_config:
                        errors.append(f"{prefix} ({op_name}): Missing required config '{param}'")
                    elif not cls._validate_config_param_type(op_config[param], param_def.get('type', 'any')):
                        errors.append(
                            f"{prefix} ({op_name}): Config '{param}' has invalid type, "
                            f"expected {param_def.get('type')}"
                        )
            except Exception as e:
                errors.append(f"{prefix} ({op_name}): Schema validation error: {e}")
        
        return errors

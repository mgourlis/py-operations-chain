"""
Transformation operations that modify the input value and return a new value.

Transformations are pure functions that take input and produce output.
They may use context for additional data but don't modify it.
"""

from abc import abstractmethod
from typing import Any, Dict
import logging

from .base import BaseOperation, OperationType, PipelineContext
from .exceptions import ValidationError

logger = logging.getLogger("operations_chain")


class TransformationOperation(BaseOperation):
    """
    Base class for transformation operations.
    
    Transformations:
    - Receive input value
    - Transform it according to business logic
    - Return transformed value
    - Are pure functions (no side effects)
    
    Error Handling:
    - Default: on_error='raise' (fail fast on transformation errors)
    - Configurable via config['on_error']:
      - 'raise': Raise ValidationError (default)
      - 'return_none': Return None on error
      - 'return_original': Return original value on error
    
    Example:
        >>> class DoubleTransformation(TransformationOperation):
        ...     async def transform(self, value, context):
        ...         return value * 2
    """
    
    def get_operation_type(self) -> OperationType:
        return OperationType.TRANSFORMATION
    
    @abstractmethod
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        """
        Transform the input value.
        
        Args:
            value: Input value to transform
            context: Pipeline context (read-only for transformations)
            
        Returns:
            Transformed value
        """
        pass
    
    async def execute(self, value: Any, context: PipelineContext) -> Any:
        """Execute the transformation with error handling."""
        try:
            return await self.transform(value, context)
        except ValidationError:
            # ValidationError already has proper error code and message
            raise
        except Exception as e:
            return self._handle_error(e, value)
    
    def _handle_error(self, error: Exception, original_value: Any) -> Any:
        """
        Handle transformation errors based on configuration.
        
        Args:
            error: The exception that occurred
            original_value: The original input value
            
        Returns:
            Value to return based on on_error config
            
        Raises:
            ValidationError: If on_error='raise' (default)
        """
        on_error = self.config.get('on_error', 'raise')
        
        if on_error == 'raise':
            logger.error(f"{self.name}: Transformation error: {str(error)}")
            raise ValidationError(
                f"{self.name}: {str(error)}",
                operation_name=self.name
            ) from error
        elif on_error == 'return_none':
            logger.warning(f"{self.name}: Transformation error (returning None): {str(error)}")
            return None
        elif on_error == 'return_original':
            logger.warning(f"{self.name}: Transformation error (returning original): {str(error)}")
            return original_value
        else:
            # Unknown on_error value, default to raise
            logger.error(f"{self.name}: Unknown on_error value '{on_error}', defaulting to raise")
            raise ValidationError(
                f"{self.name}: {str(error)}",
                operation_name=self.name
            ) from error


# ============================================================================
# Concrete Transformation Implementations
# ============================================================================


class ExtractFieldTransformation(TransformationOperation):
    """
    Extract a specific field from a dictionary or object.
    
    Supports nested field access using dot notation (e.g., 'user.profile.name').
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {
                'field': {
                    'type': 'str',
                    'description': 'Name of the field to extract (supports dot notation)',
                    'example': 'user.email'
                }
            },
            'optional': {
                'default': {
                    'type': 'any',
                    'description': 'Default value if field does not exist',
                    'default': None,
                    'example': 'unknown@example.com'
                }
            }
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        field_name = self.config.get('field')
        default = self.config.get('default')
        
        if not field_name:
            logger.warning(f"{self.name}: No field specified")
            return value
        
        # Support nested field access with dot notation
        parts = field_name.split('.')
        current = value
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif hasattr(current, part):
                current = getattr(current, part, None)
            else:
                current = None
            
            if current is None:
                return default
        
        return current if current is not None else default


class ConcatenateTransformation(TransformationOperation):
    """
    Concatenate multiple values into a string.
    
    Can concatenate list items, dictionary field values, or convert single values.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'separator': {
                    'type': 'str',
                    'description': 'String to join values with',
                    'default': '',
                    'example': ', '
                },
                'fields': {
                    'type': 'list',
                    'description': 'List of field names to extract and concatenate from input dict',
                    'example': ['first_name', 'last_name']
                }
            }
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        separator = self.config.get('separator', '')
        fields = self.config.get('fields', [])
        
        # If fields specified, extract them from value
        if fields and isinstance(value, dict):
            parts = [str(value.get(field, '')) for field in fields if value.get(field)]
        # If value is a list, concatenate all items
        elif isinstance(value, list):
            parts = [str(v) for v in value if v is not None]
        # If value is a single item, return as string
        else:
            return str(value) if value is not None else ''
        
        return separator.join(parts)


class FormatStringTransformation(TransformationOperation):
    """
    Format a string using Python format string template.
    
    Placeholders like {value}, {field_name} are replaced with actual values.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'template': {
                    'type': 'str',
                    'description': 'String template with {field} placeholders',
                    'default': '{value}',
                    'example': 'Hello, {value}!'
                },
                'fields': {
                    'type': 'dict',
                    'description': 'Dict mapping placeholder names to values',
                    'example': {'greeting': 'Hello'}
                }
            }
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        template = self.config.get('template', '{value}')
        
        # Build format arguments
        format_args = {'value': value}
        
        # Add configured fields
        if 'fields' in self.config:
            format_args.update(self.config['fields'])
        
        # Add context shared data
        format_args.update(context.shared_data)
        
        try:
            return template.format(**format_args)
        except KeyError as e:
            logger.error(f"{self.name}: Missing format key {e}")
            return value


class TypeCastTransformation(TransformationOperation):
    """
    Cast value to a specific type (int, float, str, bool).
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'target_type': {
                    'type': 'str',
                    'description': 'Target type: int, float, str, or bool',
                    'default': 'str',
                    'example': 'int'
                },
                'on_error': {
                    'type': 'str',
                    'description': 'Error handling: raise, return_default, or return_none',
                    'default': 'raise'
                },
                'default': {
                    'type': 'any',
                    'description': 'Default value if conversion fails',
                    'example': 0
                }
            }
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        target_type = self.config.get('target_type', 'str')
        on_error = self.config.get('on_error', 'raise')
        default = self.config.get('default')
        
        type_map = {
            'int': int,
            'float': float,
            'str': str,
            'bool': bool
        }
        
        converter = type_map.get(target_type)
        if not converter:
            logger.error(f"{self.name}: Unknown target type '{target_type}'")
            return value
        
        try:
            # Special handling for bool
            if target_type == 'bool' and isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return converter(value)
        except (ValueError, TypeError) as e:
            if on_error == 'raise':
                raise
            elif on_error == 'return_default':
                return default
            else:  # return_none
                return None


class DefaultValueTransformation(TransformationOperation):
    """
    Return default value if input is None or empty.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {
                'default': {
                    'type': 'any',
                    'description': 'Default value to use when input is None or empty',
                    'example': 'N/A'
                }
            },
            'optional': {
                'check_empty': {
                    'type': 'bool',
                    'description': 'If True, also check for empty strings/lists',
                    'default': False
                }
            }
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        default = self.config.get('default')
        check_empty = self.config.get('check_empty', False)
        
        if value is None:
            return default
        
        if check_empty:
            if isinstance(value, (str, list, dict)) and not value:
                return default
        
        return value


class MapValuesTransformation(TransformationOperation):
    """
    Map input value to output value based on a mapping dictionary.
    
    Useful for converting codes to labels, status values, etc.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {
                'mapping': {
                    'type': 'dict',
                    'description': 'Dict mapping input values to output values',
                    'example': {'A': 'Active', 'I': 'Inactive'}
                }
            },
            'optional': {
                'default': {
                    'type': 'any',
                    'description': 'Default value if no mapping found',
                    'example': 'Unknown'
                },
                'case_sensitive': {
                    'type': 'bool',
                    'description': 'Whether string comparison is case sensitive',
                    'default': True
                }
            }
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        mapping = self.config.get('mapping', {})
        default = self.config.get('default', value)
        case_sensitive = self.config.get('case_sensitive', True)
        
        if not case_sensitive and isinstance(value, str):
            # Create lowercase mapping
            lower_mapping = {k.lower(): v for k, v in mapping.items() if isinstance(k, str)}
            return lower_mapping.get(value.lower(), default)
        
        return mapping.get(value, default)


class JsonParseTransformation(TransformationOperation):
    """
    Parse JSON string to Python object.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'on_error': {
                    'type': 'str',
                    'description': 'Error handling: raise, return_default, or return_original',
                    'default': 'raise'
                },
                'default': {
                    'type': 'any',
                    'description': 'Default value if parsing fails',
                    'example': {}
                }
            }
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        import json
        
        if not isinstance(value, str):
            return value
        
        on_error = self.config.get('on_error', 'raise')
        default = self.config.get('default')
        
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            if on_error == 'raise':
                raise
            elif on_error == 'return_default':
                return default
            else:  # return_original
                return value


class JsonSerializeTransformation(TransformationOperation):
    """
    Serialize Python object to JSON string.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'indent': {
                    'type': 'int',
                    'description': 'Number of spaces for indentation (None for compact)',
                    'example': 2
                },
                'ensure_ascii': {
                    'type': 'bool',
                    'description': 'Ensure output is ASCII',
                    'default': False
                },
                'sort_keys': {
                    'type': 'bool',
                    'description': 'Sort dictionary keys',
                    'default': False
                },
                'on_error': {
                    'type': 'str',
                    'description': 'Error handling: raise, return_default, or return_original',
                    'default': 'raise'
                },
                'default': {
                    'type': 'any',
                    'description': 'Default value if serialization fails'
                }
            }
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        import json
        
        # Already a string, return as-is
        if isinstance(value, str):
            return value
        
        indent = self.config.get('indent')
        ensure_ascii = self.config.get('ensure_ascii', False)
        sort_keys = self.config.get('sort_keys', False)
        on_error = self.config.get('on_error', 'raise')
        default = self.config.get('default')
        
        try:
            return json.dumps(
                value,
                indent=indent,
                ensure_ascii=ensure_ascii,
                sort_keys=sort_keys
            )
        except (TypeError, ValueError) as e:
            if on_error == 'raise':
                raise
            elif on_error == 'return_default':
                return default
            else:  # return_original
                return value


class StripWhitespaceTransformation(TransformationOperation):
    """
    Strip whitespace from string values.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'mode': {
                    'type': 'str',
                    'description': 'Strip mode: both, left, or right',
                    'default': 'both',
                    'example': 'both'
                }
            }
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        if not isinstance(value, str):
            return value
        
        mode = self.config.get('mode', 'both')
        
        if mode == 'left':
            return value.lstrip()
        elif mode == 'right':
            return value.rstrip()
        else:  # both
            return value.strip()


class LowercaseTransformation(TransformationOperation):
    """Convert string to lowercase."""
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {'required': {}, 'optional': {}}
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        if isinstance(value, str):
            return value.lower()
        return value


class UppercaseTransformation(TransformationOperation):
    """Convert string to uppercase."""
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {'required': {}, 'optional': {}}
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        if isinstance(value, str):
            return value.upper()
        return value


class ReplaceTransformation(TransformationOperation):
    """
    Replace substrings in a string.
    
    Supports both plain string replacement and regex patterns.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {
                'search': {
                    'type': 'str',
                    'description': 'String or regex to search for',
                    'example': 'old'
                },
                'replace': {
                    'type': 'str',
                    'description': 'Replacement string',
                    'example': 'new'
                }
            },
            'optional': {
                'count': {
                    'type': 'int',
                    'description': 'Maximum replacements (0 for all)',
                    'default': 0
                },
                'use_regex': {
                    'type': 'bool',
                    'description': 'Whether to use regex',
                    'default': False
                }
            }
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        import re
        
        if not isinstance(value, str):
            return value
        
        search = self.config.get('search', '')
        replace = self.config.get('replace', '')
        count = self.config.get('count', 0)
        use_regex = self.config.get('use_regex', False)
        
        if use_regex:
            return re.sub(search, replace, value, count=count)
        else:
            return value.replace(search, replace, count if count > 0 else -1)


class SetValueTransformation(TransformationOperation):
    """
    Ignore input and return a static value from configuration.
    
    Useful for setting fixed values in conditional branches.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {
                'value': {
                    'type': 'any',
                    'description': 'The static value to return',
                    'example': None
                }
            },
            'optional': {}
        }
    
    async def transform(self, value: Any, context: PipelineContext) -> Any:
        """Simply returns the value from the configuration, ignoring the input."""
        return self.config.get('value')

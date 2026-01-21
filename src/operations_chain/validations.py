"""
Validation operations that check constraints and return the same value.

Validations are pure checks that either:
- Pass (return the input value unchanged)
- Fail (raise ValidationError if required, or log warning if optional)

The input value is always returned unchanged on success.
"""

from abc import abstractmethod
from typing import Any, Dict, List
import logging
import re

from .base import BaseOperation, OperationType, PipelineContext
from .exceptions import ValidationError

logger = logging.getLogger("operations_chain")


class ValidationOperation(BaseOperation):
    """
    Base class for validation operations.
    
    Validations:
    - Receive input value
    - Check constraints
    - Return the same value (unmodified)
    - Raise ValidationError if validation fails
    
    Example:
        >>> class PositiveValidation(ValidationOperation):
        ...     async def validate(self, value, context):
        ...         return value > 0
    """
    
    def get_operation_type(self) -> OperationType:
        return OperationType.VALIDATION
    
    @abstractmethod
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        """
        Validate the input value.
        
        Args:
            value: Input value to validate
            context: Pipeline context
            
        Returns:
            True if validation passes, False otherwise
        """
        pass
    
    async def execute(self, value: Any, context: PipelineContext) -> Any:
        """Execute the validation and return the original value."""
        is_valid = await self.validate(value, context)
        
        if not is_valid:
            error_message = self.config.get('error_message', f"Validation failed: {self.name}")
            raise ValidationError(error_message, operation_name=self.name)
        
        return value  # Always return original value


# ============================================================================
# Concrete Validation Implementations
# ============================================================================


class RequiredValidation(ValidationOperation):
    """
    Validate that value is not None or empty.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'allow_empty_string': {
                    'type': 'bool',
                    'description': 'If False, empty strings fail validation',
                    'default': False
                },
                'allow_empty_list': {
                    'type': 'bool',
                    'description': 'If False, empty lists fail validation',
                    'default': False
                }
            }
        }
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        if value is None:
            return False
        
        allow_empty_string = self.config.get('allow_empty_string', False)
        allow_empty_list = self.config.get('allow_empty_list', False)
        
        if isinstance(value, str) and not allow_empty_string and not value.strip():
            return False
        
        if isinstance(value, list) and not allow_empty_list and not value:
            return False
        
        return True


class RangeValidation(ValidationOperation):
    """
    Validate that numeric value is within a range.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'min': {
                    'type': 'float',
                    'description': 'Minimum value (inclusive)',
                    'example': 0
                },
                'max': {
                    'type': 'float',
                    'description': 'Maximum value (inclusive)',
                    'example': 100
                }
            }
        }
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            logger.warning(f"{self.name}: Cannot convert value to number: {value}")
            return False
        
        min_value = self.config.get('min')
        max_value = self.config.get('max')
        
        if min_value is not None and numeric_value < min_value:
            return False
        
        if max_value is not None and numeric_value > max_value:
            return False
        
        return True


class LengthValidation(ValidationOperation):
    """
    Validate string or list length.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'min_length': {
                    'type': 'int',
                    'description': 'Minimum length (inclusive)',
                    'example': 1
                },
                'max_length': {
                    'type': 'int',
                    'description': 'Maximum length (inclusive)',
                    'example': 255
                }
            }
        }
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        if value is None:
            return False
        
        try:
            length = len(value)
        except TypeError:
            logger.warning(f"{self.name}: Value has no length: {value}")
            return False
        
        min_length = self.config.get('min_length')
        max_length = self.config.get('max_length')
        
        if min_length is not None and length < min_length:
            return False
        
        if max_length is not None and length > max_length:
            return False
        
        return True


class RegexValidation(ValidationOperation):
    """
    Validate string against regex pattern.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {
                'pattern': {
                    'type': 'str',
                    'description': 'Regex pattern to match',
                    'example': r'^[A-Z]{2}\d{4}$'
                }
            },
            'optional': {
                'flags': {
                    'type': 'str',
                    'description': 'Regex flags (i=case-insensitive, m=multiline, s=dotall)',
                    'example': 'i'
                }
            }
        }
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        if not isinstance(value, str):
            return False
        
        pattern = self.config.get('pattern')
        if not pattern:
            logger.warning(f"{self.name}: No pattern specified")
            return True
        
        flags_str = self.config.get('flags', '')
        flags = 0
        if 'i' in flags_str:
            flags |= re.IGNORECASE
        if 'm' in flags_str:
            flags |= re.MULTILINE
        if 's' in flags_str:
            flags |= re.DOTALL
        
        try:
            return bool(re.match(pattern, value, flags))
        except re.error:
            logger.error(f"{self.name}: Invalid regex pattern: {pattern}")
            return False


class EmailValidation(ValidationOperation):
    """Validate email address format."""
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {'required': {}, 'optional': {}}
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        if not isinstance(value, str):
            return False
        
        # Simple email regex
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, value))


class UrlValidation(ValidationOperation):
    """
    Validate URL format.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'schemes': {
                    'type': 'list',
                    'description': 'List of allowed URL schemes',
                    'default': ['http', 'https'],
                    'example': ['http', 'https', 'ftp']
                }
            }
        }
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        if not isinstance(value, str):
            return False
        
        allowed_schemes = self.config.get('schemes', ['http', 'https'])
        
        # Simple URL validation
        pattern = r'^(' + '|'.join(allowed_schemes) + r')://[^\s/$.?#].[^\s]*$'
        return bool(re.match(pattern, value, re.IGNORECASE))


class TypeValidation(ValidationOperation):
    """
    Validate value type.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {
                'expected_type': {
                    'type': 'str',
                    'description': 'Expected type: str, int, float, bool, list, dict, or none',
                    'example': 'str'
                }
            },
            'optional': {}
        }
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        expected_type = self.config.get('expected_type')
        
        type_map = {
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'list': list,
            'dict': dict,
            'none': type(None)
        }
        
        expected_class = type_map.get(expected_type)
        if not expected_class:
            logger.warning(f"{self.name}: Unknown expected type: {expected_type}")
            return True
        
        return isinstance(value, expected_class)


class InListValidation(ValidationOperation):
    """
    Validate that value is in a list of allowed values.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {
                'allowed_values': {
                    'type': 'list',
                    'description': 'List of allowed values',
                    'example': ['active', 'pending', 'completed']
                }
            },
            'optional': {
                'case_sensitive': {
                    'type': 'bool',
                    'description': 'For strings, case sensitivity',
                    'default': True
                }
            }
        }
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        allowed_values = self.config.get('allowed_values', [])
        case_sensitive = self.config.get('case_sensitive', True)
        
        if not case_sensitive and isinstance(value, str):
            allowed_values_lower = [v.lower() for v in allowed_values if isinstance(v, str)]
            return value.lower() in allowed_values_lower
        
        return value in allowed_values


class NotInListValidation(ValidationOperation):
    """
    Validate that value is NOT in a list of forbidden values.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {
                'forbidden_values': {
                    'type': 'list',
                    'description': 'List of forbidden values',
                    'example': ['admin', 'root', 'system']
                }
            },
            'optional': {
                'case_sensitive': {
                    'type': 'bool',
                    'description': 'For strings, case sensitivity',
                    'default': True
                }
            }
        }
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        forbidden_values = self.config.get('forbidden_values', [])
        case_sensitive = self.config.get('case_sensitive', True)
        
        if not case_sensitive and isinstance(value, str):
            forbidden_values_lower = [v.lower() for v in forbidden_values if isinstance(v, str)]
            return value.lower() not in forbidden_values_lower
        
        return value not in forbidden_values


class ComparisonValidation(ValidationOperation):
    """
    Compare value against another value or context field.
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {
                'operator': {
                    'type': 'str',
                    'description': 'Comparison operator: eq, ne, lt, le, gt, or ge',
                    'example': 'gt'
                }
            },
            'optional': {
                'compare_to': {
                    'type': 'any',
                    'description': 'Value to compare against',
                    'example': 0
                },
                'context_key': {
                    'type': 'str',
                    'description': 'If set, compare against context.shared_data[context_key]',
                    'example': 'min_value'
                }
            }
        }
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        operator = self.config.get('operator', 'eq')
        
        # Get comparison value
        if 'context_key' in self.config:
            compare_to = context.shared_data.get(self.config['context_key'])
        else:
            compare_to = self.config.get('compare_to')
        
        # Perform comparison
        try:
            if operator == 'eq':
                return value == compare_to
            elif operator == 'ne':
                return value != compare_to
            elif operator == 'lt':
                return value < compare_to
            elif operator == 'le':
                return value <= compare_to
            elif operator == 'gt':
                return value > compare_to
            elif operator == 'ge':
                return value >= compare_to
            else:
                logger.warning(f"{self.name}: Unknown operator: {operator}")
                return True
        except TypeError:
            logger.warning(f"{self.name}: Cannot compare {type(value).__name__} with {type(compare_to).__name__}")
            return False


class UniqueValidation(ValidationOperation):
    """
    Validate that value is unique (not seen before in this pipeline).
    """
    
    def get_config_schema(self) -> Dict[str, Any]:
        return {
            'required': {},
            'optional': {
                'scope': {
                    'type': 'str',
                    'description': 'Uniqueness scope: pipeline (check against previous values)',
                    'default': 'pipeline'
                }
            }
        }
    
    async def validate(self, value: Any, context: PipelineContext) -> bool:
        scope = self.config.get('scope', 'pipeline')
        
        if scope == 'pipeline':
            # Check against previous values in this pipeline
            previous_values = context.get_step_values()
            return value not in previous_values
        
        return True

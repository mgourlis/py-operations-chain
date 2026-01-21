"""
Tests for exception classes and error handling.
"""

import pytest
from operations_chain.exceptions import (
    OperationError,
    ValidationError,
    OperationNotFoundError,
    ConfigurationError,
    PipelineExecutionError,
)


class TestOperationError:
    """Tests for base OperationError."""
    
    def test_to_dict(self):
        error = OperationError("Something went wrong")
        result = error.to_dict()
        
        assert result["error"] == "OperationError"
        assert result["message"] == "Something went wrong"
    
    def test_str_representation(self):
        error = OperationError("Test error")
        assert str(error) == "Test error"


class TestValidationError:
    """Tests for ValidationError."""
    
    def test_basic_error(self):
        error = ValidationError("Value is required")
        assert error.message == "Value is required"
        assert error.operation_name is None
        assert error.path is None
    
    def test_with_operation_name(self):
        error = ValidationError("Failed", operation_name="required")
        assert error.operation_name == "required"
    
    def test_with_path(self):
        error = ValidationError("Failed", path="step[2]")
        assert error.path == "step[2]"
    
    def test_to_dict(self):
        error = ValidationError(
            "Value must be positive",
            operation_name="range",
            path="step[3]"
        )
        result = error.to_dict()
        
        assert result["error"] == "VALIDATION_ERROR"
        assert result["message"] == "Value must be positive"
        assert result["operation"] == "range"
        assert result["path"] == "step[3]"


class TestOperationNotFoundError:
    """Tests for OperationNotFoundError."""
    
    def test_includes_suggestions(self):
        valid_ops = ["extract_field", "extract", "uppercase", "lowercase"]
        error = OperationNotFoundError("extrct_field", valid_ops)
        
        assert "extract_field" in error.suggestions
        assert error.operation == "extrct_field"
    
    def test_no_suggestions_when_no_match(self):
        valid_ops = ["uppercase", "lowercase"]
        error = OperationNotFoundError("xyz123", valid_ops)
        
        assert len(error.suggestions) == 0
    
    def test_to_dict(self):
        valid_ops = ["extract_field", "uppercase"]
        error = OperationNotFoundError("unknown", valid_ops)
        result = error.to_dict()
        
        assert result["error"] == "OPERATION_NOT_FOUND"
        assert result["operation"] == "unknown"
        assert "valid_operations" in result
        assert "suggestions" in result
    
    def test_message_includes_available_operations(self):
        valid_ops = ["a", "b", "c"]
        error = OperationNotFoundError("x", valid_ops)
        
        assert "Available operations:" in str(error)


class TestConfigurationError:
    """Tests for ConfigurationError."""
    
    def test_basic_error(self):
        error = ConfigurationError("Missing field")
        assert error.message == "Missing field"
    
    def test_with_operation_name(self):
        error = ConfigurationError(
            "Missing 'field' parameter",
            operation_name="extract_field"
        )
        assert "[extract_field]" in str(error)
    
    def test_with_schema(self):
        error = ConfigurationError(
            "Invalid config",
            operation_name="test",
            config_schema={"required": {"field": {}}},
            provided_config={}
        )
        result = error.to_dict()
        
        assert result["config_schema"] == {"required": {"field": {}}}
        assert result["provided_config"] == {}
    
    def test_to_dict(self):
        error = ConfigurationError(
            "Missing required parameter",
            operation_name="extract_field",
            config_schema={"required": {"field": {"type": "str"}}},
            provided_config={"default": "value"}
        )
        result = error.to_dict()
        
        assert result["error"] == "CONFIGURATION_ERROR"
        assert result["message"] == "Missing required parameter"
        assert result["operation"] == "extract_field"


class TestPipelineExecutionError:
    """Tests for PipelineExecutionError."""
    
    def test_basic_error(self):
        error = PipelineExecutionError("Execution failed")
        assert error.message == "Execution failed"
    
    def test_with_step_index(self):
        error = PipelineExecutionError("Failed", step_index=2)
        assert "Step 2:" in str(error)
        assert error.step_index == 2
    
    def test_with_original_error(self):
        original = ValueError("Invalid value")
        error = PipelineExecutionError(
            "Cast failed",
            step_index=1,
            operation_name="type_cast",
            original_error=original
        )
        
        assert error.original_error == original
        assert error.operation_name == "type_cast"
    
    def test_to_dict(self):
        original = ValueError("Invalid")
        error = PipelineExecutionError(
            "Operation failed",
            step_index=3,
            operation_name="custom_op",
            original_error=original
        )
        result = error.to_dict()
        
        assert result["error"] == "PIPELINE_EXECUTION_ERROR"
        assert result["step_index"] == 3
        assert result["operation"] == "custom_op"
        assert result["original_error"] == "Invalid"
    
    def test_to_dict_without_original_error(self):
        error = PipelineExecutionError("Failed", step_index=0)
        result = error.to_dict()
        
        assert "original_error" not in result

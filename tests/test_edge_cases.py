"""
Additional tests for edge cases and error handling paths.
"""

import pytest
from operations_chain import (
    PipelineExecutor,
    PipelineParser,
    OperationSpec,
    get_registry,
)
from operations_chain.base import PipelineContext, OperationResult, OperationType
from operations_chain.transformations import (
    TransformationOperation,
    ExtractFieldTransformation,
    TypeCastTransformation,
    JsonParseTransformation,
    JsonSerializeTransformation,
    ReplaceTransformation,
)
from operations_chain.validations import (
    RangeValidation,
    RegexValidation,
    TypeValidation,
    UrlValidation,
)
from operations_chain.side_effects import (
    StoreInContextSideEffect,
    HttpRequestSideEffect,
)
from operations_chain.exceptions import ValidationError, PipelineExecutionError


@pytest.fixture
def context():
    return PipelineContext(shared_data={})


class TestTransformationErrorHandling:
    """Tests for transformation error handling paths."""
    
    @pytest.mark.asyncio
    async def test_on_error_raise_default(self, context):
        """Default on_error should raise."""
        op = TypeCastTransformation(name="test", config={"target_type": "int"})
        with pytest.raises(ValidationError):
            await op.execute("not_a_number", context)
    
    @pytest.mark.asyncio
    async def test_on_error_return_none(self, context):
        op = TypeCastTransformation(name="test", config={
            "target_type": "int",
            "on_error": "return_none"
        })
        result = await op.execute("not_a_number", context)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_on_error_return_original(self, context):
        op = JsonParseTransformation(name="test", config={
            "on_error": "return_original"
        })
        result = await op.execute("not json", context)
        assert result == "not json"
    
    @pytest.mark.asyncio
    async def test_json_serialize_non_serializable(self, context):
        """Test serializing non-JSON-serializable object."""
        op = JsonSerializeTransformation(name="test", config={
            "on_error": "return_original"
        })
        # Lambda is not serializable
        obj = {"func": lambda x: x}
        result = await op.execute(obj, context)
        assert result == obj  # Returns original on error
    
    @pytest.mark.asyncio
    async def test_extract_from_object_attribute(self, context):
        """Test extracting from object with attributes."""
        class User:
            name = "Alice"
        
        op = ExtractFieldTransformation(name="test", config={"field": "name"})
        result = await op.execute(User(), context)
        assert result == "Alice"
    
    @pytest.mark.asyncio
    async def test_type_cast_unknown_type(self, context):
        """Test with unknown target type."""
        op = TypeCastTransformation(name="test", config={"target_type": "unknown"})
        result = await op.execute("value", context)
        assert result == "value"  # Returns original
    
    @pytest.mark.asyncio
    async def test_replace_with_count(self, context):
        """Test replace with count limit."""
        op = ReplaceTransformation(name="test", config={
            "search": "a",
            "replace": "X",
            "count": 1
        })
        result = await op.execute("banana", context)
        assert result == "bXnana"  # Only first replacement


class TestValidationErrorHandling:
    """Tests for validation error handling paths."""
    
    @pytest.mark.asyncio
    async def test_range_with_non_numeric(self, context):
        """Test range validation with non-numeric value."""
        op = RangeValidation(name="test", config={"min": 0, "max": 100})
        with pytest.raises(ValidationError):
            await op.execute("not a number", context)
    
    @pytest.mark.asyncio
    async def test_regex_invalid_pattern(self, context):
        """Test with invalid regex pattern."""
        op = RegexValidation(name="test", config={"pattern": "[invalid"})
        # Should return False, not crash
        result = await op.validate("test", context)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_regex_with_non_string(self, context):
        """Test regex with non-string input."""
        op = RegexValidation(name="test", config={"pattern": ".*"})
        result = await op.validate(123, context)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_type_validation_unknown_type(self, context):
        """Test type validation with unknown expected type."""
        op = TypeValidation(name="test", config={"expected_type": "unknown"})
        result = await op.validate("anything", context)
        assert result is True  # Unknown types pass
    
    @pytest.mark.asyncio
    async def test_url_with_non_string(self, context):
        """Test URL validation with non-string."""
        op = UrlValidation(name="test", config={})
        result = await op.validate(123, context)
        assert result is False


class TestSideEffectEdgeCases:
    """Tests for side effect edge cases."""
    
    @pytest.mark.asyncio
    async def test_store_with_missing_context_path(self, context):
        """Test store without context_path."""
        op = StoreInContextSideEffect(name="test", config={})
        # Should not crash, just log warning
        await op.perform("value", context)
        # Nothing stored
        assert len(context.shared_data) == 0
    
    @pytest.mark.asyncio
    async def test_store_value_path_non_dict(self, context):
        """Test value_path extraction from non-dict."""
        op = StoreInContextSideEffect(name="test", config={
            "context_path": "result",
            "value_path": "field"
        })
        await op.perform("string value", context)
        # Should not crash or store anything
        assert "result" not in context.shared_data
    
    @pytest.mark.asyncio
    async def test_store_value_path_missing(self, context):
        """Test value_path that doesn't exist."""
        op = StoreInContextSideEffect(name="test", config={
            "context_path": "result",
            "value_path": "nonexistent.path"
        })
        await op.perform({"other": "value"}, context)
        assert "result" not in context.shared_data
    
    @pytest.mark.asyncio
    async def test_store_conflicts_with_non_dict(self, context):
        """Test nested storage that conflicts with existing value."""
        context.shared_data["user"] = "string_value"  # Not a dict
        op = StoreInContextSideEffect(name="test", config={
            "context_path": "user.name"  # Can't nest under string
        })
        await op.perform("Alice", context)
        # Should not crash
        assert context.shared_data["user"] == "string_value"
    
    @pytest.mark.asyncio
    async def test_http_without_aiohttp(self, context):
        """Test HTTP operation raises ImportError without aiohttp."""
        op = HttpRequestSideEffect(name="test", config={
            "url": "http://example.com"
        })
        # This might work if aiohttp is installed, skip in that case
        try:
            import aiohttp
            pytest.skip("aiohttp is installed")
        except ImportError:
            with pytest.raises(ImportError, match="aiohttp"):
                await op.perform("value", context)


class TestPipelineExecutorEdgeCases:
    """Tests for pipeline executor edge cases."""
    
    @pytest.mark.asyncio
    async def test_empty_pipeline(self):
        """Test executing empty pipeline."""
        executor = PipelineExecutor()
        result = await executor.execute_pipeline([], "initial")
        assert result == "initial"
    
    @pytest.mark.asyncio
    async def test_custom_error_message(self):
        """Test custom error message propagation."""
        pipeline = [
            OperationSpec(
                operation="required",
                error_message="Email is required",
                is_required=True
            )
        ]
        
        executor = PipelineExecutor()
        with pytest.raises(ValidationError) as exc_info:
            await executor.execute_pipeline(pipeline, None)
        
        assert "Email is required" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_unknown_operation_non_required(self):
        """Test unknown operation that is not required."""
        pipeline = [
            OperationSpec(operation="nonexistent", is_required=False),
            OperationSpec(operation="uppercase"),
        ]
        
        executor = PipelineExecutor()
        result = await executor.execute_pipeline(pipeline, "hello")
        assert result == "HELLO"


class TestPipelineParserEdgeCases:
    """Tests for pipeline parser edge cases."""
    
    def test_parse_invalid_json_string(self):
        """Test parsing invalid JSON string."""
        with pytest.raises(ValueError, match="Invalid JSON"):
            PipelineParser.from_json("{not valid json")
    
    def test_parse_wrong_type(self):
        """Test parsing non-string/non-list input."""
        with pytest.raises(TypeError):
            PipelineParser.from_json(123)
    
    def test_parse_empty_list(self):
        """Test parsing empty list."""
        result = PipelineParser.from_json([])
        assert result == []
    
    def test_validate_empty_pipeline(self):
        """Test validating empty pipeline."""
        errors = PipelineParser.validate([])
        assert "cannot be empty" in errors[0].lower()
    
    def test_validate_non_dict_operation(self):
        """Test validating pipeline with non-dict operation."""
        errors = PipelineParser.validate(["not a dict"])
        assert any("dict" in e.lower() for e in errors)


class TestBaseOperationEdgeCases:
    """Tests for base operation functionality."""
    
    def test_operation_result_to_dict(self):
        """Test OperationResult serialization."""
        result = OperationResult(
            value="test",
            operation_name="test_op",
            operation_type=OperationType.TRANSFORMATION,
            success=True,
            execution_time_ms=1.5
        )
        
        d = result.to_dict()
        assert d["operation_name"] == "test_op"
        assert d["operation_type"] == "transformation"
        assert d["success"] is True
        assert d["value_type"] == "str"
    
    def test_pipeline_context_get_last_value_empty(self):
        """Test getting last value from empty context."""
        context = PipelineContext()
        assert context.get_last_value() is None
    
    def test_pipeline_context_to_dict(self):
        """Test context serialization."""
        context = PipelineContext(shared_data={"key": "value"})
        d = context.to_dict()
        
        assert d["total_steps"] == 0
        assert d["shared_data"] == {"key": "value"}


class TestRegistryEdgeCases:
    """Tests for registry edge cases."""
    
    def test_get_operation_type(self):
        """Test getting operation type by name."""
        registry = get_registry()
        
        assert registry.get_operation_type("extract_field") == "transformation"
        assert registry.get_operation_type("required") == "validation"
        assert registry.get_operation_type("log") == "side_effect"
        assert registry.get_operation_type("if_else") == "control_flow"
    
    def test_describe_operation_with_no_config(self):
        """Test describing operation with empty config schema."""
        registry = get_registry()
        info = registry.describe_operation("lowercase")
        
        assert info["name"] == "lowercase"
        # Should have empty or minimal example
        assert "example" in info

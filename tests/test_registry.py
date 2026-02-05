"""
Tests for the operation registry and introspection API.
"""

import pytest
from operations_chain import (
    get_registry,
    register_operation,
)
from operations_chain.transformations import TransformationOperation
from operations_chain.base import PipelineContext
from operations_chain.exceptions import OperationNotFoundError


class TestOperationRegistry:
    """Tests for OperationRegistry."""

    def test_get_registered_operation(self):
        registry = get_registry()
        op = registry.get_operation("extract_field", {"field": "name"})
        assert op is not None
        assert op.name == "extract_field"

    def test_get_unknown_operation_raises(self):
        registry = get_registry()
        with pytest.raises(OperationNotFoundError) as exc_info:
            registry.get_operation("nonexistent")

        assert "nonexistent" in str(exc_info.value)

    def test_fuzzy_suggestions_on_typo(self):
        registry = get_registry()
        with pytest.raises(OperationNotFoundError) as exc_info:
            registry.get_operation("extrct_field")

        error = exc_info.value
        assert "extract_field" in error.suggestions

    def test_has_operation(self):
        registry = get_registry()
        assert registry.has_operation("uppercase")
        assert not registry.has_operation("nonexistent")

    def test_list_operations(self):
        registry = get_registry()
        ops = registry.list_operations()

        assert len(ops) > 0
        assert all("name" in op for op in ops)
        assert all("type" in op for op in ops)
        assert all("description" in op for op in ops)

    def test_list_operations_by_category(self):
        registry = get_registry()
        transformations = registry.list_operations(category="transformation")

        assert len(transformations) > 0
        assert all(op["type"] == "transformation" for op in transformations)

    def test_describe_operation(self):
        registry = get_registry()
        info = registry.describe_operation("extract_field")

        assert info["name"] == "extract_field"
        assert info["type"] == "transformation"
        assert "description" in info
        assert "config_schema" in info
        assert "example" in info

    def test_describe_operation_includes_schema(self):
        registry = get_registry()
        info = registry.describe_operation("extract_field")

        schema = info["config_schema"]
        assert "required" in schema
        assert "field" in schema["required"]
        assert "description" in schema["required"]["field"]

    def test_describe_operation_includes_example(self):
        registry = get_registry()
        info = registry.describe_operation("extract_field")

        example = info["example"]
        assert example["operation"] == "extract_field"
        assert "operation_config" in example

    def test_list_by_type(self):
        registry = get_registry()
        by_type = registry.list_by_type()

        assert "transformation" in by_type
        assert "validation" in by_type
        assert "side_effect" in by_type
        assert "control_flow" in by_type
        assert len(by_type["transformation"]) > 0

    def test_get_operation_config_schema(self):
        registry = get_registry()
        schema = registry.get_operation_config_schema("range")

        assert "required" in schema
        assert "optional" in schema
        assert "min" in schema["optional"]
        assert "max" in schema["optional"]


class TestCustomOperationRegistration:
    """Tests for registering custom operations."""

    def test_register_custom_operation(self):
        class DoubleTransformation(TransformationOperation):
            """Double the input value."""

            async def transform(self, value, context):
                return value * 2

        # Use a unique name to avoid conflicts
        register_operation("test_double", DoubleTransformation)

        registry = get_registry()
        assert registry.has_operation("test_double")

        op = registry.get_operation("test_double")
        assert op is not None

    @pytest.mark.asyncio
    async def test_custom_operation_executes(self):
        class TripleTransformation(TransformationOperation):
            """Triple the input value."""

            async def transform(self, value, context):
                return value * 3

        register_operation("test_triple", TripleTransformation)

        registry = get_registry()
        op = registry.get_operation("test_triple")
        context = PipelineContext()

        result = await op.execute(5, context)
        assert result == 15


class TestErrorMessages:
    """Tests for AI-friendly error messages."""

    def test_error_has_suggestions(self):
        registry = get_registry()
        try:
            registry.get_operation("upprcase")  # typo
        except OperationNotFoundError as e:
            assert len(e.suggestions) > 0
            assert "uppercase" in e.suggestions

    def test_error_to_dict(self):
        registry = get_registry()
        try:
            registry.get_operation("unknown_op")
        except OperationNotFoundError as e:
            error_dict = e.to_dict()

            assert error_dict["error"] == "OPERATION_NOT_FOUND"
            assert error_dict["operation"] == "unknown_op"
            assert "valid_operations" in error_dict
            assert "suggestions" in error_dict

    def test_error_includes_available_operations(self):
        registry = get_registry()
        try:
            registry.get_operation("xyz")
        except OperationNotFoundError as e:
            assert len(e.valid_operations) > 20  # Has many operations

"""
Tests for transformation operations.
"""

import pytest
from operations_chain.transformations import (
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
from operations_chain.base import PipelineContext


@pytest.fixture
def context():
    """Create a fresh pipeline context for each test."""
    return PipelineContext(shared_data={})


class TestExtractFieldTransformation:
    """Tests for ExtractFieldTransformation."""

    @pytest.mark.asyncio
    async def test_extract_simple_field(self, context):
        op = ExtractFieldTransformation(name="test", config={"field": "name"})
        result = await op.execute({"name": "Alice", "age": 30}, context)
        assert result == "Alice"

    @pytest.mark.asyncio
    async def test_extract_nested_field(self, context):
        op = ExtractFieldTransformation(
            name="test", config={"field": "user.profile.name"}
        )
        result = await op.execute({"user": {"profile": {"name": "Bob"}}}, context)
        assert result == "Bob"

    @pytest.mark.asyncio
    async def test_extract_missing_field_returns_default(self, context):
        op = ExtractFieldTransformation(
            name="test", config={"field": "missing", "default": "N/A"}
        )
        result = await op.execute({"name": "Alice"}, context)
        assert result == "N/A"

    @pytest.mark.asyncio
    async def test_extract_missing_field_returns_none(self, context):
        op = ExtractFieldTransformation(name="test", config={"field": "missing"})
        result = await op.execute({"name": "Alice"}, context)
        assert result is None


class TestConcatenateTransformation:
    """Tests for ConcatenateTransformation."""

    @pytest.mark.asyncio
    async def test_concatenate_list(self, context):
        op = ConcatenateTransformation(name="test", config={"separator": ", "})
        result = await op.execute(["a", "b", "c"], context)
        assert result == "a, b, c"

    @pytest.mark.asyncio
    async def test_concatenate_dict_fields(self, context):
        op = ConcatenateTransformation(
            name="test", config={"separator": " ", "fields": ["first", "last"]}
        )
        result = await op.execute({"first": "John", "last": "Doe"}, context)
        assert result == "John Doe"

    @pytest.mark.asyncio
    async def test_concatenate_single_value(self, context):
        op = ConcatenateTransformation(name="test", config={})
        result = await op.execute("hello", context)
        assert result == "hello"


class TestFormatStringTransformation:
    """Tests for FormatStringTransformation."""

    @pytest.mark.asyncio
    async def test_format_with_value(self, context):
        op = FormatStringTransformation(
            name="test", config={"template": "Hello, {value}!"}
        )
        result = await op.execute("World", context)
        assert result == "Hello, World!"

    @pytest.mark.asyncio
    async def test_format_with_fields(self, context):
        op = FormatStringTransformation(
            name="test",
            config={"template": "{greeting}, {value}!", "fields": {"greeting": "Hi"}},
        )
        result = await op.execute("there", context)
        assert result == "Hi, there!"

    @pytest.mark.asyncio
    async def test_format_with_context_data(self, context):
        context.shared_data["user"] = "Alice"
        op = FormatStringTransformation(
            name="test", config={"template": "Hello, {user}! Value: {value}"}
        )
        result = await op.execute(42, context)
        assert result == "Hello, Alice! Value: 42"


class TestTypeCastTransformation:
    """Tests for TypeCastTransformation."""

    @pytest.mark.asyncio
    async def test_cast_to_int(self, context):
        op = TypeCastTransformation(name="test", config={"target_type": "int"})
        result = await op.execute("42", context)
        assert result == 42
        assert isinstance(result, int)

    @pytest.mark.asyncio
    async def test_cast_to_float(self, context):
        op = TypeCastTransformation(name="test", config={"target_type": "float"})
        result = await op.execute("3.14", context)
        assert result == 3.14

    @pytest.mark.asyncio
    async def test_cast_to_bool_true(self, context):
        op = TypeCastTransformation(name="test", config={"target_type": "bool"})
        result = await op.execute("true", context)
        assert result is True

    @pytest.mark.asyncio
    async def test_cast_to_bool_false(self, context):
        op = TypeCastTransformation(name="test", config={"target_type": "bool"})
        result = await op.execute("false", context)
        assert result is False

    @pytest.mark.asyncio
    async def test_cast_error_return_default(self, context):
        op = TypeCastTransformation(
            name="test",
            config={"target_type": "int", "on_error": "return_default", "default": -1},
        )
        result = await op.execute("not_a_number", context)
        assert result == -1


class TestDefaultValueTransformation:
    """Tests for DefaultValueTransformation."""

    @pytest.mark.asyncio
    async def test_returns_value_when_not_none(self, context):
        op = DefaultValueTransformation(name="test", config={"default": "fallback"})
        result = await op.execute("actual", context)
        assert result == "actual"

    @pytest.mark.asyncio
    async def test_returns_default_when_none(self, context):
        op = DefaultValueTransformation(name="test", config={"default": "fallback"})
        result = await op.execute(None, context)
        assert result == "fallback"

    @pytest.mark.asyncio
    async def test_check_empty_string(self, context):
        op = DefaultValueTransformation(
            name="test", config={"default": "fallback", "check_empty": True}
        )
        result = await op.execute("", context)
        assert result == "fallback"


class TestMapValuesTransformation:
    """Tests for MapValuesTransformation."""

    @pytest.mark.asyncio
    async def test_map_value(self, context):
        op = MapValuesTransformation(
            name="test", config={"mapping": {"A": "Active", "I": "Inactive"}}
        )
        result = await op.execute("A", context)
        assert result == "Active"

    @pytest.mark.asyncio
    async def test_map_value_not_found_uses_default(self, context):
        op = MapValuesTransformation(
            name="test", config={"mapping": {"A": "Active"}, "default": "Unknown"}
        )
        result = await op.execute("X", context)
        assert result == "Unknown"

    @pytest.mark.asyncio
    async def test_map_case_insensitive(self, context):
        op = MapValuesTransformation(
            name="test",
            config={"mapping": {"YES": "Affirmative"}, "case_sensitive": False},
        )
        result = await op.execute("yes", context)
        assert result == "Affirmative"


class TestJsonTransformations:
    """Tests for JSON parse/serialize operations."""

    @pytest.mark.asyncio
    async def test_json_parse(self, context):
        op = JsonParseTransformation(name="test", config={})
        result = await op.execute('{"name": "Alice"}', context)
        assert result == {"name": "Alice"}

    @pytest.mark.asyncio
    async def test_json_serialize(self, context):
        op = JsonSerializeTransformation(name="test", config={})
        result = await op.execute({"name": "Alice"}, context)
        assert result == '{"name": "Alice"}'

    @pytest.mark.asyncio
    async def test_json_serialize_with_indent(self, context):
        op = JsonSerializeTransformation(name="test", config={"indent": 2})
        result = await op.execute({"a": 1}, context)
        assert "  " in result  # Has indentation


class TestStringTransformations:
    """Tests for string transformation operations."""

    @pytest.mark.asyncio
    async def test_strip(self, context):
        op = StripWhitespaceTransformation(name="test", config={})
        result = await op.execute("  hello  ", context)
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_strip_left(self, context):
        op = StripWhitespaceTransformation(name="test", config={"mode": "left"})
        result = await op.execute("  hello  ", context)
        assert result == "hello  "

    @pytest.mark.asyncio
    async def test_lowercase(self, context):
        op = LowercaseTransformation(name="test", config={})
        result = await op.execute("HELLO", context)
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_uppercase(self, context):
        op = UppercaseTransformation(name="test", config={})
        result = await op.execute("hello", context)
        assert result == "HELLO"

    @pytest.mark.asyncio
    async def test_replace(self, context):
        op = ReplaceTransformation(
            name="test", config={"search": "old", "replace": "new"}
        )
        result = await op.execute("old value", context)
        assert result == "new value"

    @pytest.mark.asyncio
    async def test_replace_regex(self, context):
        op = ReplaceTransformation(
            name="test", config={"search": r"\d+", "replace": "X", "use_regex": True}
        )
        result = await op.execute("item123", context)
        assert result == "itemX"


class TestSetValueTransformation:
    """Tests for SetValueTransformation."""

    @pytest.mark.asyncio
    async def test_set_value_ignores_input(self, context):
        op = SetValueTransformation(name="test", config={"value": "fixed"})
        result = await op.execute("anything", context)
        assert result == "fixed"

    @pytest.mark.asyncio
    async def test_set_value_none(self, context):
        op = SetValueTransformation(name="test", config={"value": None})
        result = await op.execute("anything", context)
        assert result is None

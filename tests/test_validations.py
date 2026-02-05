"""
Tests for validation operations.
"""

import pytest
from operations_chain.validations import (
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
from operations_chain.base import PipelineContext
from operations_chain.exceptions import ValidationError


@pytest.fixture
def context():
    """Create a fresh pipeline context for each test."""
    return PipelineContext(shared_data={})


class TestRequiredValidation:
    """Tests for RequiredValidation."""

    @pytest.mark.asyncio
    async def test_passes_with_value(self, context):
        op = RequiredValidation(name="test", config={})
        result = await op.execute("hello", context)
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_fails_with_none(self, context):
        op = RequiredValidation(name="test", config={})
        with pytest.raises(ValidationError):
            await op.execute(None, context)

    @pytest.mark.asyncio
    async def test_fails_with_empty_string(self, context):
        op = RequiredValidation(name="test", config={})
        with pytest.raises(ValidationError):
            await op.execute("   ", context)

    @pytest.mark.asyncio
    async def test_passes_with_empty_string_when_allowed(self, context):
        op = RequiredValidation(name="test", config={"allow_empty_string": True})
        result = await op.execute("", context)
        assert result == ""

    @pytest.mark.asyncio
    async def test_fails_with_empty_list(self, context):
        op = RequiredValidation(name="test", config={})
        with pytest.raises(ValidationError):
            await op.execute([], context)


class TestRangeValidation:
    """Tests for RangeValidation."""

    @pytest.mark.asyncio
    async def test_passes_within_range(self, context):
        op = RangeValidation(name="test", config={"min": 0, "max": 100})
        result = await op.execute(50, context)
        assert result == 50

    @pytest.mark.asyncio
    async def test_fails_below_min(self, context):
        op = RangeValidation(name="test", config={"min": 0})
        with pytest.raises(ValidationError):
            await op.execute(-5, context)

    @pytest.mark.asyncio
    async def test_fails_above_max(self, context):
        op = RangeValidation(name="test", config={"max": 100})
        with pytest.raises(ValidationError):
            await op.execute(150, context)

    @pytest.mark.asyncio
    async def test_passes_at_boundary(self, context):
        op = RangeValidation(name="test", config={"min": 0, "max": 100})
        result = await op.execute(0, context)
        assert result == 0
        result = await op.execute(100, context)
        assert result == 100


class TestLengthValidation:
    """Tests for LengthValidation."""

    @pytest.mark.asyncio
    async def test_passes_within_length(self, context):
        op = LengthValidation(name="test", config={"min_length": 2, "max_length": 10})
        result = await op.execute("hello", context)
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_fails_too_short(self, context):
        op = LengthValidation(name="test", config={"min_length": 5})
        with pytest.raises(ValidationError):
            await op.execute("hi", context)

    @pytest.mark.asyncio
    async def test_fails_too_long(self, context):
        op = LengthValidation(name="test", config={"max_length": 5})
        with pytest.raises(ValidationError):
            await op.execute("hello world", context)

    @pytest.mark.asyncio
    async def test_works_with_list(self, context):
        op = LengthValidation(name="test", config={"min_length": 2})
        result = await op.execute([1, 2, 3], context)
        assert result == [1, 2, 3]


class TestRegexValidation:
    """Tests for RegexValidation."""

    @pytest.mark.asyncio
    async def test_passes_matching_pattern(self, context):
        op = RegexValidation(name="test", config={"pattern": r"^\d{3}-\d{4}$"})
        result = await op.execute("123-4567", context)
        assert result == "123-4567"

    @pytest.mark.asyncio
    async def test_fails_non_matching_pattern(self, context):
        op = RegexValidation(name="test", config={"pattern": r"^\d{3}-\d{4}$"})
        with pytest.raises(ValidationError):
            await op.execute("12-34567", context)

    @pytest.mark.asyncio
    async def test_case_insensitive_flag(self, context):
        op = RegexValidation(name="test", config={"pattern": r"^abc$", "flags": "i"})
        result = await op.execute("ABC", context)
        assert result == "ABC"


class TestEmailValidation:
    """Tests for EmailValidation."""

    @pytest.mark.asyncio
    async def test_passes_valid_email(self, context):
        op = EmailValidation(name="test", config={})
        result = await op.execute("user@example.com", context)
        assert result == "user@example.com"

    @pytest.mark.asyncio
    async def test_fails_invalid_email(self, context):
        op = EmailValidation(name="test", config={})
        with pytest.raises(ValidationError):
            await op.execute("not-an-email", context)


class TestUrlValidation:
    """Tests for UrlValidation."""

    @pytest.mark.asyncio
    async def test_passes_valid_url(self, context):
        op = UrlValidation(name="test", config={})
        result = await op.execute("https://example.com/path", context)
        assert result == "https://example.com/path"

    @pytest.mark.asyncio
    async def test_fails_invalid_url(self, context):
        op = UrlValidation(name="test", config={})
        with pytest.raises(ValidationError):
            await op.execute("not-a-url", context)

    @pytest.mark.asyncio
    async def test_custom_schemes(self, context):
        op = UrlValidation(name="test", config={"schemes": ["ftp"]})
        result = await op.execute("ftp://files.example.com", context)
        assert "ftp://" in result


class TestTypeValidation:
    """Tests for TypeValidation."""

    @pytest.mark.asyncio
    async def test_passes_correct_type(self, context):
        op = TypeValidation(name="test", config={"expected_type": "str"})
        result = await op.execute("hello", context)
        assert result == "hello"

    @pytest.mark.asyncio
    async def test_fails_wrong_type(self, context):
        op = TypeValidation(name="test", config={"expected_type": "str"})
        with pytest.raises(ValidationError):
            await op.execute(123, context)

    @pytest.mark.asyncio
    async def test_validates_dict(self, context):
        op = TypeValidation(name="test", config={"expected_type": "dict"})
        result = await op.execute({"key": "value"}, context)
        assert result == {"key": "value"}


class TestInListValidation:
    """Tests for InListValidation."""

    @pytest.mark.asyncio
    async def test_passes_in_list(self, context):
        op = InListValidation(name="test", config={"allowed_values": ["a", "b", "c"]})
        result = await op.execute("b", context)
        assert result == "b"

    @pytest.mark.asyncio
    async def test_fails_not_in_list(self, context):
        op = InListValidation(name="test", config={"allowed_values": ["a", "b", "c"]})
        with pytest.raises(ValidationError):
            await op.execute("x", context)

    @pytest.mark.asyncio
    async def test_case_insensitive(self, context):
        op = InListValidation(
            name="test",
            config={"allowed_values": ["YES", "NO"], "case_sensitive": False},
        )
        result = await op.execute("yes", context)
        assert result == "yes"


class TestNotInListValidation:
    """Tests for NotInListValidation."""

    @pytest.mark.asyncio
    async def test_passes_not_in_list(self, context):
        op = NotInListValidation(
            name="test", config={"forbidden_values": ["admin", "root"]}
        )
        result = await op.execute("user", context)
        assert result == "user"

    @pytest.mark.asyncio
    async def test_fails_in_forbidden_list(self, context):
        op = NotInListValidation(
            name="test", config={"forbidden_values": ["admin", "root"]}
        )
        with pytest.raises(ValidationError):
            await op.execute("admin", context)


class TestComparisonValidation:
    """Tests for ComparisonValidation."""

    @pytest.mark.asyncio
    async def test_equal(self, context):
        op = ComparisonValidation(
            name="test", config={"operator": "eq", "compare_to": 10}
        )
        result = await op.execute(10, context)
        assert result == 10

    @pytest.mark.asyncio
    async def test_greater_than(self, context):
        op = ComparisonValidation(
            name="test", config={"operator": "gt", "compare_to": 5}
        )
        result = await op.execute(10, context)
        assert result == 10

    @pytest.mark.asyncio
    async def test_fails_comparison(self, context):
        op = ComparisonValidation(
            name="test", config={"operator": "gt", "compare_to": 10}
        )
        with pytest.raises(ValidationError):
            await op.execute(5, context)

    @pytest.mark.asyncio
    async def test_compare_to_context(self, context):
        context.shared_data["threshold"] = 50
        op = ComparisonValidation(
            name="test", config={"operator": "ge", "context_key": "threshold"}
        )
        result = await op.execute(50, context)
        assert result == 50


class TestUniqueValidation:
    """Tests for UniqueValidation."""

    @pytest.mark.asyncio
    async def test_passes_unique_value(self, context):
        op = UniqueValidation(name="test", config={})
        result = await op.execute("new_value", context)
        assert result == "new_value"

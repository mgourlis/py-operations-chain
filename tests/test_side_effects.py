"""
Tests for side effect operations.
"""

import pytest
from operations_chain.side_effects import (
    LogValueSideEffect,
    StoreInContextSideEffect,
    IncrementCounterSideEffect,
    NotifySideEffect,
)
from operations_chain.base import PipelineContext


@pytest.fixture
def context():
    """Create a fresh pipeline context for each test."""
    return PipelineContext(shared_data={})


class TestLogValueSideEffect:
    """Tests for LogValueSideEffect."""
    
    @pytest.mark.asyncio
    async def test_returns_original_value(self, context):
        op = LogValueSideEffect(name="test", config={"level": "debug"})
        result = await op.execute("hello", context)
        assert result == "hello"
    
    @pytest.mark.asyncio
    async def test_with_message(self, context):
        op = LogValueSideEffect(name="test", config={"message": "Processing"})
        result = await op.execute(42, context)
        assert result == 42


class TestStoreInContextSideEffect:
    """Tests for StoreInContextSideEffect."""
    
    @pytest.mark.asyncio
    async def test_store_value_in_context(self, context):
        op = StoreInContextSideEffect(name="test", config={"context_path": "result"})
        result = await op.execute("stored_value", context)
        assert result == "stored_value"  # Returns original
        assert context.shared_data["result"] == "stored_value"
    
    @pytest.mark.asyncio
    async def test_store_nested_path(self, context):
        op = StoreInContextSideEffect(name="test", config={"context_path": "user.name"})
        result = await op.execute("Alice", context)
        assert context.shared_data["user"]["name"] == "Alice"
    
    @pytest.mark.asyncio
    async def test_extract_and_store(self, context):
        op = StoreInContextSideEffect(name="test", config={
            "context_path": "user_id",
            "value_path": "data.id"
        })
        result = await op.execute({"data": {"id": 123}}, context)
        assert context.shared_data["user_id"] == 123
    
    @pytest.mark.asyncio
    async def test_no_overwrite_when_false(self, context):
        context.shared_data["existing"] = "original"
        op = StoreInContextSideEffect(name="test", config={
            "context_path": "existing",
            "overwrite": False
        })
        await op.execute("new_value", context)
        assert context.shared_data["existing"] == "original"


class TestIncrementCounterSideEffect:
    """Tests for IncrementCounterSideEffect."""
    
    @pytest.mark.asyncio
    async def test_increment_counter(self, context):
        op = IncrementCounterSideEffect(name="test", config={"key": "count"})
        await op.execute("value1", context)
        assert context.shared_data["count"] == 1
        await op.execute("value2", context)
        assert context.shared_data["count"] == 2
    
    @pytest.mark.asyncio
    async def test_custom_increment(self, context):
        op = IncrementCounterSideEffect(name="test", config={
            "key": "total",
            "increment": 10
        })
        await op.execute("value", context)
        assert context.shared_data["total"] == 10
    
    @pytest.mark.asyncio
    async def test_returns_original_value(self, context):
        op = IncrementCounterSideEffect(name="test", config={})
        result = await op.execute("hello", context)
        assert result == "hello"


class TestNotifySideEffect:
    """Tests for NotifySideEffect (placeholder implementation)."""
    
    @pytest.mark.asyncio
    async def test_returns_original_value(self, context):
        op = NotifySideEffect(name="test", config={
            "channel": "email",
            "recipient": "test@example.com"
        })
        result = await op.execute("notification content", context)
        assert result == "notification content"

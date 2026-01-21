"""
Tests for pipeline execution and control flow.
"""

import pytest
from operations_chain import (
    PipelineExecutor,
    PipelineParser,
    OperationSpec,
    get_registry,
)
from operations_chain.control_flow import IfElseOperation, ExecutePipelineOnPath
from operations_chain.base import PipelineContext
from operations_chain.exceptions import ValidationError, PipelineExecutionError


@pytest.fixture
def context():
    """Create a fresh pipeline context for each test."""
    return PipelineContext(shared_data={})


class TestPipelineExecutor:
    """Tests for PipelineExecutor."""
    
    @pytest.mark.asyncio
    async def test_simple_pipeline(self):
        pipeline = [
            OperationSpec(operation="extract_field", operation_config={"field": "name"}),
            OperationSpec(operation="uppercase"),
        ]
        
        executor = PipelineExecutor(shared_data={})
        result = await executor.execute_pipeline(pipeline, {"name": "alice"})
        
        assert result == "ALICE"
    
    @pytest.mark.asyncio
    async def test_pipeline_with_shared_data(self):
        pipeline = [
            OperationSpec(operation="store", operation_config={"context_path": "original"}),
            OperationSpec(operation="uppercase"),
        ]
        
        executor = PipelineExecutor(shared_data={"user": "test"})
        result = await executor.execute_pipeline(pipeline, "hello")
        
        assert result == "HELLO"
        assert executor.context.shared_data["original"] == "hello"
        assert executor.context.shared_data["user"] == "test"
    
    @pytest.mark.asyncio
    async def test_pipeline_respects_order(self):
        pipeline = [
            OperationSpec(operation="uppercase", order_index=1),
            OperationSpec(operation="extract_field", operation_config={"field": "name"}, order_index=0),
        ]
        
        executor = PipelineExecutor()
        result = await executor.execute_pipeline(pipeline, {"name": "alice"})
        
        # Should extract first (order 0), then uppercase (order 1)
        assert result == "ALICE"
    
    @pytest.mark.asyncio
    async def test_non_required_operation_continues_on_error(self):
        pipeline = [
            OperationSpec(operation="required", is_required=False),  # Will fail on None
            OperationSpec(operation="default", operation_config={"default": "fallback"}),
        ]
        
        executor = PipelineExecutor()
        result = await executor.execute_pipeline(pipeline, None)
        
        assert result == "fallback"
    
    @pytest.mark.asyncio
    async def test_required_operation_stops_on_error(self):
        pipeline = [
            OperationSpec(operation="required", is_required=True),
            OperationSpec(operation="uppercase"),
        ]
        
        executor = PipelineExecutor()
        with pytest.raises(ValidationError):
            await executor.execute_pipeline(pipeline, None)
    
    @pytest.mark.asyncio
    async def test_execution_log(self):
        pipeline = [
            OperationSpec(operation="strip"),
            OperationSpec(operation="uppercase"),
        ]
        
        executor = PipelineExecutor()
        await executor.execute_pipeline(pipeline, "  hello  ")
        
        log = executor.get_execution_log()
        assert len(log) == 2
        assert log[0]["operation_name"] == "strip"
        assert log[1]["operation_name"] == "uppercase"
        assert all(step["success"] for step in log)


class TestPipelineParser:
    """Tests for PipelineParser."""
    
    def test_parse_from_list(self):
        json_def = [
            {"operation": "required"},
            {"operation": "uppercase"},
        ]
        
        operations = PipelineParser.from_json(json_def)
        
        assert len(operations) == 2
        assert operations[0].operation == "required"
        assert operations[1].operation == "uppercase"
    
    def test_parse_from_json_string(self):
        json_str = '[{"operation": "required"}, {"operation": "uppercase"}]'
        
        operations = PipelineParser.from_json(json_str)
        
        assert len(operations) == 2
    
    def test_parse_with_config(self):
        json_def = [
            {"operation": "range", "operation_config": {"min": 0, "max": 100}},
        ]
        
        operations = PipelineParser.from_json(json_def)
        
        assert operations[0].operation_config == {"min": 0, "max": 100}
    
    def test_parse_missing_operation_raises(self):
        json_def = [{"operation_config": {"field": "name"}}]
        
        with pytest.raises(ValueError, match="missing required 'operation' field"):
            PipelineParser.from_json(json_def)
    
    def test_validate_valid_pipeline(self):
        json_def = [
            {"operation": "required"},
            {"operation": "uppercase"},
        ]
        
        errors = PipelineParser.validate(json_def)
        assert errors == []
    
    def test_validate_unknown_operation(self):
        json_def = [{"operation": "unknown_op"}]
        
        errors = PipelineParser.validate(json_def)
        assert any("Unknown operation" in e for e in errors)
    
    def test_validate_missing_required_config(self):
        json_def = [{"operation": "extract_field"}]  # Missing "field"
        
        errors = PipelineParser.validate(json_def)
        assert any("Missing required config" in e for e in errors)


class TestIfElseOperation:
    """Tests for IfElseOperation."""
    
    @pytest.mark.asyncio
    async def test_executes_then_branch_on_pass(self, context):
        op = IfElseOperation(name="test", config={
            "condition": [{"operation": "required"}],
            "then_branch": [{"operation": "uppercase"}],
            "else_branch": [{"operation": "set", "operation_config": {"value": "N/A"}}]
        })
        
        result = await op.execute("hello", context)
        assert result == "HELLO"
    
    @pytest.mark.asyncio
    async def test_executes_else_branch_on_fail(self, context):
        op = IfElseOperation(name="test", config={
            "condition": [{"operation": "required"}],
            "then_branch": [{"operation": "uppercase"}],
            "else_branch": [{"operation": "set", "operation_config": {"value": "N/A"}}]
        })
        
        result = await op.execute(None, context)
        assert result == "N/A"
    
    @pytest.mark.asyncio
    async def test_returns_original_when_no_else(self, context):
        op = IfElseOperation(name="test", config={
            "condition": [{"operation": "required"}],
            "then_branch": [{"operation": "uppercase"}]
        })
        
        result = await op.execute(None, context)
        assert result is None


class TestExecutePipelineOnPath:
    """Tests for ExecutePipelineOnPath."""
    
    @pytest.mark.asyncio
    async def test_transforms_nested_value(self, context):
        op = ExecutePipelineOnPath(name="test", config={
            "path": "user.name",
            "pipeline": [
                {"operation": "strip"},
                {"operation": "uppercase"}
            ]
        })
        
        data = {"user": {"name": "  alice  "}}
        result = await op.execute(data, context)
        
        assert result["user"]["name"] == "ALICE"
    
    @pytest.mark.asyncio
    async def test_raises_on_invalid_path(self, context):
        op = ExecutePipelineOnPath(name="test", config={
            "path": "nonexistent.path",
            "pipeline": [{"operation": "uppercase"}]
        })
        
        with pytest.raises(ValueError, match="does not exist"):
            await op.execute({"user": {}}, context)

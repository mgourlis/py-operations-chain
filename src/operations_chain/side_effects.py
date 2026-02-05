"""
Side-effect operations that perform actions without modifying the value.

Side effects:
- Execute actions (API calls, database updates, notifications)
- Update context.shared_data for use by later operations
- Always return the input value unchanged
- May raise exceptions if critical operations fail
"""

from abc import abstractmethod
from typing import Any, Dict
import logging

from .base import BaseOperation, OperationType, PipelineContext
from .exceptions import ValidationError

logger = logging.getLogger("operations_chain")


class SideEffectOperation(BaseOperation):
    """
    Base class for side-effect operations.

    Side effects:
    - Receive input value
    - Perform external actions
    - Optionally update context.shared_data
    - Return the same value (unmodified)

    Error Handling:
    - Default: on_error='ignore' (side effects shouldn't break pipeline)
    - Configurable via config['on_error']:
      - 'ignore': Log warning and continue (default)
      - 'raise': Raise ValidationError

    Example:
        >>> class LogToFileSideEffect(SideEffectOperation):
        ...     async def perform(self, value, context):
        ...         with open('log.txt', 'a') as f:
        ...             f.write(str(value))
    """

    def get_operation_type(self) -> OperationType:
        return OperationType.SIDE_EFFECT

    @abstractmethod
    async def perform(self, value: Any, context: PipelineContext) -> None:
        """
        Perform the side effect.

        Args:
            value: Input value (passed for reference)
            context: Pipeline context (can be modified to store data)

        Returns:
            None (modifies context or external systems)
        """
        pass

    async def execute(self, value: Any, context: PipelineContext) -> Any:
        """Execute the side effect and return the original value."""
        try:
            await self.perform(value, context)
            return value  # Always return original value
        except ValidationError:
            # ValidationError already has proper error code and message
            raise
        except Exception as e:
            return self._handle_error(e, value)

    def _handle_error(self, error: Exception, original_value: Any) -> Any:
        """Handle side effect errors based on configuration."""
        on_error = self.config.get("on_error", "ignore")

        if on_error == "raise":
            logger.error(f"{self.name}: Side effect error: {str(error)}")
            raise ValidationError(
                f"{self.name}: {str(error)}", operation_name=self.name
            ) from error
        else:  # 'ignore' or any other value defaults to ignore
            logger.warning(f"{self.name}: Side effect error (ignored): {str(error)}")
            return original_value


# ============================================================================
# Concrete Side-Effect Implementations
# ============================================================================


class LogValueSideEffect(SideEffectOperation):
    """
    Log the current value for debugging.
    """

    def get_config_schema(self) -> Dict[str, Any]:
        return {
            "required": {},
            "optional": {
                "level": {
                    "type": "str",
                    "description": "Log level: debug, info, warning, or error",
                    "default": "info",
                    "example": "debug",
                },
                "message": {
                    "type": "str",
                    "description": "Optional message prefix",
                    "example": "Processing value",
                },
            },
        }

    async def perform(self, value: Any, context: PipelineContext) -> None:
        level = self.config.get("level", "info").lower()
        message = self.config.get("message", f"{self.name}")

        log_message = f"{message}: {value}"

        if level == "debug":
            logger.debug(log_message)
        elif level == "warning":
            logger.warning(log_message)
        elif level == "error":
            logger.error(log_message)
        else:  # info
            logger.info(log_message)


class StoreInContextSideEffect(SideEffectOperation):
    """
    Store all or part of the current value in context.shared_data.

    Useful for passing data between operations or for later use.
    """

    def get_config_schema(self) -> Dict[str, Any]:
        return {
            "required": {
                "context_path": {
                    "type": "str",
                    "description": "Dot-separated path to store value under in context",
                    "example": "user.id",
                }
            },
            "optional": {
                "value_path": {
                    "type": "str",
                    "description": "Optional dot-separated path to extract value from input",
                    "example": "data.user_id",
                },
                "overwrite": {
                    "type": "bool",
                    "description": "If False, do not overwrite existing value",
                    "default": True,
                },
            },
        }

    async def perform(self, value: Any, context: PipelineContext) -> None:
        context_path = self.config.get("context_path")
        value_path = self.config.get("value_path")
        overwrite = self.config.get("overwrite", True)

        if not context_path:
            logger.warning(
                f"{self.name}: 'context_path' not specified in configuration."
            )
            return

        # Determine the data to be stored
        data_to_store = value
        if value_path:
            if not isinstance(value, dict):
                logger.warning(
                    f"{self.name}: 'value_path' is set, but input is not a dict."
                )
                return

            # Traverse the value dictionary
            current_data = value
            try:
                for key in value_path.split("."):
                    current_data = current_data[key]
                data_to_store = current_data
            except (KeyError, TypeError):
                logger.warning(
                    f"{self.name}: 'value_path' '{value_path}' not found in input."
                )
                return

        # Store in context at the specified path
        keys = context_path.split(".")
        target_dict = context.shared_data
        for key in keys[:-1]:
            target_dict = target_dict.setdefault(key, {})
            if not isinstance(target_dict, dict):
                logger.warning(
                    f"{self.name}: Context path '{context_path}' conflicts with non-dict value."
                )
                return

        final_key = keys[-1]

        if not overwrite and final_key in target_dict:
            logger.debug(
                f"{self.name}: Context path '{context_path}' exists and overwrite=false. Skipping."
            )
            return

        target_dict[final_key] = data_to_store
        logger.debug(f"'{self.name}': Stored value in context at path '{context_path}'")


class IncrementCounterSideEffect(SideEffectOperation):
    """
    Increment a counter in context.

    Useful for counting processed items.
    """

    def get_config_schema(self) -> Dict[str, Any]:
        return {
            "required": {},
            "optional": {
                "key": {
                    "type": "str",
                    "description": "Counter key in context",
                    "default": "counter",
                    "example": "processed_count",
                },
                "increment": {
                    "type": "int",
                    "description": "Amount to increment by",
                    "default": 1,
                    "example": 1,
                },
            },
        }

    async def perform(self, value: Any, context: PipelineContext) -> None:
        key = self.config.get("key", "counter")
        increment = self.config.get("increment", 1)

        current = context.shared_data.get(key, 0)
        context.shared_data[key] = current + increment


class HttpRequestSideEffect(SideEffectOperation):
    """
    Make an HTTP request.

    Requires the 'aiohttp' package to be installed:
        pip install operations-chain[http]
    """

    def get_config_schema(self) -> Dict[str, Any]:
        return {
            "required": {
                "url": {
                    "type": "str",
                    "description": "URL (can use {value} placeholder)",
                    "example": "https://api.example.com/items/{value}",
                }
            },
            "optional": {
                "method": {
                    "type": "str",
                    "description": "HTTP method: GET, POST, PUT, or DELETE",
                    "default": "GET",
                },
                "headers": {
                    "type": "dict",
                    "description": "Dict of HTTP headers",
                    "example": {"Authorization": "Bearer token"},
                },
                "body_template": {
                    "type": "str",
                    "description": "Body template (for POST/PUT)",
                    "example": '{"id": "{value}"}',
                },
                "store_response_key": {
                    "type": "str",
                    "description": "Store response in context under this key",
                    "example": "api_response",
                },
                "timeout": {
                    "type": "int",
                    "description": "Request timeout in seconds",
                    "default": 30,
                },
            },
        }

    async def perform(self, value: Any, context: PipelineContext) -> None:
        try:
            import aiohttp
        except ImportError:
            raise ImportError(
                "aiohttp is required for HttpRequestSideEffect. "
                "Install it with: pip install operations-chain[http]"
            )

        method = self.config.get("method", "GET").upper()
        url_template = self.config.get("url", "")
        headers = self.config.get("headers", {})
        body_template = self.config.get("body_template")
        store_response_key = self.config.get("store_response_key")
        timeout = self.config.get("timeout", 30)

        # Format URL
        url = url_template.format(value=value, **context.shared_data)

        # Prepare request kwargs
        request_kwargs = {
            "headers": headers,
            "timeout": aiohttp.ClientTimeout(total=timeout),
        }

        # Add body for POST/PUT
        if method in ["POST", "PUT"] and body_template:
            body = body_template.format(value=value, **context.shared_data)
            request_kwargs["data"] = body

        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(method, url, **request_kwargs) as response:
                    response_data = await response.text()
                    logger.info(f"{self.name}: {method} {url} -> {response.status}")

                    if store_response_key:
                        context.shared_data[store_response_key] = {
                            "status": response.status,
                            "data": response_data,
                        }
        except Exception as e:
            logger.error(f"{self.name}: HTTP request failed: {e}")
            raise


class NotifySideEffect(SideEffectOperation):
    """
    Placeholder for notifications.

    Logs the notification message. Override or extend for actual implementation.
    """

    def get_config_schema(self) -> Dict[str, Any]:
        return {
            "required": {
                "channel": {
                    "type": "str",
                    "description": "Notification channel: email, sms, or webhook",
                    "example": "email",
                },
                "recipient": {
                    "type": "str",
                    "description": "Recipient address",
                    "example": "user@example.com",
                },
            },
            "optional": {
                "message": {
                    "type": "str",
                    "description": "Message template",
                    "default": "{value}",
                    "example": "New item created: {value}",
                }
            },
        }

    async def perform(self, value: Any, context: PipelineContext) -> None:
        channel = self.config.get("channel", "email")
        recipient = self.config.get("recipient")
        message_template = self.config.get("message", "{value}")

        message = message_template.format(value=value, **context.shared_data)

        logger.info(f"{self.name}: Would send {channel} to {recipient}: {message}")
        # TODO: Implement actual notification sending

"""
Telegram Notification Operator for Apache Airflow.

Custom operator for sending notifications via Telegram Bot API.
Supports Markdown/HTML formatting, silent notifications, and message length handling.
"""

import json
from typing import Any

import requests
from airflow.exceptions import AirflowException

from src.operators.notifications.base_notification import BaseNotificationOperator
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TelegramNotificationOperator(BaseNotificationOperator):
    """
    Operator for sending notifications via Telegram Bot API.

    Supports message formatting (Markdown/HTML), silent notifications,
    and automatic handling of Telegram's 4096 character limit.

    :param bot_token: Telegram Bot API token (format: "123456:ABC-DEF...")
    :param chat_id: Telegram chat ID (can be user ID or group chat ID)
    :param message_template: Message text template (supports Jinja2)
    :param parse_mode: Message parsing mode ("Markdown", "HTML", or None)
    :param disable_notification: Send message silently (no notification sound)
    :param disable_web_page_preview: Disable link previews in message
    :param timeout: Request timeout in seconds
    """

    template_fields = ("message_template", "chat_id")
    ui_color = "#0088cc"  # Telegram blue

    # Telegram message length limit
    MAX_MESSAGE_LENGTH = 4096


    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: str,
        message_template: str,
        parse_mode: str | None = None,
        disable_notification: bool = False,
        disable_web_page_preview: bool = False,
        timeout: int = 30,
        **kwargs,
    ):
        """Initialize TelegramNotificationOperator."""
        super().__init__(message_template=message_template, **kwargs)

        # Validate bot token format
        if not bot_token or ":" not in bot_token:
            raise ValueError(
                f"Invalid bot_token format: {bot_token[:10]}... "
                "Bot token must be in format '123456:ABC-DEF...'"
            )

        # Validate chat_id (can be numeric or string with minus sign for groups)
        if not chat_id:
            raise ValueError("chat_id parameter is required")

        # Validate parse_mode if provided
        if parse_mode and parse_mode not in ["Markdown", "HTML", "MarkdownV2"]:
            raise ValueError(
                f"Invalid parse_mode: {parse_mode}. "
                "Must be 'Markdown', 'HTML', 'MarkdownV2', or None"
            )

        self.bot_token = bot_token
        self.chat_id = chat_id
        self.parse_mode = parse_mode
        self.disable_notification = disable_notification
        self.disable_web_page_preview = disable_web_page_preview
        self.timeout = timeout

        # Build API endpoint
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

        logger.info(
            f"Telegram operator initialized for chat_id: {chat_id}, " f"parse_mode: {parse_mode}"
        )

    def _truncate_message(self, message: str) -> str:
        """
        Truncate message to Telegram's character limit.

        :param message: Original message
        :return: Truncated message if needed
        """
        if len(message) <= self.MAX_MESSAGE_LENGTH:
            return message

        logger.warning(
            f"Message length {len(message)} exceeds Telegram limit "
            f"{self.MAX_MESSAGE_LENGTH}. Truncating."
        )

        # Truncate with ellipsis
        truncated = message[: self.MAX_MESSAGE_LENGTH - 20] + "\n\n[...truncated]"
        return truncated

    def _build_payload(self, message: str) -> dict[str, Any]:
        """
        Build Telegram sendMessage API payload.

        :param message: Message text (already rendered)
        :return: API request payload
        """
        payload = {
            "chat_id": self.chat_id,
            "text": self._truncate_message(message),
        }

        # Add optional parameters
        if self.parse_mode:
            payload["parse_mode"] = self.parse_mode

        if self.disable_notification:
            payload["disable_notification"] = True

        if self.disable_web_page_preview:
            payload["disable_web_page_preview"] = True

        return payload

    def send_notification(self, message: str, context: dict[str, Any]) -> bool:
        """
        Send notification via Telegram Bot API.

        :param message: Rendered message content
        :param context: Airflow context dictionary
        :return: True if notification sent successfully
        :raises AirflowException: If API request fails
        """
        try:
            # Build API payload
            payload = self._build_payload(message)

            logger.info(
                "Sending Telegram notification",
                extra={
                    "chat_id": self.chat_id,
                    "message_length": len(message),
                    "parse_mode": self.parse_mode,
                },
            )

            # Send POST request to Telegram API
            response = requests.post(
                self.api_url,
                json=payload,
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
            )

            # Parse response
            response_data = response.json()

            # Check if request was successful
            if response.ok and response_data.get("ok"):
                message_id = response_data.get("result", {}).get("message_id")
                logger.info(
                    "Telegram notification sent successfully", extra={"message_id": message_id}
                )
                return True
            else:
                error_code = response_data.get("error_code")
                error_description = response_data.get("description", "Unknown error")

                # Handle rate limiting specifically
                if error_code == 429:
                    retry_after = response_data.get("parameters", {}).get("retry_after", 30)
                    error_msg = (
                        f"Telegram rate limit hit. Retry after {retry_after} seconds. "
                        f"{error_description}"
                    )
                    logger.warning(error_msg)
                    raise AirflowException(error_msg)

                error_msg = f"Telegram API returned error: {error_code} - {error_description}"
                logger.error(error_msg)
                raise AirflowException(error_msg)

        except requests.Timeout as e:
            logger.error(f"Telegram API request timed out: {str(e)}")
            raise AirflowException(f"Telegram API timeout: {str(e)}") from e

        except requests.ConnectionError as e:
            logger.error(f"Telegram API connection error: {str(e)}")
            raise AirflowException(f"Telegram API connection failed: {str(e)}") from e

        except requests.RequestException as e:
            logger.error(f"Telegram API request failed: {str(e)}")
            raise AirflowException(f"Telegram API error: {str(e)}") from e

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Telegram API response: {str(e)}")
            raise AirflowException(f"Telegram API response parsing failed: {str(e)}") from e

        except Exception as e:
            logger.error(f"Unexpected error sending Telegram notification: {str(e)}", exc_info=True)
            raise AirflowException(f"Telegram notification failed: {str(e)}") from e

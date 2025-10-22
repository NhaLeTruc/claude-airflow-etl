"""
Unit tests for TelegramNotificationOperator.

Tests cover bot API integration, message formatting, chat ID validation,
parse modes (Markdown/HTML), and silent notification options.
"""

import json
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from airflow.exceptions import AirflowException


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    return {
        "dag": Mock(dag_id="test_dag"),
        "task": Mock(task_id="telegram_task"),
        "execution_date": datetime(2025, 1, 15, 10, 0, 0),
        "run_id": "test_run_123",
        "task_instance": Mock(try_number=1, max_tries=3),
        "ds": "2025-01-15",
    }


class TestTelegramNotificationOperator:
    """Test suite for TelegramNotificationOperator."""

    def test_operator_initialization(self):
        """Test operator initializes with bot token and chat ID."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="-1001234567890",
                message_template="Test message",
            )

            assert operator.task_id == "send_telegram"
            assert operator.bot_token == "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
            assert operator.chat_id == "-1001234567890"
            assert operator.message_template == "Test message"
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    def test_bot_token_validation(self):
        """Test that bot token format is validated."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            # Valid token format
            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template="Test",
            )

            assert ":" in operator.bot_token
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    def test_invalid_bot_token_format(self):
        """Test that invalid bot token raises error."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            with pytest.raises((ValueError, AirflowException)):
                TelegramNotificationOperator(
                    task_id="send_telegram",
                    bot_token="invalid-token",  # Missing colon separator
                    chat_id="12345",
                    message_template="Test",
                )
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    def test_chat_id_validation_numeric(self):
        """Test validation of numeric chat ID."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="123456789",
                message_template="Test",
            )

            assert operator.chat_id == "123456789"
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    def test_chat_id_validation_group(self):
        """Test validation of group chat ID (negative number)."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="-1001234567890",  # Group chat
                message_template="Test",
            )

            assert operator.chat_id.startswith("-")
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_send_message_api_call_success(self, mock_post, mock_context):
        """Test successful Telegram Bot API sendMessage call."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_response.json.return_value = {"ok": True, "result": {"message_id": 123}}
            mock_post.return_value = mock_response

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template="Test message: {{ dag.dag_id }}",
            )

            operator.execute(mock_context)

            # Verify API endpoint
            call_args = mock_post.call_args
            assert "sendMessage" in call_args[0][0]
            assert "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11" in call_args[0][0]
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_send_message_api_call_failure(self, mock_post, mock_context):
        """Test Telegram Bot API error handling."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 400
            mock_response.ok = False
            mock_response.json.return_value = {
                "ok": False,
                "error_code": 400,
                "description": "Bad Request: chat not found",
            }
            mock_post.return_value = mock_response

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="invalid_chat",
                message_template="Test",
            )

            with pytest.raises(AirflowException):
                operator.execute(mock_context)
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_markdown_parse_mode(self, mock_post, mock_context):
        """Test Markdown parse mode for message formatting."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_response.json.return_value = {"ok": True}
            mock_post.return_value = mock_response

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template="*Bold* and _italic_ text",
                parse_mode="Markdown",
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = (
                call_args[1]["json"] if "json" in call_args[1] else json.loads(call_args[1]["data"])
            )

            assert payload["parse_mode"] == "Markdown"
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_html_parse_mode(self, mock_post, mock_context):
        """Test HTML parse mode for message formatting."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_response.json.return_value = {"ok": True}
            mock_post.return_value = mock_response

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template="<b>Bold</b> and <i>italic</i> text",
                parse_mode="HTML",
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = (
                call_args[1]["json"] if "json" in call_args[1] else json.loads(call_args[1]["data"])
            )

            assert payload["parse_mode"] == "HTML"
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_silent_notification_option(self, mock_post, mock_context):
        """Test silent notification (disable notification sound)."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_response.json.return_value = {"ok": True}
            mock_post.return_value = mock_response

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template="Silent notification",
                disable_notification=True,
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = (
                call_args[1]["json"] if "json" in call_args[1] else json.loads(call_args[1]["data"])
            )

            assert payload.get("disable_notification") is True
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_template_rendering_in_message(self, mock_post, mock_context):
        """Test Jinja2 template rendering in message."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_response.json.return_value = {"ok": True}
            mock_post.return_value = mock_response

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template="DAG: {{ dag.dag_id }}, Date: {{ ds }}",
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = (
                call_args[1]["json"] if "json" in call_args[1] else json.loads(call_args[1]["data"])
            )

            # Verify template was rendered
            assert "test_dag" in payload["text"]
            assert "2025-01-15" in payload["text"]
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_disable_web_page_preview(self, mock_post, mock_context):
        """Test disabling web page preview for links."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_response.json.return_value = {"ok": True}
            mock_post.return_value = mock_response

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template="Check: https://example.com",
                disable_web_page_preview=True,
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = (
                call_args[1]["json"] if "json" in call_args[1] else json.loads(call_args[1]["data"])
            )

            assert payload.get("disable_web_page_preview") is True
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_connection_timeout_handling(self, mock_post, mock_context):
        """Test handling of connection timeout errors."""
        try:
            import requests

            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            mock_post.side_effect = requests.Timeout("Connection timeout")

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template="Test",
            )

            with pytest.raises(AirflowException):
                operator.execute(mock_context)
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_network_error_handling(self, mock_post, mock_context):
        """Test handling of network connection errors."""
        try:
            import requests

            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            mock_post.side_effect = requests.ConnectionError("Network unreachable")

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template="Test",
            )

            with pytest.raises(AirflowException):
                operator.execute(mock_context)
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    def test_template_fields_defined(self):
        """Test that template_fields includes relevant fields."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            assert hasattr(TelegramNotificationOperator, "template_fields")
            template_fields = TelegramNotificationOperator.template_fields

            # Should include message_template
            assert "message_template" in template_fields
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    def test_ui_color_for_telegram_operator(self):
        """Test that Telegram operator has distinct UI color."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            assert hasattr(TelegramNotificationOperator, "ui_color")
            # Telegram typically uses blue colors
            assert TelegramNotificationOperator.ui_color.startswith("#")
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_message_length_limit_handling(self, mock_post, mock_context):
        """Test handling of Telegram's 4096 character limit."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_response.json.return_value = {"ok": True}
            mock_post.return_value = mock_response

            # Create a very long message
            long_message = "A" * 5000  # Exceeds 4096 limit

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template=long_message,
            )

            # Should either truncate or split the message
            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = (
                call_args[1]["json"] if "json" in call_args[1] else json.loads(call_args[1]["data"])
            )

            # Message should be truncated or operator should raise error
            assert len(payload["text"]) <= 4096 or mock_post.call_count > 1
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

    @patch("requests.post")
    def test_retry_on_rate_limit(self, mock_post, mock_context):
        """Test retry behavior on rate limit (429 error)."""
        try:
            from src.operators.notifications.telegram_operator import TelegramNotificationOperator

            # First attempt hits rate limit, second succeeds
            mock_response_fail = Mock()
            mock_response_fail.status_code = 429
            mock_response_fail.ok = False
            mock_response_fail.json.return_value = {
                "ok": False,
                "error_code": 429,
                "description": "Too Many Requests: retry after 30",
                "parameters": {"retry_after": 1},
            }

            mock_response_success = Mock()
            mock_response_success.status_code = 200
            mock_response_success.ok = True
            mock_response_success.json.return_value = {"ok": True}

            mock_post.side_effect = [mock_response_fail, mock_response_success]

            operator = TelegramNotificationOperator(
                task_id="send_telegram",
                bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
                chat_id="12345",
                message_template="Test",
                retries=2,
            )

            # Should eventually succeed after retry
            operator.execute(mock_context)
        except ImportError:
            pytest.skip("TelegramNotificationOperator not implemented yet")

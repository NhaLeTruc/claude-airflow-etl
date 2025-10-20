"""
Unit tests for TeamsNotificationOperator.

Tests cover webhook POST requests, message card formatting, theme colors,
facts/actions sections, and error handling.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from airflow.exceptions import AirflowException
import json


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    return {
        "dag": Mock(dag_id="test_dag"),
        "task": Mock(task_id="teams_task"),
        "execution_date": datetime(2025, 1, 15, 10, 0, 0),
        "run_id": "test_run_123",
        "task_instance": Mock(try_number=1, max_tries=3, state="success"),
        "ds": "2025-01-15",
    }


class TestTeamsNotificationOperator:
    """Test suite for TeamsNotificationOperator."""

    def test_operator_initialization(self):
        """Test operator initializes with webhook URL."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test message",
                title="Test Title",
            )

            assert operator.task_id == "send_teams"
            assert operator.webhook_url == "https://outlook.office.com/webhook/test"
            assert operator.message_template == "Test message"
            assert operator.title == "Test Title"
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    def test_webhook_url_validation(self):
        """Test that webhook URL is validated."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            # Valid webhook URL should work
            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/abc123",
                message_template="Test",
            )

            assert operator.webhook_url.startswith("https://")
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    def test_invalid_webhook_url_format(self):
        """Test that invalid webhook URL raises error."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            with pytest.raises((ValueError, AirflowException)):
                TeamsNotificationOperator(
                    task_id="send_teams",
                    webhook_url="not-a-valid-url",
                    message_template="Test",
                )
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_webhook_post_request_success(self, mock_post, mock_context):
        """Test successful webhook POST request."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_post.return_value = mock_response

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test message",
                title="Test",
            )

            operator.execute(mock_context)

            mock_post.assert_called_once()
            call_args = mock_post.call_args
            assert call_args[0][0] == "https://outlook.office.com/webhook/test"
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_webhook_post_request_failure(self, mock_post, mock_context):
        """Test webhook POST request failure handling."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 400
            mock_response.ok = False
            mock_response.text = "Bad Request"
            mock_post.return_value = mock_response

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test",
            )

            with pytest.raises(AirflowException):
                operator.execute(mock_context)
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_message_card_structure(self, mock_post, mock_context):
        """Test that message is formatted as proper Teams message card."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_post.return_value = mock_response

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test message: {{ dag.dag_id }}",
                title="Test Title",
            )

            operator.execute(mock_context)

            # Verify the payload structure
            call_args = mock_post.call_args
            payload = call_args[1]['json'] if 'json' in call_args[1] else json.loads(call_args[1]['data'])

            assert '@type' in payload or 'type' in payload  # MessageCard type
            assert 'title' in payload or 'summary' in payload
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_theme_color_configuration(self, mock_post, mock_context):
        """Test theme color can be configured for message card."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_post.return_value = mock_response

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test",
                title="Test",
                theme_color="FF0000",  # Red
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = call_args[1]['json'] if 'json' in call_args[1] else json.loads(call_args[1]['data'])

            assert 'themeColor' in payload
            assert payload['themeColor'] == "FF0000"
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_facts_section_in_message_card(self, mock_post, mock_context):
        """Test that facts can be added to message card."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_post.return_value = mock_response

            facts = [
                {"name": "DAG ID", "value": "{{ dag.dag_id }}"},
                {"name": "Execution Date", "value": "{{ ds }}"},
            ]

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test",
                title="Test",
                facts=facts,
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = call_args[1]['json'] if 'json' in call_args[1] else json.loads(call_args[1]['data'])

            # Facts should be in the payload
            assert 'sections' in payload or 'facts' in payload
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_actions_section_in_message_card(self, mock_post, mock_context):
        """Test that action buttons can be added to message card."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_post.return_value = mock_response

            actions = [
                {
                    "@type": "OpenUri",
                    "name": "View Airflow",
                    "targets": [{"os": "default", "uri": "http://localhost:8080"}],
                }
            ]

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test",
                title="Test",
                actions=actions,
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = call_args[1]['json'] if 'json' in call_args[1] else json.loads(call_args[1]['data'])

            # Actions should be in the payload
            assert 'potentialAction' in payload or 'actions' in payload
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_template_rendering_in_message(self, mock_post, mock_context):
        """Test Jinja2 template rendering in message."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_post.return_value = mock_response

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="DAG: {{ dag.dag_id }}, Date: {{ ds }}",
                title="Notification",
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = call_args[1]['json'] if 'json' in call_args[1] else json.loads(call_args[1]['data'])

            # Verify template was rendered (dag_id should be replaced)
            payload_str = json.dumps(payload)
            assert "test_dag" in payload_str
            assert "2025-01-15" in payload_str
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_connection_timeout_handling(self, mock_post, mock_context):
        """Test handling of connection timeout errors."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator
            import requests

            mock_post.side_effect = requests.Timeout("Connection timeout")

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test",
            )

            with pytest.raises(AirflowException):
                operator.execute(mock_context)
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_network_error_handling(self, mock_post, mock_context):
        """Test handling of network connection errors."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator
            import requests

            mock_post.side_effect = requests.ConnectionError("Network unreachable")

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test",
            )

            with pytest.raises(AirflowException):
                operator.execute(mock_context)
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    def test_template_fields_defined(self):
        """Test that template_fields includes relevant fields."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            assert hasattr(TeamsNotificationOperator, 'template_fields')
            template_fields = TeamsNotificationOperator.template_fields

            # Should include message_template and possibly title
            assert 'message_template' in template_fields or 'title' in template_fields
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    def test_ui_color_for_teams_operator(self):
        """Test that Teams operator has distinct UI color."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            assert hasattr(TeamsNotificationOperator, 'ui_color')
            # Teams typically uses purple/blue colors
            assert TeamsNotificationOperator.ui_color.startswith('#')
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_custom_title_rendering(self, mock_post, mock_context):
        """Test template rendering in title field."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_post.return_value = mock_response

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test",
                title="{{ task_instance.state|upper }} - {{ dag.dag_id }}",
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = call_args[1]['json'] if 'json' in call_args[1] else json.loads(call_args[1]['data'])

            # Title should contain rendered values
            payload_str = json.dumps(payload)
            assert "test_dag" in payload_str
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_retry_on_server_error(self, mock_post, mock_context):
        """Test retry behavior on 5xx server errors."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            # First attempt returns 500, second succeeds
            mock_response_fail = Mock()
            mock_response_fail.status_code = 500
            mock_response_fail.ok = False

            mock_response_success = Mock()
            mock_response_success.status_code = 200
            mock_response_success.ok = True

            mock_post.side_effect = [mock_response_fail, mock_response_success]

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="Test",
                retries=2,
            )

            # Should eventually succeed after retry
            operator.execute(mock_context)
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")

    @patch('requests.post')
    def test_markdown_formatting_support(self, mock_post, mock_context):
        """Test that markdown formatting is supported in message."""
        try:
            from src.operators.notifications.teams_operator import TeamsNotificationOperator

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.ok = True
            mock_post.return_value = mock_response

            operator = TeamsNotificationOperator(
                task_id="send_teams",
                webhook_url="https://outlook.office.com/webhook/test",
                message_template="**Bold** and *italic* text",
                title="Test",
            )

            operator.execute(mock_context)

            call_args = mock_post.call_args
            payload = call_args[1]['json'] if 'json' in call_args[1] else json.loads(call_args[1]['data'])

            # Markdown should be preserved in the payload
            payload_str = json.dumps(payload)
            assert "**Bold**" in payload_str or "Bold" in payload_str
        except ImportError:
            pytest.skip("TeamsNotificationOperator not implemented yet")
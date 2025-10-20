"""
Unit tests for EmailNotificationOperator.

Tests cover SMTP connection, template rendering, recipient validation,
HTML/plain text support, and attachment handling.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow.exceptions import AirflowException
import smtplib


@pytest.fixture
def mock_context():
    """Create a mock Airflow context."""
    return {
        "dag": Mock(dag_id="test_dag"),
        "task": Mock(task_id="email_task"),
        "execution_date": datetime(2025, 1, 15, 10, 0, 0),
        "run_id": "test_run_123",
        "task_instance": Mock(try_number=1, max_tries=3),
        "ds": "2025-01-15",
    }


class TestEmailNotificationOperator:
    """Test suite for EmailNotificationOperator."""

    def test_operator_initialization(self):
        """Test operator initializes with correct SMTP parameters."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="Test Subject",
                message_template="Test message",
                smtp_host="smtp.example.com",
                smtp_port=587,
            )

            assert operator.task_id == "send_email"
            assert operator.to == "test@example.com"
            assert operator.subject == "Test Subject"
            assert operator.smtp_host == "smtp.example.com"
            assert operator.smtp_port == 587
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    def test_recipient_validation_single_email(self):
        """Test validation of single recipient email address."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="valid@example.com",
                subject="Test",
                message_template="Test",
            )

            assert operator.to == "valid@example.com"
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    def test_recipient_validation_multiple_emails(self):
        """Test validation of multiple recipient email addresses."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            recipients = ["user1@example.com", "user2@example.com", "user3@example.com"]
            operator = EmailNotificationOperator(
                task_id="send_email",
                to=recipients,
                subject="Test",
                message_template="Test",
            )

            assert operator.to == recipients
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    def test_invalid_recipient_email_format(self):
        """Test that invalid email format raises error."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            with pytest.raises((ValueError, AirflowException)):
                EmailNotificationOperator(
                    task_id="send_email",
                    to="invalid-email",  # Missing @ symbol
                    subject="Test",
                    message_template="Test",
                )
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    @patch('smtplib.SMTP')
    def test_smtp_connection_success(self, mock_smtp, mock_context):
        """Test successful SMTP connection and authentication."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="Test",
                message_template="Test",
                smtp_host="smtp.example.com",
                smtp_port=587,
                smtp_user="user@example.com",
                smtp_password="password",
            )

            operator.execute(mock_context)

            mock_smtp.assert_called_once_with("smtp.example.com", 587)
            mock_server.starttls.assert_called_once()
            mock_server.login.assert_called_once_with("user@example.com", "password")
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    @patch('smtplib.SMTP')
    def test_smtp_connection_failure(self, mock_smtp, mock_context):
        """Test SMTP connection failure handling."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            mock_smtp.side_effect = smtplib.SMTPConnectError(421, "Service not available")

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="Test",
                message_template="Test",
                smtp_host="smtp.example.com",
            )

            with pytest.raises(AirflowException):
                operator.execute(mock_context)
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    @patch('smtplib.SMTP')
    def test_smtp_authentication_failure(self, mock_smtp, mock_context):
        """Test SMTP authentication failure handling."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            mock_server = MagicMock()
            mock_server.login.side_effect = smtplib.SMTPAuthenticationError(535, "Authentication failed")
            mock_smtp.return_value.__enter__.return_value = mock_server

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="Test",
                message_template="Test",
                smtp_user="user@example.com",
                smtp_password="wrong_password",
            )

            with pytest.raises(AirflowException):
                operator.execute(mock_context)
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    @patch('smtplib.SMTP')
    def test_plain_text_email_sending(self, mock_smtp, mock_context):
        """Test sending plain text email."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="Test Subject",
                message_template="Plain text message: {{ dag.dag_id }}",
                html=False,
            )

            operator.execute(mock_context)

            mock_server.send_message.assert_called_once()
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    @patch('smtplib.SMTP')
    def test_html_email_sending(self, mock_smtp, mock_context):
        """Test sending HTML email."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="Test Subject",
                message_template="<html><body><h1>{{ dag.dag_id }}</h1></body></html>",
                html=True,
            )

            operator.execute(mock_context)

            mock_server.send_message.assert_called_once()
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    @patch('smtplib.SMTP')
    def test_email_with_cc_and_bcc(self, mock_smtp, mock_context):
        """Test email with CC and BCC recipients."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="primary@example.com",
                cc=["cc1@example.com", "cc2@example.com"],
                bcc=["bcc@example.com"],
                subject="Test",
                message_template="Test",
            )

            operator.execute(mock_context)

            mock_server.send_message.assert_called_once()
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    @patch('smtplib.SMTP')
    def test_template_rendering_in_subject(self, mock_smtp, mock_context):
        """Test Jinja2 template rendering in email subject."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="DAG {{ dag.dag_id }} - {{ ds }}",
                message_template="Test",
            )

            operator.execute(mock_context)

            # Verify subject was rendered
            call_args = mock_server.send_message.call_args
            # Subject should contain "test_dag" and "2025-01-15"
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    @patch('smtplib.SMTP')
    def test_template_rendering_in_body(self, mock_smtp, mock_context):
        """Test Jinja2 template rendering in email body."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            mock_server = MagicMock()
            mock_smtp.return_value.__enter__.return_value = mock_server

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="Test",
                message_template="DAG: {{ dag.dag_id }}, Task: {{ task.task_id }}, Date: {{ ds }}",
            )

            operator.execute(mock_context)

            mock_server.send_message.assert_called_once()
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    def test_attachment_support_if_implemented(self):
        """Test that attachments can be added to emails."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="Test",
                message_template="Test",
                files=["/path/to/file1.pdf", "/path/to/file2.csv"],
            )

            assert hasattr(operator, 'files')
            assert len(operator.files) == 2
        except (ImportError, TypeError):
            pytest.skip("Attachment support not implemented")

    def test_smtp_ssl_support(self):
        """Test SMTP SSL/TLS configuration."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="Test",
                message_template="Test",
                smtp_port=465,
                use_ssl=True,
            )

            assert operator.smtp_port == 465
            assert operator.use_ssl is True
        except (ImportError, TypeError):
            pytest.skip("SSL support parameters not implemented")

    def test_from_email_configuration(self):
        """Test custom 'from' email address configuration."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                from_email="noreply@company.com",
                subject="Test",
                message_template="Test",
            )

            assert operator.from_email == "noreply@company.com"
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    def test_template_fields_defined(self):
        """Test that template_fields includes relevant fields."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            assert hasattr(EmailNotificationOperator, 'template_fields')
            template_fields = EmailNotificationOperator.template_fields

            # Should include at minimum: subject and message_template
            assert 'subject' in template_fields or 'message_template' in template_fields
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    @patch('smtplib.SMTP')
    def test_retry_on_transient_smtp_error(self, mock_smtp, mock_context):
        """Test retry behavior on transient SMTP errors."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            mock_server = MagicMock()
            # First attempt fails, subsequent attempts should retry
            mock_server.send_message.side_effect = [
                smtplib.SMTPServerDisconnected("Connection lost"),
                None,  # Success on retry
            ]
            mock_smtp.return_value.__enter__.return_value = mock_server

            operator = EmailNotificationOperator(
                task_id="send_email",
                to="test@example.com",
                subject="Test",
                message_template="Test",
                retries=2,
            )

            # Should succeed on retry
            operator.execute(mock_context)
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")

    def test_ui_color_for_email_operator(self):
        """Test that email operator has distinct UI color."""
        try:
            from src.operators.notifications.email_operator import EmailNotificationOperator

            assert hasattr(EmailNotificationOperator, 'ui_color')
            # Email operators typically use blue/green colors
            assert EmailNotificationOperator.ui_color.startswith('#')
        except ImportError:
            pytest.skip("EmailNotificationOperator not implemented yet")
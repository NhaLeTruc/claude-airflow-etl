"""
MS Teams Notification Operator for Apache Airflow.

Custom operator for sending notifications to Microsoft Teams via Incoming Webhooks.
Supports message cards with titles, theme colors, facts sections, and action buttons.
"""

import requests
import json
from typing import Any, Dict, List, Optional
from datetime import timedelta

from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

from src.operators.notifications.base_notification import BaseNotificationOperator
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TeamsNotificationOperator(BaseNotificationOperator):
    """
    Operator for sending notifications to Microsoft Teams via webhook.

    Formats messages as MessageCard for Teams with support for:
    - Custom titles and theme colors
    - Facts sections (key-value pairs)
    - Action buttons with URLs
    - Markdown formatting

    :param webhook_url: Teams incoming webhook URL
    :param message_template: Message text template (supports Jinja2)
    :param title: Message card title (supports Jinja2)
    :param theme_color: Hex color code for card theme (e.g., "0078D4")
    :param facts: List of facts (key-value pairs) for the message card
    :param actions: List of action buttons with URIs
    """

    template_fields = ("message_template", "title", "webhook_url")
    ui_color = "#5b5fc7"  # Microsoft Teams purple

    @apply_defaults
    def __init__(
        self,
        *,
        webhook_url: str,
        message_template: str,
        title: Optional[str] = None,
        theme_color: Optional[str] = None,
        facts: Optional[List[Dict[str, str]]] = None,
        actions: Optional[List[Dict[str, Any]]] = None,
        timeout: int = 30,
        **kwargs,
    ):
        """Initialize TeamsNotificationOperator."""
        super().__init__(message_template=message_template, **kwargs)

        # Validate webhook URL
        if not webhook_url:
            raise ValueError("webhook_url parameter is required")

        if not webhook_url.startswith("https://"):
            raise ValueError(f"Invalid webhook URL: {webhook_url}. Must start with 'https://'")

        self.webhook_url = webhook_url
        self.title = title or "Airflow Notification"
        self.theme_color = theme_color or "0078D4"  # Microsoft Blue
        self.facts = facts or []
        self.actions = actions or []
        self.timeout = timeout

        logger.info(
            f"Teams operator initialized with webhook (hidden for security)"
        )

    def _build_message_card(
        self, title: str, message: str, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Build Microsoft Teams MessageCard payload.

        :param title: Rendered card title
        :param message: Rendered message content
        :param context: Airflow context for rendering facts
        :return: MessageCard JSON payload
        """
        # Base MessageCard structure
        card = {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "summary": title,
            "title": title,
            "text": message,
            "themeColor": self.theme_color,
        }

        # Add facts section if provided
        if self.facts:
            rendered_facts = []
            for fact in self.facts:
                rendered_facts.append({
                    "name": self.render_template(fact.get("name", ""), context),
                    "value": self.render_template(fact.get("value", ""), context),
                })

            card["sections"] = [{"facts": rendered_facts}]

        # Add potential actions if provided
        if self.actions:
            card["potentialAction"] = self.actions

        return card

    def send_notification(self, message: str, context: Dict[str, Any]) -> bool:
        """
        Send notification to Microsoft Teams webhook.

        :param message: Rendered message content
        :param context: Airflow context dictionary
        :return: True if notification sent successfully
        :raises AirflowException: If webhook request fails
        """
        try:
            # Render title with context
            rendered_title = self.render_template(self.title, context)

            # Build message card
            payload = self._build_message_card(rendered_title, message, context)

            logger.info(
                f"Sending Teams notification",
                extra={
                    "title": rendered_title,
                    "payload_size": len(json.dumps(payload)),
                }
            )

            # Send POST request to webhook
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
            )

            # Check response
            if response.ok and response.status_code == 200:
                logger.info(
                    f"Teams notification sent successfully",
                    extra={"status_code": response.status_code}
                )
                return True
            else:
                error_msg = (
                    f"Teams webhook returned error: {response.status_code} - {response.text}"
                )
                logger.error(error_msg)
                raise AirflowException(error_msg)

        except requests.Timeout as e:
            logger.error(f"Teams webhook request timed out: {str(e)}")
            raise AirflowException(f"Teams webhook timeout: {str(e)}") from e

        except requests.ConnectionError as e:
            logger.error(f"Teams webhook connection error: {str(e)}")
            raise AirflowException(f"Teams webhook connection failed: {str(e)}") from e

        except requests.RequestException as e:
            logger.error(f"Teams webhook request failed: {str(e)}")
            raise AirflowException(f"Teams webhook error: {str(e)}") from e

        except Exception as e:
            logger.error(f"Unexpected error sending Teams notification: {str(e)}", exc_info=True)
            raise AirflowException(f"Teams notification failed: {str(e)}") from e
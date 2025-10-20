"""
Notification operators for multi-channel alerting.

Provides operators for sending notifications via email, Microsoft Teams,
and Telegram with template rendering and error handling.
"""

from src.operators.notifications.base_notification import BaseNotificationOperator
from src.operators.notifications.email_operator import EmailNotificationOperator
from src.operators.notifications.teams_operator import TeamsNotificationOperator
from src.operators.notifications.telegram_operator import TelegramNotificationOperator

__all__ = [
    "BaseNotificationOperator",
    "EmailNotificationOperator",
    "TeamsNotificationOperator",
    "TelegramNotificationOperator",
]
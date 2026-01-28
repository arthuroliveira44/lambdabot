"""
Slack Web API helper utilities for worker-side posting/updating messages.
"""

from typing import Any, Dict, Optional

from slack_sdk import WebClient

from data_slacklake.config import SLACK_BOT_TOKEN


def get_client() -> WebClient:
    return WebClient(token=SLACK_BOT_TOKEN)


def post_message(
    client: WebClient,
    channel: str,
    text: str,
    thread_ts: Optional[str] = None,
) -> Dict[str, Any]:
    return client.chat_postMessage(channel=channel, text=text, thread_ts=thread_ts)


def update_message(
    client: WebClient,
    channel: str,
    ts: str,
    text: str,
) -> Dict[str, Any]:
    return client.chat_update(channel=channel, ts=ts, text=text)


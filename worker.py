import logging
from typing import Any

from slack_sdk import WebClient

from data_slacklake.config import SLACK_BOT_TOKEN
from data_slacklake.services.slack_mention_service import process_app_mention_event


def _configure_logger() -> logging.Logger:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    configured_logger = logging.getLogger(__name__)
    configured_logger.setLevel(logging.INFO)
    return configured_logger


logger = _configure_logger()
slack_client = WebClient(token=SLACK_BOT_TOKEN)


def _send_message(channel_id: str, text: str, thread_ts: str | None = None) -> None:
    if not channel_id:
        raise ValueError("channel_id ausente para envio da mensagem no Slack.")
    slack_client.chat_postMessage(channel=channel_id, text=text, thread_ts=thread_ts)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    _ = context

    event_id = str(event.get("event_id", "")).strip()
    event_payload = event.get("event_payload") or {}
    if not isinstance(event_payload, dict):
        logger.warning("Payload inválido no worker. event_id=%s", event_id or "unknown")
        return {"statusCode": 400, "body": "Invalid payload"}

    channel_id = str(event_payload.get("channel", "")).strip()
    if not channel_id:
        logger.warning("Payload sem channel no worker. event_id=%s", event_id or "unknown")
        return {"statusCode": 400, "body": "Invalid payload"}

    logger.info(
        "WORKER RECEBIDO: event_id=%s, event_type=%s, channel=%s",
        event_id or "unknown",
        event_payload.get("type"),
        channel_id or "unknown",
    )

    def _sender(message_text: str, thread_ts: str | None) -> None:
        _send_message(channel_id=channel_id, text=message_text, thread_ts=thread_ts)

    try:
        process_app_mention_event(event_payload, _sender)
    except Exception as exc:
        logger.error("Falha no processamento do worker para event_id=%s: %s", event_id or "unknown", exc, exc_info=True)
        return {"statusCode": 500, "body": "Internal Server Error"}

    return {"statusCode": 200, "body": "OK"}

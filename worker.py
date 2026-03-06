import logging
import time
from threading import Lock
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
USER_PROFILE_TTL_SECONDS = 60 * 60
_USER_PROFILE_CACHE: dict[str, dict[str, Any]] = {}
_USER_PROFILE_CACHE_LOCK = Lock()


def _send_message(channel_id: str, text: str, thread_ts: str | None = None) -> None:
    if not channel_id:
        raise ValueError("channel_id ausente para envio da mensagem no Slack.")
    slack_client.chat_postMessage(channel=channel_id, text=text, thread_ts=thread_ts)


def _prune_user_profile_cache(now_timestamp: float) -> None:
    expiration_limit = now_timestamp - USER_PROFILE_TTL_SECONDS
    expired_user_ids = [
        user_id
        for user_id, state in _USER_PROFILE_CACHE.items()
        if float(state.get("updated_at", 0.0)) < expiration_limit
    ]
    for user_id in expired_user_ids:
        _USER_PROFILE_CACHE.pop(user_id, None)


def _extract_user_display_name_from_profile(profile: dict[str, Any]) -> str | None:
    for key in ("display_name_normalized", "display_name", "real_name_normalized", "real_name"):
        value = str(profile.get(key) or "").strip()
        if value:
            return value
    return None


def _get_slack_user_display_name(user_id: str) -> str | None:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return None

    now_timestamp = time.time()
    with _USER_PROFILE_CACHE_LOCK:
        _prune_user_profile_cache(now_timestamp)
        cached_state = _USER_PROFILE_CACHE.get(normalized_user_id)
        if cached_state:
            cached_name = str(cached_state.get("display_name") or "").strip()
            if cached_name:
                cached_state["updated_at"] = now_timestamp
                return cached_name

    try:
        response = slack_client.users_info(user=normalized_user_id)
    except Exception as exc:
        logger.warning("Falha ao buscar nome do usuário Slack (%s): %s", normalized_user_id, exc)
        return None

    user_data = response.get("user") if isinstance(response, dict) else {}
    profile = user_data.get("profile") if isinstance(user_data, dict) else {}
    display_name = _extract_user_display_name_from_profile(profile if isinstance(profile, dict) else {})
    if not display_name:
        display_name = str(user_data.get("name") or "").strip() if isinstance(user_data, dict) else ""
    if not display_name:
        return None

    with _USER_PROFILE_CACHE_LOCK:
        _prune_user_profile_cache(now_timestamp)
        _USER_PROFILE_CACHE[normalized_user_id] = {"display_name": display_name, "updated_at": now_timestamp}
    return display_name


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

    user_id = str(event_payload.get("user", "")).strip()
    user_display_name = _get_slack_user_display_name(user_id)
    if user_display_name:
        event_payload = dict(event_payload)
        event_payload["username"] = user_display_name

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

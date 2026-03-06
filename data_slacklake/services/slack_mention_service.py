"""Serviço de processamento de mensagens do Slack (app_mention e DM)."""

from __future__ import annotations

import logging
import time
from threading import Lock
from typing import Any, Callable

logger = logging.getLogger(__name__)
GREETING_TTL_SECONDS = 60 * 60
_GREETING_STATE: dict[str, float] = {}
_GREETING_STATE_LOCK = Lock()


def _extract_question_from_mention(message_text: str | None) -> str:
    normalized_text = str(message_text or "").strip()
    if ">" in normalized_text:
        return normalized_text.split(">", 1)[1].strip()
    return normalized_text


def _build_conversation_key(event_payload: dict[str, Any]) -> str:
    channel_id = str(event_payload.get("channel") or "unknown-channel").strip() or "unknown-channel"
    thread_ts = str(event_payload.get("thread_ts") or event_payload.get("ts") or "no-thread").strip() or "no-thread"
    user_id = str(event_payload.get("user") or "unknown-user").strip() or "unknown-user"
    return f"slack:{channel_id}:{thread_ts}:{user_id}"


def _build_requester_identity(event_payload: dict[str, Any]) -> str | None:
    user_id = str(event_payload.get("user") or "").strip()
    user_name = str(event_payload.get("username") or event_payload.get("user_name") or "").strip()
    if user_name and user_id:
        return f"{user_name} ({user_id})"
    if user_name:
        return user_name
    if user_id:
        return user_id
    return None


def _prune_expired_greetings(now_timestamp: float) -> None:
    expiration_limit = now_timestamp - GREETING_TTL_SECONDS
    expired_keys = [key for key, updated_at in _GREETING_STATE.items() if updated_at < expiration_limit]
    for key in expired_keys:
        _GREETING_STATE.pop(key, None)


def _is_first_interaction_for_conversation(conversation_key: str) -> bool:
    now_timestamp = time.time()
    with _GREETING_STATE_LOCK:
        _prune_expired_greetings(now_timestamp)
        if conversation_key in _GREETING_STATE:
            _GREETING_STATE[conversation_key] = now_timestamp
            return False
        _GREETING_STATE[conversation_key] = now_timestamp
        return True


def _build_genie_usage_message() -> str:
    """Monta mensagem de ajuda com comandos Genie disponíveis."""
    # Import tardio para evitar custo de import no cold start antes de uso real.
    from data_slacklake.services.ai_service import (  # pylint: disable=import-outside-toplevel
        list_configured_genie_commands,
    )

    commands = list_configured_genie_commands()
    if commands:
        commands_text = ", ".join(commands)
        first_command = commands[0]
        return (
            "Me envie uma pergunta para consultar a Genie.\n"
            f"Comandos configurados: {commands_text}\n"
            f"Exemplo: `{first_command} quantas operações tivemos esse ano?`"
        )

    return "Me envie uma pergunta para consultar a Genie. Exemplo: `quantas operações tivemos esse ano?`"


def process_app_mention_event(
    event_payload: dict[str, Any],
    send_message: Callable[[str, str | None], Any],
) -> None:
    """Processa mensagens suportadas do Slack e envia respostas via callback de envio."""
    message_text = str(event_payload.get("text", ""))
    user_id = str(event_payload.get("user", "Desconhecido")).strip() or "Desconhecido"
    event_ts = str(event_payload.get("ts", "")).strip()
    thread_ts = event_payload.get("thread_ts") or event_ts
    conversation_key = _build_conversation_key(event_payload)
    user_question = _extract_question_from_mention(message_text)

    if not user_question:
        usage_message = _build_genie_usage_message()
        send_message(f"Olá <@{user_id}>! {usage_message}", thread_ts)
        return

    logger.info("Pergunta de %s: %s", user_id, user_question)
    if _is_first_interaction_for_conversation(conversation_key):
        send_message(f"Olá <@{user_id}>! Consultando a Genie...", thread_ts)

    try:
        from data_slacklake.services.ai_service import (  # pylint: disable=import-outside-toplevel
            process_question,
        )

        requester_identity = _build_requester_identity(event_payload)
        answer_text, sql_debug = process_question(
            user_question,
            conversation_key=conversation_key,
            requester_identity=requester_identity,
        )
        send_message(answer_text, thread_ts)
        if sql_debug:
            send_message(f"*Debug SQL:* ```{sql_debug}```", thread_ts)
    except Exception as exc:
        logger.error("Erro ao processar menção: %s", exc, exc_info=True)
        send_message("Erro crítico ao processar sua solicitação. Tente novamente em instantes.", thread_ts)

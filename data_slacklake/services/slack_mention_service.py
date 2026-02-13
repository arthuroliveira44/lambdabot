from __future__ import annotations

import logging
from typing import Any, Callable


logger = logging.getLogger(__name__)


def _extract_question_from_mention(message_text: str) -> str:
    if ">" in message_text:
        return message_text.split(">", 1)[1].strip()
    return message_text.strip()


def _build_conversation_key(event_payload: dict[str, Any]) -> str:
    channel_id = event_payload.get("channel", "unknown-channel")
    thread_ts = event_payload.get("thread_ts") or event_payload.get("ts") or "no-thread"
    user_id = event_payload.get("user", "unknown-user")
    return f"slack:{channel_id}:{thread_ts}:{user_id}"


def _build_genie_usage_message() -> str:
    # Import tardio para evitar custo de import no cold start antes de uso real.
    from data_slacklake.services.ai_service import list_configured_genie_commands

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
    message_text = event_payload.get("text", "")
    user_id = event_payload.get("user", "Desconhecido")
    event_ts = event_payload.get("ts")
    thread_ts = event_payload.get("thread_ts") or event_ts
    user_question = _extract_question_from_mention(message_text)

    if not user_question:
        usage_message = _build_genie_usage_message()
        send_message(f"Olá <@{user_id}>! {usage_message}", thread_ts)
        return

    logger.info("Pergunta de %s: %s", user_id, user_question)
    send_message(f"Olá <@{user_id}>! Consultando a Genie...", thread_ts)

    try:
        from data_slacklake.services.ai_service import process_question

        conversation_key = _build_conversation_key(event_payload)
        answer_text, sql_debug = process_question(user_question, conversation_key=conversation_key)
        send_message(answer_text, thread_ts)
        if sql_debug:
            send_message(f"*Debug SQL:* ```{sql_debug}```", thread_ts)
    except Exception as exc:
        logger.error("Erro ao processar menção: %s", exc, exc_info=True)
        send_message(f"Erro crítico: {str(exc)}", thread_ts)

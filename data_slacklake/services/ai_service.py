"""
Service responsible for routing all questions exclusively to Databricks Genie.
"""
from __future__ import annotations

import json
import time
from threading import Lock
from typing import Any

from data_slacklake.config import GENIE_BOT_SPACE_MAP, GENIE_SPACE_ID, logger
from data_slacklake.services.genie_service import ask_genie

CONVERSATION_TTL_SECONDS = 60 * 60
_CONVERSATION_STATE: dict[str, dict[str, Any]] = {}
_CONVERSATION_LOCK = Lock()


def _prune_expired_conversations(now_timestamp: float) -> None:
    expiration_limit = now_timestamp - CONVERSATION_TTL_SECONDS
    expired_keys = [
        key
        for key, value in _CONVERSATION_STATE.items()
        if float(value.get("updated_at", 0.0)) < expiration_limit
    ]
    for key in expired_keys:
        _CONVERSATION_STATE.pop(key, None)


def _get_or_create_conversation_state(conversation_key: str, now_timestamp: float) -> dict[str, Any]:
    state = _CONVERSATION_STATE.get(conversation_key)
    if state is None:
        state = {"genie_conversation_ids": {}, "updated_at": now_timestamp}
        _CONVERSATION_STATE[conversation_key] = state
    else:
        state["updated_at"] = now_timestamp
    return state


def _get_genie_conversation_id(conversation_key: str | None, space_id: str) -> str | None:
    if not (conversation_key and space_id):
        return None

    now_timestamp = time.time()
    with _CONVERSATION_LOCK:
        _prune_expired_conversations(now_timestamp)
        state = _CONVERSATION_STATE.get(conversation_key)
        if not state:
            return None

        state["updated_at"] = now_timestamp
        conversation_ids = state.get("genie_conversation_ids") or {}
        conversation_id = conversation_ids.get(space_id)
        return str(conversation_id).strip() if conversation_id else None


def _set_genie_conversation_id(conversation_key: str | None, space_id: str, conversation_id: str | None) -> None:
    if not (conversation_key and space_id and conversation_id):
        return

    now_timestamp = time.time()
    with _CONVERSATION_LOCK:
        _prune_expired_conversations(now_timestamp)
        state = _get_or_create_conversation_state(conversation_key, now_timestamp)
        conversation_ids = state.setdefault("genie_conversation_ids", {})
        conversation_ids[space_id] = conversation_id


def _normalize_alias(alias: str) -> str:
    normalized_alias = (alias or "").strip().lower()
    if not normalized_alias:
        return ""
    if not normalized_alias.startswith("!"):
        normalized_alias = f"!{normalized_alias}"
    return normalized_alias


def _parse_genie_bot_map(raw_mapping: str | None) -> dict[str, str]:
    raw = (raw_mapping or "").strip()
    if not raw:
        return {}

    try:
        parsed_mapping = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("GENIE_BOT_SPACE_MAP inválido. Esperado JSON, recebido: %s", raw)
        return {}

    if not isinstance(parsed_mapping, dict):
        logger.warning("GENIE_BOT_SPACE_MAP deve ser um objeto JSON (dict).")
        return {}

    normalized_map: dict[str, str] = {}
    for raw_alias, raw_space_id in parsed_mapping.items():
        alias = _normalize_alias(str(raw_alias))
        space_id = str(raw_space_id).strip()
        if alias and space_id:
            normalized_map[alias] = space_id
    return normalized_map


def list_configured_genie_commands() -> list[str]:
    """Retorna aliases configurados (ex.: ['!marketing', '!remessagpt'])."""
    alias_map = _parse_genie_bot_map(GENIE_BOT_SPACE_MAP)
    return sorted(alias_map.keys())


def _extract_alias_and_question(question: str) -> tuple[str | None, str]:
    normalized_question = (question or "").strip()
    if not normalized_question:
        return None, ""

    first_token, _, remaining_text = normalized_question.partition(" ")
    if first_token.startswith("!"):
        return first_token, remaining_text.strip()

    return None, normalized_question


def _format_available_aliases(alias_map: dict[str, str]) -> str:
    if not alias_map:
        return ""
    return ", ".join(sorted(alias_map.keys()))


def _resolve_genie_target(question: str) -> tuple[str | None, str | None, str | None]:
    alias_map = _parse_genie_bot_map(GENIE_BOT_SPACE_MAP)
    alias, clean_question = _extract_alias_and_question(question)

    if alias:
        normalized_alias = _normalize_alias(alias)
        selected_space_id = alias_map.get(normalized_alias)
        if not selected_space_id:
            available_aliases = _format_available_aliases(alias_map)
            if available_aliases:
                return (
                    None,
                    None,
                    f"Não encontrei a Genie `{alias}`. Use um dos comandos: {available_aliases}.",
                )
            return (
                None,
                None,
                "Não encontrei a Genie solicitada e nenhum alias foi configurado no ambiente.",
            )
    else:
        selected_space_id = (GENIE_SPACE_ID or "").strip()
        if not selected_space_id:
            if len(alias_map) == 1:
                selected_space_id = next(iter(alias_map.values()))
            elif alias_map:
                available_aliases = _format_available_aliases(alias_map)
                return (
                    None,
                    None,
                    f"Informe a Genie usando um comando no início da pergunta. Opções: {available_aliases}.",
                )
            else:
                return (
                    None,
                    None,
                    "Nenhuma Genie configurada. Defina GENIE_SPACE_ID ou GENIE_BOT_SPACE_MAP.",
                )

    if not clean_question:
        available_aliases = _format_available_aliases(alias_map)
        if available_aliases:
            return (
                None,
                None,
                f"Envie a pergunta após o comando da Genie. Exemplo: `!remessagpt qual o total de operações este ano?`",
            )
        return (
            None,
            None,
            "Envie uma pergunta para eu consultar a Genie.",
        )

    return selected_space_id, clean_question, None


def process_question(pergunta: str, conversation_key: str | None = None) -> tuple[str, str | None]:
    """Roteia toda pergunta para o Databricks Genie."""
    space_id, clean_question, error_message = _resolve_genie_target(pergunta)
    if error_message:
        return error_message, None
    if not space_id or clean_question is None:
        return "Não foi possível determinar a Genie para responder a pergunta.", None

    genie_conversation_id = _get_genie_conversation_id(conversation_key, space_id)

    try:
        answer_text, sql_debug, updated_conversation_id = ask_genie(
            space_id=space_id,
            pergunta=clean_question,
            conversation_id=genie_conversation_id,
        )
    except Exception as exc:
        logger.warning("Falha ao consultar Genie: %s", exc)
        return f"Falha ao consultar Genie: {str(exc)}", None

    _set_genie_conversation_id(conversation_key, space_id, updated_conversation_id)
    final_answer = (answer_text or "").strip() or "A Genie não retornou uma resposta textual."
    return final_answer, sql_debug

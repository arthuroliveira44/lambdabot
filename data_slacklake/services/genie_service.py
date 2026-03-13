"""
Integração com Databricks Genie (Spaces).

Objetivo: permitir que determinados contextos usem modelos/Spaces do Genie
para responder perguntas, com retorno textual (e SQL opcional) sem estourar tokens.
"""

from __future__ import annotations

import os
import time
from functools import lru_cache
from typing import Any

from databricks.sdk import WorkspaceClient

from data_slacklake.config import (
    DATABRICKS_CLIENT_ID,
    DATABRICKS_CLIENT_SECRET,
    DATABRICKS_HOST,
    DATABRICKS_TOKEN,
    logger,
)

GENIE_RETRY_ATTEMPTS = max(1, int(os.getenv("GENIE_RETRY_ATTEMPTS", "3")))
GENIE_RETRY_BASE_DELAY_SECONDS = max(0.0, float(os.getenv("GENIE_RETRY_BASE_DELAY_SECONDS", "0.5")))
GENIE_RETRY_MAX_DELAY_SECONDS = max(0.0, float(os.getenv("GENIE_RETRY_MAX_DELAY_SECONDS", "4.0")))


def _resolve_databricks_auth_kwargs() -> tuple[dict[str, str], str]:
    normalized_client_id = str(DATABRICKS_CLIENT_ID or "").strip()
    normalized_client_secret = str(DATABRICKS_CLIENT_SECRET or "").strip()
    normalized_token = str(DATABRICKS_TOKEN or "").strip()

    # Preferir Service Principal quando disponível.
    if normalized_client_id and normalized_client_secret:
        return (
            {
                "client_id": normalized_client_id,
                "client_secret": normalized_client_secret,
            },
            "service_principal",
        )

    if normalized_client_id or normalized_client_secret:
        raise ValueError(
            "Credenciais Databricks incompletas para Service Principal. "
            "Defina ambos DATABRICKS_CLIENT_ID e DATABRICKS_CLIENT_SECRET."
        )

    if normalized_token:
        return ({"token": normalized_token}, "pat")

    raise ValueError(
        "Credenciais Databricks não configuradas. "
        "Defina DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET ou DATABRICKS_TOKEN."
    )


def _clear_conflicting_databricks_env(auth_mode: str) -> None:
    if auth_mode != "service_principal":
        return

    # O SDK do Databricks também lê variáveis de ambiente.
    # Se PAT estiver presente junto com OAuth M2M, ele falha por configuração ambígua.
    for env_var_name in ("DATABRICKS_TOKEN",):
        if os.getenv(env_var_name):
            os.environ.pop(env_var_name, None)


def _is_retryable_genie_error(error: Exception) -> bool:
    normalized_error = str(error).strip().lower()
    if not normalized_error:
        return False

    # Erros de configuração não devem sofrer retry.
    non_retryable_tokens = (
        "unable to get space",
        "does not exist",
        "not found",
        "permission",
        "unauthorized",
        "forbidden",
        "invalid",
    )
    if any(token in normalized_error for token in non_retryable_tokens):
        return False

    retryable_tokens = (
        "temporarily unavailable",
        "timeout",
        "timed out",
        "deadline exceeded",
        "too many requests",
        "rate limit",
        "connection reset",
        "connection aborted",
        "service unavailable",
        "internalerror",
        "429",
        "500",
        "502",
        "503",
        "504",
    )
    return any(token in normalized_error for token in retryable_tokens)


def _call_genie_with_retry(
    call_label: str,
    invoke_operation: Any,
    *,
    space_id: str,
    has_conversation_id: bool,
):
    last_error: Exception | None = None
    for attempt in range(1, GENIE_RETRY_ATTEMPTS + 1):
        try:
            if attempt > 1:
                logger.info(
                    "Retry Genie (%s) attempt=%s/%s space_id=%s has_conversation_id=%s",
                    call_label,
                    attempt,
                    GENIE_RETRY_ATTEMPTS,
                    space_id,
                    has_conversation_id,
                )
            return invoke_operation()
        except Exception as exc:
            last_error = exc
            should_retry = _is_retryable_genie_error(exc)
            if not should_retry or attempt >= GENIE_RETRY_ATTEMPTS:
                raise

            delay_seconds = min(
                GENIE_RETRY_MAX_DELAY_SECONDS,
                GENIE_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
            )
            logger.warning(
                "Falha transitória no Genie (%s) attempt=%s/%s. Retry em %.2fs. Erro: %s",
                call_label,
                attempt,
                GENIE_RETRY_ATTEMPTS,
                delay_seconds,
                exc,
            )
            time.sleep(delay_seconds)

    if last_error is not None:
        raise last_error


@lru_cache(maxsize=4)
def get_workspace_client() -> WorkspaceClient:
    """
    Cria cliente do Databricks SDK. Reusa em ambientes warm (Lambda).
    """
    normalized_host = str(DATABRICKS_HOST or "").strip()
    if not normalized_host:
        raise ValueError("DATABRICKS_HOST não configurado.")

    auth_kwargs, auth_mode = _resolve_databricks_auth_kwargs()
    _clear_conflicting_databricks_env(auth_mode)
    logger.info("Databricks auth mode selecionado para Genie: %s", auth_mode)
    return WorkspaceClient(host=normalized_host, **auth_kwargs)


def _extract_genie_response_parts(message: Any) -> tuple[str, str | None]:
    text_parts: list[str] = []
    sql_parts: list[str] = []

    for attachment in getattr(message, "attachments", None) or []:
        attachment_text = getattr(getattr(attachment, "text", None), "content", None)
        attachment_query = getattr(getattr(attachment, "query", None), "query", None)
        if attachment_text:
            text_parts.append(str(attachment_text).strip())
        if attachment_query:
            sql_parts.append(str(attachment_query).strip())

    response_text = "\n\n".join([part for part in text_parts if part]).strip()
    if not response_text:
        response_text = "Não consegui obter uma resposta textual do Genie para essa pergunta."

    sql_debug = "\n\n".join([query for query in sql_parts if query]).strip() or None
    return response_text, sql_debug


def ask_genie(
    space_id: str,
    pergunta: str,
    conversation_id: str | None = None,
) -> tuple[str, str | None, str | None]:
    """
    Envia pergunta para um Genie Space e retorna:
    - resposta_texto (string)
    - sql_debug (string opcional, se Genie gerar query)
    - conversation_id (para encadear conversas, se desejado)
    """
    normalized_space_id = str(space_id or "").strip()
    if not normalized_space_id:
        raise ValueError("space_id da Genie não pode ser vazio.")

    normalized_question = str(pergunta or "").strip()
    if not normalized_question:
        raise ValueError("A pergunta para Genie não pode ser vazia.")

    workspace_client = get_workspace_client()

    normalized_conversation_id = str(conversation_id or "").strip()
    if normalized_conversation_id:
        message = _call_genie_with_retry(
            "create_message",
            lambda: workspace_client.genie.create_message_and_wait(
                space_id=normalized_space_id,
                conversation_id=normalized_conversation_id,
                content=normalized_question,
            ),
            space_id=normalized_space_id,
            has_conversation_id=True,
        )
    else:
        message = _call_genie_with_retry(
            "start_conversation",
            lambda: workspace_client.genie.start_conversation_and_wait(
                space_id=normalized_space_id,
                content=normalized_question,
            ),
            space_id=normalized_space_id,
            has_conversation_id=False,
        )

    message_error = getattr(message, "error", None)
    if message_error:
        logger.warning(
            "Genie retornou erro: %s",
            message_error.as_dict() if hasattr(message_error, "as_dict") else str(message_error),
        )

    response_text, sql_debug = _extract_genie_response_parts(message)
    updated_conversation_id = str(getattr(message, "conversation_id", "")).strip() or None
    return response_text, sql_debug, updated_conversation_id

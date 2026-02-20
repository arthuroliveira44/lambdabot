"""
Integração com Databricks Genie (Spaces).

Objetivo: permitir que determinados contextos usem modelos/Spaces do Genie
para responder perguntas, com retorno textual (e SQL opcional) sem estourar tokens.
"""

from __future__ import annotations

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


@lru_cache(maxsize=4)
def get_workspace_client() -> WorkspaceClient:
    """
    Cria cliente do Databricks SDK. Reusa em ambientes warm (Lambda).
    """
    normalized_host = str(DATABRICKS_HOST or "").strip()
    if not normalized_host:
        raise ValueError("DATABRICKS_HOST não configurado.")

    auth_kwargs, auth_mode = _resolve_databricks_auth_kwargs()
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
        message = workspace_client.genie.create_message_and_wait(
            space_id=normalized_space_id,
            conversation_id=normalized_conversation_id,
            content=normalized_question,
        )
    else:
        message = workspace_client.genie.start_conversation_and_wait(
            space_id=normalized_space_id,
            content=normalized_question,
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

"""
Integração com Databricks Genie (Spaces).

Objetivo: permitir que determinados contextos usem modelos/Spaces do Genie
para responder perguntas, com retorno textual (e SQL opcional) sem estourar tokens.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from databricks.sdk import WorkspaceClient

from data_slacklake.config import DATABRICKS_HOST, DATABRICKS_TOKEN, logger


@lru_cache(maxsize=4)
def get_workspace_client() -> WorkspaceClient:
    """
    Cria cliente do Databricks SDK. Reusa em ambientes warm (Lambda).
    """
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        raise ValueError("DATABRICKS_HOST/DATABRICKS_TOKEN não configurados.")
    return WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)


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

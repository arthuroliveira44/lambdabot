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

    for attachment in message.attachments or []:
        if attachment.text and attachment.text.content:
            text_parts.append(attachment.text.content.strip())
        if attachment.query and attachment.query.query:
            sql_parts.append(attachment.query.query.strip())

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
    workspace_client = get_workspace_client()

    if conversation_id:
        message = workspace_client.genie.create_message_and_wait(
            space_id=space_id,
            conversation_id=conversation_id,
            content=pergunta,
        )
    else:
        message = workspace_client.genie.start_conversation_and_wait(space_id=space_id, content=pergunta)

    if message.error:
        logger.warning(
            "Genie retornou erro: %s",
            message.error.as_dict() if hasattr(message.error, "as_dict") else str(message.error),
        )

    response_text, sql_debug = _extract_genie_response_parts(message)
    return response_text, sql_debug, message.conversation_id

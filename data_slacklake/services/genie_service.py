"""
Integração com Databricks Genie (Spaces).

Objetivo: permitir que determinados contextos usem modelos/Spaces do Genie
para responder perguntas, com retorno textual (e SQL opcional) sem estourar tokens.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Optional, Tuple

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


def ask_genie(space_id: str, pergunta: str, conversation_id: Optional[str] = None) -> Tuple[str, Optional[str], str]:
    """
    Envia pergunta para um Genie Space e retorna:
    - resposta_texto (string)
    - sql_debug (string opcional, se Genie gerar query)
    - conversation_id (para encadear conversas, se desejado)
    """
    ws = get_workspace_client()

    if conversation_id:
        msg = ws.genie.create_message_and_wait(space_id=space_id, conversation_id=conversation_id, content=pergunta)
    else:
        msg = ws.genie.start_conversation_and_wait(space_id=space_id, content=pergunta)

    text_parts = []
    sql_parts = []

    for att in msg.attachments or []:
        if att.text and att.text.content:
            text_parts.append(att.text.content.strip())
        if att.query and att.query.query:
            sql_parts.append(att.query.query.strip())

    if msg.error:
        logger.warning("Genie retornou erro: %s", msg.error.as_dict() if hasattr(msg.error, "as_dict") else str(msg.error))

    resposta = "\n\n".join([p for p in text_parts if p]).strip()
    if not resposta:
        resposta = "Não consegui obter uma resposta textual do Genie para essa pergunta."

    sql_debug = "\n\n".join([q for q in sql_parts if q]).strip() or None
    return resposta, sql_debug, msg.conversation_id

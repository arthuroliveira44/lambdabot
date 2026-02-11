"""
Service responsible for orchestrating AI calls and Natural Language Processing (NLP) workflows.
"""
from __future__ import annotations

import json
import re
import time
from functools import lru_cache
from threading import Lock
from typing import Any, Sequence

from data_slacklake.config import (
    GENIE_ENABLED,
    GENIE_SPACE_ID,
    GENIE_SPACE_MAP,
    LLM_ENDPOINT,
    logger,
)
from data_slacklake.prompts import INTERPRET_TEMPLATE, SQL_GEN_TEMPLATE
from data_slacklake.services.db_service import execute_query
from data_slacklake.services.genie_service import ask_genie
from data_slacklake.services.router_service import identify_table


@lru_cache(maxsize=16)
def get_llm():
    """
    Lazy Loader: Only imports and connects to Databricks when it's actually going to be used.
    Uses lru_cache to guarantee the Singleton pattern (connects only once).
    """
    # pylint: disable=import-outside-toplevel
    from databricks_langchain import ChatDatabricks

    return ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)


FORBIDDEN_SQL_COMMANDS_PATTERN = re.compile(
    r"\b(drop|delete|insert|update|merge|alter|create|replace|truncate|grant|revoke|call)\b",
    re.IGNORECASE,
)

CONVERSATION_TTL_SECONDS = 60 * 60
CONVERSATION_MAX_MESSAGES = 12
CONVERSATION_CONTEXT_MAX_CHARS = 3500
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
        state = {
            "messages": [],
            "genie_conversation_ids": {},
            "updated_at": now_timestamp,
        }
        _CONVERSATION_STATE[conversation_key] = state
    else:
        state["updated_at"] = now_timestamp
    return state


def _get_recent_messages(conversation_key: str | None) -> list[dict[str, str]]:
    if not conversation_key:
        return []

    now_timestamp = time.time()
    with _CONVERSATION_LOCK:
        _prune_expired_conversations(now_timestamp)
        state = _CONVERSATION_STATE.get(conversation_key)
        if not state:
            return []

        state["updated_at"] = now_timestamp
        messages = state.get("messages") or []
        return [dict(message) for message in messages[-CONVERSATION_MAX_MESSAGES:]]


def _build_contextual_question(question: str, conversation_key: str | None) -> str:
    recent_messages = _get_recent_messages(conversation_key)
    if not recent_messages:
        return question

    lines = [
        (
            "Contexto recente da conversa "
            "(use para resolver referências como 'isso', 'o mesmo período', 'essa métrica'):"
        )
    ]
    for message in recent_messages:
        role = "Usuário" if message.get("role") == "user" else "Assistente"
        content = (message.get("content") or "").strip()
        if content:
            lines.append(f"{role}: {content}")

    lines.append(f"Pergunta atual do usuário: {question}")
    contextualized_question = "\n".join(lines).strip()
    if len(contextualized_question) > CONVERSATION_CONTEXT_MAX_CHARS:
        contextualized_question = contextualized_question[-CONVERSATION_CONTEXT_MAX_CHARS:]
    return contextualized_question


def _append_turn(conversation_key: str | None, question: str, answer: str) -> None:
    if not conversation_key:
        return

    now_timestamp = time.time()
    with _CONVERSATION_LOCK:
        _prune_expired_conversations(now_timestamp)
        state = _get_or_create_conversation_state(conversation_key, now_timestamp)
        messages = state.setdefault("messages", [])
        messages.extend(
            [
                {"role": "user", "content": (question or "").strip()},
                {"role": "assistant", "content": (answer or "").strip()},
            ]
        )
        if len(messages) > CONVERSATION_MAX_MESSAGES:
            state["messages"] = messages[-CONVERSATION_MAX_MESSAGES:]


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


def _invoke_llm_template(template: str, llm: Any, payload: dict[str, Any]) -> str:
    """Executa chain template -> modelo -> parser e retorna string final."""
    # pylint: disable=import-outside-toplevel
    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    prompt_template = ChatPromptTemplate.from_template(template)
    output_parser = StrOutputParser()
    chain = prompt_template | llm | output_parser
    return chain.invoke(payload)


def _normalize_sql(sql_query: str) -> str:
    normalized_sql = (sql_query or "").replace("```sql", "").replace("```", "").strip()
    if normalized_sql.endswith(";"):
        normalized_sql = normalized_sql[:-1].strip()
    return normalized_sql


def _apply_sql_guardrails(sql_query: str) -> str:
    """
    Guardrails mínimos para reduzir risco de SQL perigoso gerado pelo LLM.
    """
    sql = _normalize_sql(sql_query)
    if not sql:
        raise ValueError("SQL vazio gerado pelo modelo.")

    if ";" in sql:
        raise ValueError("SQL contém múltiplas instruções (';'), o que não é permitido.")

    lowered = sql.lstrip().lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError("Apenas queries SELECT/WITH são permitidas.")

    if FORBIDDEN_SQL_COMMANDS_PATTERN.search(sql):
        raise ValueError("SQL contém comandos potencialmente destrutivos e foi bloqueado.")

    if re.search(r"\blimit\b", sql, re.IGNORECASE) is None:
        sql = f"{sql}\nLIMIT 100"

    return sql


def _truncate_cell_value(cell_value: Any, max_chars: int) -> Any:
    if cell_value is None:
        return None
    if isinstance(cell_value, (int, float, bool)):
        return cell_value

    text = str(cell_value)
    if len(text) > max_chars:
        return text[: max_chars - 1] + "…"
    return text


def _prepare_result_for_llm(
    columns: Sequence[Any] | None,
    rows: Sequence[Any] | None,
    *,
    max_rows: int = 50,
    max_cols: int = 20,
    max_cell_chars: int = 300,
) -> str:
    """
    Compacta resultado para não estourar tokens na interpretação.
    Retorna JSON enxuto (lista de objetos) com truncagem e cap de linhas/colunas.
    """
    limited_columns = list(columns or [])[:max_cols]
    limited_rows = list(rows or [])[:max_rows]

    compact_rows: list[dict[str, Any]] = []
    for row in limited_rows:
        row_values = list(row) if isinstance(row, (list, tuple)) else [row]
        row_values = row_values[: len(limited_columns)]
        compact_row = {
            str(limited_columns[index]): _truncate_cell_value(row_values[index], max_cell_chars)
            for index in range(len(row_values))
        }
        compact_rows.append(compact_row)

    meta = {
        "colunas_total": len(columns or []),
        "colunas_enviadas": len(limited_columns),
        "linhas_total_retornadas": len(rows or []),
        "linhas_enviadas": len(limited_rows),
        "truncado": (len(columns or []) > len(limited_columns)) or (len(rows or []) > len(limited_rows)),
    }
    payload = {"meta": meta, "rows": compact_rows}
    return json.dumps(payload, ensure_ascii=False)


def _generate_sql(question: str, table_metadata: dict[str, Any], llm: Any) -> str:
    table_context = table_metadata.get("sql_context") or table_metadata.get("contexto")
    return _invoke_llm_template(
        template=SQL_GEN_TEMPLATE,
        llm=llm,
        payload={
            "contexto_tabela": table_context,
            "pergunta": question,
        },
    )


def _interpret(question: str, columns: Sequence[Any], rows: Sequence[Any], llm: Any) -> str:
    compact_data = _prepare_result_for_llm(columns, rows)
    return _invoke_llm_template(
        template=INTERPRET_TEMPLATE,
        llm=llm,
        payload={
            "pergunta": question,
            "colunas": columns,
            "dados": compact_data,
        },
    )


def _get_genie_space_id(table_metadata: dict[str, Any]) -> str | None:
    """
    Resolve Genie Space ID para um contexto.

    Prioridade:
    1) tabela_info['genie_space_id']
    2) config GENIE_SPACE_ID (Space global para todos os contextos)
    3) config/env GENIE_SPACE_MAP (JSON: {"context_id": "space_id"}) (fallback legado)
    """
    if not GENIE_ENABLED:
        return None

    if table_metadata.get("genie_space_id"):
        return str(table_metadata["genie_space_id"]).strip() or None

    if GENIE_SPACE_ID:
        return GENIE_SPACE_ID

    context_identifier = table_metadata.get("id")
    raw_mapping = GENIE_SPACE_MAP
    if raw_mapping and context_identifier:
        try:
            parsed_mapping = json.loads(raw_mapping)
        except Exception:
            parsed_mapping = None

        if isinstance(parsed_mapping, dict):
            space_id = parsed_mapping.get(context_identifier)
            return str(space_id).strip() if space_id else None

    return None


def _ask_genie_or_capture_error(
    *,
    space_id: str,
    question: str,
    failure_message: str,
    conversation_id: str | None = None,
) -> tuple[str | None, str | None, str | None, str | None]:
    try:
        answer_text, sql_debug, updated_conversation_id = ask_genie(
            space_id=space_id,
            pergunta=question,
            conversation_id=conversation_id,
        )
        return answer_text, sql_debug, None, updated_conversation_id
    except Exception as exc:
        logger.warning("%s: %s", failure_message, exc)
        return None, None, f"{failure_message}: {str(exc)}", None


def _process_with_genie(
    *,
    question: str,
    contextual_question: str,
    conversation_key: str | None,
    space_id: str,
    failure_message: str,
) -> tuple[str, str | None]:
    genie_conversation_id = _get_genie_conversation_id(conversation_key, space_id)
    genie_question = question if genie_conversation_id else contextual_question
    genie_answer, genie_sql_debug, genie_error, updated_conversation_id = _ask_genie_or_capture_error(
        space_id=space_id,
        question=genie_question,
        failure_message=failure_message,
        conversation_id=genie_conversation_id,
    )
    _set_genie_conversation_id(conversation_key, space_id, updated_conversation_id)
    answer_text = genie_answer or genie_error or "Falha ao consultar Genie."
    if genie_answer is not None:
        _append_turn(conversation_key, question, answer_text)
    sql_debug = genie_sql_debug if genie_answer is not None else None
    return answer_text, sql_debug


def _process_with_sql(
    *,
    question: str,
    contextual_question: str,
    conversation_key: str | None,
    table_metadata: dict[str, Any],
) -> tuple[str, str | None]:
    llm = get_llm()
    raw_sql_query = _generate_sql(contextual_question, table_metadata, llm)

    try:
        safe_sql_query = _apply_sql_guardrails(raw_sql_query)
    except Exception as exc:
        return f"Não consegui gerar um SQL seguro para executar: {str(exc)}", _normalize_sql(raw_sql_query)

    try:
        result_columns, result_rows = execute_query(safe_sql_query)
    except Exception as exc:
        return f"Erro ao executar a query: {str(exc)}", safe_sql_query

    final_answer = _interpret(contextual_question, result_columns, result_rows, llm)
    _append_turn(conversation_key, question, final_answer)
    return final_answer, safe_sql_query


def process_question(pergunta: str, conversation_key: str | None = None) -> tuple[str, str | None]:
    """Fluxo: Router -> SQL -> DB -> Resposta"""
    contextual_question = _build_contextual_question(pergunta, conversation_key)

    if GENIE_ENABLED and GENIE_SPACE_ID:
        return _process_with_genie(
            question=pergunta,
            contextual_question=contextual_question,
            conversation_key=conversation_key,
            space_id=GENIE_SPACE_ID,
            failure_message="Falha ao consultar Genie",
        )

    table_metadata = identify_table(contextual_question)
    if not table_metadata:
        return "Desculpe, não consegui processar sua pergunta.", None

    context_space_id = _get_genie_space_id(table_metadata)
    if context_space_id:
        return _process_with_genie(
            question=pergunta,
            contextual_question=contextual_question,
            conversation_key=conversation_key,
            space_id=context_space_id,
            failure_message="Falha ao consultar Genie (fallback para SQL)",
        )

    return _process_with_sql(
        question=pergunta,
        contextual_question=contextual_question,
        conversation_key=conversation_key,
        table_metadata=table_metadata,
    )

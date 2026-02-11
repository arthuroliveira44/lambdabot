"""
Service responsible for orchestrating AI calls and Natural Language Processing (NLP) workflows.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
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
    if not (raw_mapping and context_identifier):
        return None

    try:
        parsed_mapping = json.loads(raw_mapping)
    except Exception:
        return None

    if not isinstance(parsed_mapping, dict):
        return None

    space_id = parsed_mapping.get(context_identifier)
    return str(space_id).strip() if space_id else None


def _ask_genie_or_capture_error(
    *,
    space_id: str,
    question: str,
    failure_message: str,
) -> tuple[str | None, str | None, str | None]:
    try:
        answer_text, sql_debug, _conversation_id = ask_genie(space_id=space_id, pergunta=question)
        return answer_text, sql_debug, None
    except Exception as exc:
        logger.warning("%s: %s", failure_message, exc)
        return None, None, f"{failure_message}: {str(exc)}"


def process_question(pergunta: str) -> tuple[str, str | None]:
    """Fluxo: Router -> SQL -> DB -> Resposta"""
    # pylint: disable=too-many-return-statements

    if GENIE_ENABLED and GENIE_SPACE_ID:
        genie_answer, genie_sql_debug, genie_error = _ask_genie_or_capture_error(
            space_id=GENIE_SPACE_ID,
            question=pergunta,
            failure_message="Falha ao consultar Genie",
        )
        if genie_answer is not None:
            return genie_answer, genie_sql_debug
        return genie_error or "Falha ao consultar Genie.", None

    table_metadata = identify_table(pergunta)
    if not table_metadata:
        return "Desculpe, não consegui processar sua pergunta.", None

    context_space_id = _get_genie_space_id(table_metadata)
    if context_space_id:
        genie_answer, genie_sql_debug, genie_error = _ask_genie_or_capture_error(
            space_id=context_space_id,
            question=pergunta,
            failure_message="Falha ao consultar Genie (fallback para SQL)",
        )
        if genie_answer is not None:
            return genie_answer, genie_sql_debug
        return genie_error or "Falha ao consultar Genie.", None

    llm = get_llm()

    raw_sql_query = _generate_sql(pergunta, table_metadata, llm)

    try:
        safe_sql_query = _apply_sql_guardrails(raw_sql_query)
    except Exception as exc:
        return f"Não consegui gerar um SQL seguro para executar: {str(exc)}", _normalize_sql(raw_sql_query)

    try:
        result_columns, result_rows = execute_query(safe_sql_query)
    except Exception as exc:
        return f"Erro ao executar a query: {str(exc)}", safe_sql_query

    final_answer = _interpret(pergunta, result_columns, result_rows, llm)
    return final_answer, safe_sql_query

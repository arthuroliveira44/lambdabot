"""
Service responsible for orchestrating AI calls and Natural Language Processing (NLP) workflows.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache

from data_slacklake.config import LLM_ENDPOINT
import data_slacklake.config as cfg
from data_slacklake.prompts import INTERPRET_TEMPLATE, SQL_GEN_TEMPLATE
from data_slacklake.services.db_service import execute_query
from data_slacklake.services.router_service import identify_table
from data_slacklake.services.genie_service import ask_genie


@lru_cache(maxsize=16)
def get_llm():
    """
    Lazy Loader: Only imports and connects to Databricks when it's actually going to be used.
    Uses lru_cache to guarantee the Singleton pattern (connects only once).
    """
    # pylint: disable=import-outside-toplevel
    from databricks_langchain import ChatDatabricks

    return ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)


_FORBIDDEN_SQL_RE = re.compile(
    r"\b(drop|delete|insert|update|merge|alter|create|replace|truncate|grant|revoke|call)\b",
    re.IGNORECASE,
)


def _normalize_sql(sql_query: str) -> str:
    sql_query = (sql_query or "").replace("```sql", "").replace("```", "").strip()
    # remove ; apenas se estiver no final
    if sql_query.endswith(";"):
        sql_query = sql_query[:-1].strip()
    return sql_query


def _apply_sql_guardrails(sql_query: str) -> str:
    """
    Guardrails mínimos para reduzir risco de SQL perigoso gerado pelo LLM.
    """
    sql = _normalize_sql(sql_query)
    if not sql:
        raise ValueError("SQL vazio gerado pelo modelo.")

    # Bloqueia múltiplas statements.
    if ";" in sql:
        raise ValueError("SQL contém múltiplas instruções (';'), o que não é permitido.")

    # Permite apenas SELECT/WITH.
    lowered = sql.lstrip().lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError("Apenas queries SELECT/WITH são permitidas.")

    if _FORBIDDEN_SQL_RE.search(sql):
        raise ValueError("SQL contém comandos potencialmente destrutivos e foi bloqueado.")

    # Força LIMIT quando não houver (proteção contra resultados gigantes).
    if re.search(r"\blimit\b", sql, re.IGNORECASE) is None:
        sql = f"{sql}\nLIMIT 100"

    return sql


def _truncate_cell(value, max_chars: int):
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    text = str(value)
    if len(text) > max_chars:
        return text[: max_chars - 1] + "…"
    return text


def _prepare_result_for_llm(colunas, dados, *, max_rows: int = 50, max_cols: int = 20, max_cell_chars: int = 300) -> str:
    """
    Compacta resultado para não estourar tokens na interpretação.
    Retorna JSON enxuto (lista de objetos) com truncagem e cap de linhas/colunas.
    """
    cols = list(colunas or [])[:max_cols]
    rows = list(dados or [])[:max_rows]

    compact_rows = []
    for row in rows:
        # row pode vir como tuple/list
        values = list(row) if isinstance(row, (list, tuple)) else [row]
        values = values[: len(cols)]
        obj = {cols[i]: _truncate_cell(values[i], max_cell_chars) for i in range(len(values))}
        compact_rows.append(obj)

    meta = {
        "colunas_total": len(colunas or []),
        "colunas_enviadas": len(cols),
        "linhas_total_retornadas": len(dados or []),
        "linhas_enviadas": len(rows),
        "truncado": (len(colunas or []) > len(cols)) or (len(dados or []) > len(rows)),
    }
    payload = {"meta": meta, "rows": compact_rows}
    return json.dumps(payload, ensure_ascii=False)


def _generate_sql(pergunta: str, tabela_info: dict, llm) -> str:
    # pylint: disable=import-outside-toplevel
    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    prompt_sql = ChatPromptTemplate.from_template(SQL_GEN_TEMPLATE)
    chain_sql = prompt_sql | llm | StrOutputParser()

    return chain_sql.invoke(
        {
            "contexto_tabela": tabela_info.get("sql_context") or tabela_info.get("contexto"),
            "pergunta": pergunta,
        }
    )


def _interpret(pergunta: str, colunas, dados, llm) -> str:
    # pylint: disable=import-outside-toplevel
    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    prompt_interpret = ChatPromptTemplate.from_template(INTERPRET_TEMPLATE)
    chain_interpret = prompt_interpret | llm | StrOutputParser()

    dados_compactos = _prepare_result_for_llm(colunas, dados)
    return chain_interpret.invoke(
        {
            "pergunta": pergunta,
            "colunas": colunas,
            "dados": dados_compactos,
        }
    )


def _get_genie_space_id(tabela_info: dict) -> str | None:
    """
    Resolve Genie Space ID para um contexto.

    Prioridade:
    1) tabela_info['genie_space_id']
    2) env GENIE_SPACE_MAP (JSON: {"context_id": "space_id"})
    """
    if not cfg.GENIE_ENABLED:
        return None

    if tabela_info.get("genie_space_id"):
        return str(tabela_info["genie_space_id"]).strip() or None

    ctx_id = tabela_info.get("id")
    raw = cfg.GENIE_SPACE_MAP
    if not (raw and ctx_id):
        return None

    try:
        mapping = json.loads(raw)
    except Exception:
        return None

    space_id = mapping.get(ctx_id)
    return str(space_id).strip() if space_id else None


def process_question(pergunta):
    """Fluxo: Router -> SQL -> DB -> Resposta"""

    llm = get_llm()

    tabela_info = identify_table(pergunta)
    if not tabela_info:
        return "Desculpe, não encontrei uma tabela no meu catálogo que responda isso.", None

    # Caminho Genie (se configurado para o contexto escolhido).
    space_id = _get_genie_space_id(tabela_info)
    if space_id:
        try:
            resposta, sql_debug, _conversation_id = ask_genie(space_id=space_id, pergunta=pergunta)
            return resposta, sql_debug
        except Exception as e:
            # fallback para fluxo SQL tradicional
            return f"Falha ao consultar Genie (fallback para SQL): {str(e)}", None

    sql_query_raw = _generate_sql(pergunta, tabela_info, llm)

    try:
        sql_query = _apply_sql_guardrails(sql_query_raw)
    except Exception as e:
        return f"Não consegui gerar um SQL seguro para executar: {str(e)}", _normalize_sql(sql_query_raw)

    try:
        colunas, dados = execute_query(sql_query)
    except Exception as e:
        return f"Erro ao executar a query: {str(e)}", sql_query

    resposta_final = _interpret(pergunta, colunas, dados, llm)
    return resposta_final, sql_query

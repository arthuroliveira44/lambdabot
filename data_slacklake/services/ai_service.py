"""
Service responsible for orchestrating AI calls and Natural Language Processing (NLP) workflows.
"""
from __future__ import annotations

import json
import re
import time
from functools import lru_cache
from threading import Lock

import data_slacklake.config as cfg
from data_slacklake.config import LLM_ENDPOINT
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


_FORBIDDEN_SQL_RE = re.compile(
    r"\b(drop|delete|insert|update|merge|alter|create|replace|truncate|grant|revoke|call)\b",
    re.IGNORECASE,
)

_CONVERSATION_TTL_SECONDS = 60 * 60
_CONVERSATION_MAX_MESSAGES = 12
_CONVERSATION_CONTEXT_MAX_CHARS = 3500
_CONVERSATION_STATE: dict[str, dict] = {}
_CONVERSATION_LOCK = Lock()


def _prune_expired_conversations(now_ts: float) -> None:
    expiration_limit = now_ts - _CONVERSATION_TTL_SECONDS
    expired_keys = [
        key
        for key, value in _CONVERSATION_STATE.items()
        if value.get("updated_at", 0.0) < expiration_limit
    ]
    for key in expired_keys:
        _CONVERSATION_STATE.pop(key, None)


def _get_or_create_conversation_state(conversation_key: str, now_ts: float) -> dict:
    state = _CONVERSATION_STATE.get(conversation_key)
    if state is None:
        state = {
            "messages": [],
            "genie_conversation_ids": {},
            "updated_at": now_ts,
        }
        _CONVERSATION_STATE[conversation_key] = state
    else:
        state["updated_at"] = now_ts
    return state


def _get_recent_messages(conversation_key: str | None) -> list[dict]:
    if not conversation_key:
        return []

    now_ts = time.time()
    with _CONVERSATION_LOCK:
        _prune_expired_conversations(now_ts)
        state = _CONVERSATION_STATE.get(conversation_key)
        if not state:
            return []

        state["updated_at"] = now_ts
        messages = state.get("messages") or []
        return [dict(m) for m in messages[-_CONVERSATION_MAX_MESSAGES:]]


def _build_contextual_question(pergunta: str, conversation_key: str | None) -> str:
    recent_messages = _get_recent_messages(conversation_key)
    if not recent_messages:
        return pergunta

    lines = [
        "Contexto recente da conversa (use para resolver referências do tipo 'isso', 'o mesmo período', 'essa métrica'):"
    ]
    for msg in recent_messages:
        role = "Usuário" if msg.get("role") == "user" else "Assistente"
        content = (msg.get("content") or "").strip()
        if content:
            lines.append(f"{role}: {content}")

    lines.append(f"Pergunta atual do usuário: {pergunta}")
    contextualized = "\n".join(lines).strip()

    if len(contextualized) > _CONVERSATION_CONTEXT_MAX_CHARS:
        contextualized = contextualized[-_CONVERSATION_CONTEXT_MAX_CHARS:]
    return contextualized


def _append_turn(conversation_key: str | None, pergunta: str, resposta: str) -> None:
    if not conversation_key:
        return

    now_ts = time.time()
    with _CONVERSATION_LOCK:
        _prune_expired_conversations(now_ts)
        state = _get_or_create_conversation_state(conversation_key, now_ts)
        messages = state.setdefault("messages", [])
        messages.extend(
            [
                {"role": "user", "content": (pergunta or "").strip()},
                {"role": "assistant", "content": (resposta or "").strip()},
            ]
        )
        if len(messages) > _CONVERSATION_MAX_MESSAGES:
            state["messages"] = messages[-_CONVERSATION_MAX_MESSAGES:]


def _get_genie_conversation_id(conversation_key: str | None, space_id: str) -> str | None:
    if not (conversation_key and space_id):
        return None

    now_ts = time.time()
    with _CONVERSATION_LOCK:
        _prune_expired_conversations(now_ts)
        state = _CONVERSATION_STATE.get(conversation_key)
        if not state:
            return None

        state["updated_at"] = now_ts
        conversation_ids = state.get("genie_conversation_ids") or {}
        conversation_id = conversation_ids.get(space_id)
        return str(conversation_id).strip() if conversation_id else None


def _set_genie_conversation_id(conversation_key: str | None, space_id: str, conversation_id: str | None) -> None:
    if not (conversation_key and space_id and conversation_id):
        return

    now_ts = time.time()
    with _CONVERSATION_LOCK:
        _prune_expired_conversations(now_ts)
        state = _get_or_create_conversation_state(conversation_key, now_ts)
        conversation_ids = state.setdefault("genie_conversation_ids", {})
        conversation_ids[space_id] = conversation_id


def _normalize_sql(sql_query: str) -> str:
    sql_query = (sql_query or "").replace("```sql", "").replace("```", "").strip()
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

    if ";" in sql:
        raise ValueError("SQL contém múltiplas instruções (';'), o que não é permitido.")

    lowered = sql.lstrip().lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError("Apenas queries SELECT/WITH são permitidas.")

    if _FORBIDDEN_SQL_RE.search(sql):
        raise ValueError("SQL contém comandos potencialmente destrutivos e foi bloqueado.")

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
    2) config GENIE_SPACE_ID (Space global para todos os contextos)
    3) config/env GENIE_SPACE_MAP (JSON: {"context_id": "space_id"}) (fallback legado)
    """
    if not cfg.GENIE_ENABLED:
        return None

    if tabela_info.get("genie_space_id"):
        return str(tabela_info["genie_space_id"]).strip() or None

    if cfg.GENIE_SPACE_ID:
        return cfg.GENIE_SPACE_ID

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


def process_question(pergunta: str, conversation_key: str | None = None):
    """Fluxo: Router -> SQL -> DB -> Resposta"""
    pergunta_contextualizada = _build_contextual_question(pergunta, conversation_key)

    tabela_info = identify_table(pergunta_contextualizada)
    if not tabela_info:
        return "Desculpe, não consegui processar sua pergunta.", None

    space_id = _get_genie_space_id(tabela_info)
    if space_id:
        try:
            genie_conversation_id = _get_genie_conversation_id(conversation_key, space_id)
            pergunta_genie = pergunta if genie_conversation_id else pergunta_contextualizada
            resposta, sql_debug, new_conversation_id = ask_genie(
                space_id=space_id,
                pergunta=pergunta_genie,
                conversation_id=genie_conversation_id,
            )
            _set_genie_conversation_id(conversation_key, space_id, new_conversation_id)
            _append_turn(conversation_key, pergunta, resposta)
            return resposta, sql_debug
        except Exception as e:
            return f"Falha ao consultar Genie (fallback para SQL): {str(e)}", None

    llm = get_llm()

    sql_query_raw = _generate_sql(pergunta_contextualizada, tabela_info, llm)

    try:
        sql_query = _apply_sql_guardrails(sql_query_raw)
    except Exception as e:
        return f"Não consegui gerar um SQL seguro para executar: {str(e)}", _normalize_sql(sql_query_raw)

    try:
        colunas, dados = execute_query(sql_query)
    except Exception as e:
        return f"Erro ao executar a query: {str(e)}", sql_query

    resposta_final = _interpret(pergunta_contextualizada, colunas, dados, llm)
    _append_turn(conversation_key, pergunta, resposta_final)
    return resposta_final, sql_query

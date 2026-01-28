"""
Fallback "ao vivo" para buscar tabelas diretamente no metastore (datalake) quando
não existir no catálogo curado (`definitions.py`) / gerado (JSON).

Objetivo: NÃO gerar catálogo inteiro em runtime; apenas montar um contexto mínimo
para poucas tabelas candidatas.

Controle via env:
- LIVE_CATALOG_ENABLED=true|false (default: false)
- LIVE_CATALOG_TARGETS="catalog.schema,catalog.schema" (obrigatório se habilitar)
- LIVE_CATALOG_MAX_TABLES=50 (default)
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from data_slacklake.services.db_service import execute_query


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def is_live_catalog_enabled() -> bool:
    return _env_bool("LIVE_CATALOG_ENABLED", False)


def _get_targets() -> list[tuple[str, str]]:
    raw = os.getenv("LIVE_CATALOG_TARGETS", "").strip()
    if not raw:
        return []
    out: list[tuple[str, str]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "." not in part:
            continue
        catalog, schema = part.split(".", 1)
        out.append((catalog.strip(), schema.strip()))
    return out


def _max_tables() -> int:
    try:
        return int(os.getenv("LIVE_CATALOG_MAX_TABLES", "50"))
    except Exception:
        return 50


def _rows_to_dicts(columns: list[str], rows: list[list[Any]] | list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    return [dict(zip(columns, row)) for row in rows]


@lru_cache(maxsize=32)
def _list_tables_for_target(catalog: str, schema: str) -> list[dict[str, Any]]:
    # Usa information_schema (mais rápido). Se o workspace tiver limitações, pode ajustar depois.
    q = f"""
SELECT table_catalog, table_schema, table_name, comment
FROM system.information_schema.tables
WHERE table_catalog = '{catalog.replace("'", "''")}'
  AND table_schema  = '{schema.replace("'", "''")}'
  AND table_type IN ('MANAGED','EXTERNAL','VIEW')
ORDER BY table_name
""".strip()
    cols, rows = execute_query(q)
    return _rows_to_dicts(cols, rows)


@lru_cache(maxsize=256)
def _describe_columns(fqn: str) -> list[dict[str, Any]]:
    # `DESCRIBE` é suportado no SQL do Databricks.
    q = f"DESCRIBE {fqn}"
    cols, rows = execute_query(q)
    # Normalmente retorna col_name/data_type/comment. Filtra linhas de metadata.
    dicts = _rows_to_dicts(cols, rows)
    out: list[dict[str, Any]] = []
    ordinal = 1
    for r in dicts:
        col_name = (r.get("col_name") or r.get("col_name".upper()) or r.get("COL_NAME") or r.get("col_name")).strip() if r.get("col_name") else None
        if not col_name:
            continue
        if isinstance(col_name, str) and (not col_name or col_name.startswith("#")):
            continue
        out.append(
            {
                "column_name": str(col_name),
                "data_type": r.get("data_type"),
                "comment": r.get("comment"),
                "ordinal_position": ordinal,
            }
        )
        ordinal += 1
    return out


def _render_context(
    *,
    fqn: str,
    table_comment: str | None,
    columns: list[dict[str, Any]],
) -> str:
    lines: list[str] = []
    lines.append(f"Você é um analista de dados. Tabela: `{fqn}`")
    lines.append("")
    if table_comment:
        lines.append(f"Descrição da tabela: {table_comment.strip()}")
        lines.append("")
    lines.append("Colunas:")
    if not columns:
        lines.append("- (sem colunas encontradas)")
    else:
        for c in columns[:200]:
            cn = c.get("column_name")
            dt = c.get("data_type")
            cm = c.get("comment")
            type_part = f" ({dt})" if dt else ""
            comment_part = f": {str(cm).strip()}" if cm else ""
            lines.append(f"- {cn}{type_part}{comment_part}.")
    lines.append("")
    lines.append("Regras:")
    lines.append("1. Prefira selecionar apenas as colunas necessárias (evite SELECT *).")
    lines.append("2. Use filtros por período quando aplicável (ex.: datas/partições).")
    lines.append("3. Se não houver agregação explícita, use LIMIT 100.")
    lines.append("4. Ao agregar, confira o grão para evitar duplicação (JOINs podem multiplicar linhas).")
    return "\n".join(lines).strip() + "\n"


def _build_router_options(tables: list[dict[str, Any]], limit: int) -> str:
    # Opções com FQN como ID (para não precisar de catálogo prévio)
    out_lines: list[str] = []
    for t in tables[:limit]:
        fqn = f"{t.get('table_catalog')}.{t.get('table_schema')}.{t.get('table_name')}"
        desc = t.get("comment") or ""
        out_lines.append(f"- ID: {fqn} | Descrição: {desc}")
    return "\n".join(out_lines)


def build_live_table_context(*, pergunta: str, llm, router_prompt_template: str) -> dict[str, Any] | None:
    """
    Retorna um dict no formato esperado pelo restante do pipeline:
    {descricao, contexto, ...}
    """
    if not is_live_catalog_enabled():
        return None
    targets = _get_targets()
    if not targets:
        return None

    all_tables: list[dict[str, Any]] = []
    for catalog, schema in targets:
        all_tables.extend(_list_tables_for_target(catalog, schema))

    if not all_tables:
        return None

    # Limita para não explodir prompt/custo
    max_tables = _max_tables()
    options_text = _build_router_options(all_tables, max_tables)

    from langchain_core.output_parsers import StrOutputParser  # pylint: disable=import-outside-toplevel
    from langchain_core.prompts import ChatPromptTemplate  # pylint: disable=import-outside-toplevel

    prompt = ChatPromptTemplate.from_template(router_prompt_template)
    chain = prompt | llm | StrOutputParser()

    tabela_id = chain.invoke({"pergunta": pergunta, "opcoes": options_text}).strip()
    tabela_id = tabela_id.replace("ID:", "").strip()

    # tabela_id aqui é FQN
    if tabela_id == "NONE" or "." not in tabela_id:
        return None

    # encontra comment (se existir)
    table_comment = None
    for t in all_tables:
        fqn = f"{t.get('table_catalog')}.{t.get('table_schema')}.{t.get('table_name')}"
        if fqn == tabela_id:
            table_comment = t.get("comment")
            break

    cols = _describe_columns(tabela_id)
    contexto = _render_context(fqn=tabela_id, table_comment=table_comment, columns=cols)
    descricao = (table_comment.strip() if isinstance(table_comment, str) and table_comment.strip() else f"Tabela `{tabela_id}`.")

    return {
        "descricao": descricao,
        "contexto": contexto,
        "tags": [],
        "sinonimos": [],
        "grao": "desconhecido",
        "colunas_importantes": [],
        "metricas": {},
    }


"""
Gera contextos (CATALOGO) automaticamente a partir do schema do datalake.

Fonte de verdade:
- system.information_schema.tables
- system.information_schema.columns

Saída:
- Um JSON no mesmo formato do `data_slacklake.catalog.definitions.CATALOGO`
  (id -> {descricao, contexto})

Exemplo:
  python -m data_slacklake.catalog.generate_contexts \
    --table-catalog dev \
    --table-schema diamond \
    --table-like "mart_%" \
    --output data_slacklake/catalog/generated_catalog.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from typing import Any, Iterable

from data_slacklake.services.db_service import execute_query


@dataclass(frozen=True)
class TableInfo:
    table_catalog: str
    table_schema: str
    table_name: str
    comment: str | None

    @property
    def fqn(self) -> str:
        return f"{self.table_catalog}.{self.table_schema}.{self.table_name}"


@dataclass(frozen=True)
class ColumnInfo:
    table_name: str
    column_name: str
    data_type: str | None
    comment: str | None
    ordinal_position: int | None


def _rows_to_dicts(columns: list[str], rows: Iterable[Iterable[Any]]) -> list[dict[str, Any]]:
    return [dict(zip(columns, row)) for row in rows]


def _sql_escape_literal(value: str) -> str:
    # Usado apenas para literais simples em queries de catálogo
    return value.replace("'", "''")


def fetch_tables(*, table_catalog: str, table_schema: str, table_like: str) -> list[TableInfo]:
    query = f"""
SELECT
  table_catalog,
  table_schema,
  table_name,
  comment
FROM system.information_schema.tables
WHERE table_catalog = '{_sql_escape_literal(table_catalog)}'
  AND table_schema = '{_sql_escape_literal(table_schema)}'
  AND table_type = 'BASE TABLE'
  AND table_name LIKE '{_sql_escape_literal(table_like)}'
ORDER BY table_name
""".strip()

    cols, rows = execute_query(query)
    result = _rows_to_dicts(cols, rows)

    tables: list[TableInfo] = []
    for r in result:
        tables.append(
            TableInfo(
                table_catalog=str(r["table_catalog"]),
                table_schema=str(r["table_schema"]),
                table_name=str(r["table_name"]),
                comment=(r.get("comment") if r.get("comment") else None),
            )
        )
    return tables


def fetch_columns(*, table_catalog: str, table_schema: str) -> list[ColumnInfo]:
    query = f"""
SELECT
  table_name,
  column_name,
  data_type,
  comment,
  ordinal_position
FROM system.information_schema.columns
WHERE table_catalog = '{_sql_escape_literal(table_catalog)}'
  AND table_schema = '{_sql_escape_literal(table_schema)}'
ORDER BY table_name, ordinal_position
""".strip()

    cols, rows = execute_query(query)
    result = _rows_to_dicts(cols, rows)

    columns: list[ColumnInfo] = []
    for r in result:
        columns.append(
            ColumnInfo(
                table_name=str(r["table_name"]),
                column_name=str(r["column_name"]),
                data_type=(str(r["data_type"]) if r.get("data_type") else None),
                comment=(r.get("comment") if r.get("comment") else None),
                ordinal_position=(int(r["ordinal_position"]) if r.get("ordinal_position") is not None else None),
            )
        )
    return columns


def build_context(*, table: TableInfo, columns: list[ColumnInfo]) -> str:
    lines: list[str] = []
    lines.append(f"Você é um analista de dados. Tabela: `{table.fqn}`")
    lines.append("")
    if table.comment:
        lines.append(f"Descrição da tabela: {table.comment.strip()}")
        lines.append("")

    lines.append("Colunas:")
    if not columns:
        lines.append("- (sem colunas encontradas no information_schema)")
    else:
        for c in columns:
            type_part = f" ({c.data_type})" if c.data_type else ""
            comment_part = f": {c.comment.strip()}" if c.comment else ""
            lines.append(f"- {c.column_name}{type_part}{comment_part}.")
    lines.append("")
    lines.append("Regras:")
    lines.append("1. Prefira selecionar apenas as colunas necessárias (evite SELECT *).")
    lines.append("2. Use filtros por período quando aplicável (ex.: datas/partições).")
    lines.append("3. Se não houver agregação explícita, use LIMIT 100.")
    lines.append("4. Ao agregar, confira o grão para evitar duplicação (JOINs podem multiplicar linhas).")
    return "\n".join(lines).strip() + "\n"


def build_description(*, table: TableInfo) -> str:
    if table.comment and table.comment.strip():
        return table.comment.strip()
    return f"Tabela `{table.fqn}`."


def generate_catalog(
    *,
    table_catalog: str,
    table_schema: str,
    table_like: str,
    table_regex: str | None,
    id_prefix: str | None,
) -> dict[str, dict[str, str]]:
    tables = fetch_tables(table_catalog=table_catalog, table_schema=table_schema, table_like=table_like)
    cols_all = fetch_columns(table_catalog=table_catalog, table_schema=table_schema)

    cols_by_table: dict[str, list[ColumnInfo]] = {}
    for c in cols_all:
        cols_by_table.setdefault(c.table_name, []).append(c)

    regex_compiled = re.compile(table_regex) if table_regex else None

    catalog: dict[str, dict[str, str]] = {}
    for t in tables:
        if regex_compiled and not regex_compiled.search(t.table_name):
            continue

        table_id = t.table_name
        if id_prefix:
            table_id = f"{id_prefix}{table_id}"

        col_list = cols_by_table.get(t.table_name, [])
        catalog[table_id] = {
            "descricao": build_description(table=t),
            "contexto": build_context(table=t, columns=col_list),
        }

    return catalog


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera contextos do catálogo a partir do information_schema do Databricks."
    )
    parser.add_argument("--table-catalog", required=True, help="Ex: dev")
    parser.add_argument("--table-schema", required=True, help="Ex: diamond")
    parser.add_argument("--table-like", default="%", help="Filtro LIKE do nome da tabela. Ex: mart_%")
    parser.add_argument(
        "--table-regex",
        default=None,
        help="Filtro regex adicional (Python). Ex: ^mart_.*_core$",
    )
    parser.add_argument(
        "--id-prefix",
        default=None,
        help="Prefixo opcional para os IDs do catálogo. Ex: diamond_",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Caminho de saída (JSON). Ex: data_slacklake/catalog/generated_catalog.json",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Imprime o JSON no stdout (ainda escreve em --output).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    catalog = generate_catalog(
        table_catalog=args.table_catalog,
        table_schema=args.table_schema,
        table_like=args.table_like,
        table_regex=args.table_regex,
        id_prefix=args.id_prefix,
    )

    payload = json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(payload + "\n")

    if args.stdout:
        print(payload)

    print(f"OK: {len(catalog)} contextos gerados em '{args.output}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


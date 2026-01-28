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
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Iterable

from data_slacklake.services.db_service import execute_query

try:
    from pydantic import BaseModel, Field
except Exception:  # pragma: no cover
    BaseModel = object  # type: ignore
    Field = lambda default=None, **_kwargs: default  # type: ignore


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


class LlmCatalogEntry(BaseModel):
    """
    Saída esperada da LLM.

    Observação: `tags`/`sinonimos` são opcionais para evoluir o roteamento
    sem quebrar compatibilidade com o uso atual (que só exige `descricao`/`contexto`).
    """

    descricao: str = Field(..., min_length=1)
    grao: str = Field(default="desconhecido")
    colunas_importantes: list[str] = Field(default_factory=list)
    metricas: dict[str, str] = Field(default_factory=dict)
    regras: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    sinonimos: list[str] = Field(default_factory=list)


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
    # Mantido por compatibilidade: para o catálogo estendido, o contexto final é
    # renderizado em `render_context(...)`.
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


def infer_grao(*, table: TableInfo, columns: list[ColumnInfo]) -> str:
    names = [c.column_name.lower() for c in columns]
    has_customer = any(n in names for n in ("id_customer", "customer_id"))
    has_currency = "currency" in names
    time_candidates = [n for n in names if n.endswith("_date") or n in ("date", "dt", "day", "week", "month")]
    time_col = time_candidates[0] if time_candidates else None

    parts: list[str] = []
    if has_customer:
        parts.append("cliente")
    if has_currency:
        parts.append("moeda")
    if time_col:
        parts.append(f"tempo ({time_col})")

    if parts:
        return "Provável grão por " + ", ".join(parts) + ". Confirme antes de agregar."
    return "Grão não inferido a partir do schema. Confirme as chaves/dimensões antes de agregar."


def infer_colunas_importantes(*, columns: list[ColumnInfo]) -> list[str]:
    names = [c.column_name.lower() for c in columns]
    scored: list[tuple[int, str]] = []
    for n in names:
        score = 0
        if n in ("id", "id_customer", "customer_id", "cpf", "cnpj"):
            score += 100
        if any(k in n for k in ("date", "day", "week", "month", "year", "created_at", "updated_at", "timestamp")):
            score += 80
        if any(k in n for k in ("status", "type", "segment", "bu", "business", "country", "currency")):
            score += 50
        if any(k in n for k in ("value", "amount", "balance", "revenue", "gmv", "count", "total", "pct", "ratio")):
            score += 70
        if score > 0:
            scored.append((score, n))
    scored.sort(reverse=True)
    out: list[str] = []
    for _s, n in scored:
        if n not in out:
            out.append(n)
        if len(out) >= 12:
            break
    return out


def infer_metricas(*, columns: list[ColumnInfo]) -> dict[str, str]:
    metricas: dict[str, str] = {}
    for c in columns:
        name = c.column_name
        n = name.lower()
        dt = (c.data_type or "").lower()
        is_numeric = any(x in dt for x in ("int", "bigint", "double", "float", "decimal", "long", "smallint", "tinyint"))
        if not is_numeric:
            continue
        if any(k in n for k in ("pct", "ratio", "rate", "share", "zscore")):
            metricas[name] = "Métrica de razão/percentual: geralmente usar AVG/mediana (não somar)."
        elif any(k in n for k in ("balance", "current_balance")):
            metricas[name] = "Snapshot de saldo: use último valor no período por chave (não somar)."
        elif any(k in n for k in ("count", "qtd", "qty", "total")):
            metricas[name] = "Métrica contagem/total: normalmente aditiva (SUM), valide duplicação pelo grão."
        elif any(k in n for k in ("amount", "value", "revenue", "gmv")):
            metricas[name] = "Métrica monetária/valor: normalmente aditiva (SUM), valide moeda e duplicação pelo grão."
        else:
            metricas[name] = "Métrica numérica: escolha agregação apropriada (SUM/AVG/MAX) conforme o significado."
    if len(metricas) > 25:
        metricas = dict(list(metricas.items())[:25])
    return metricas


def infer_regras(*, columns: list[ColumnInfo]) -> list[str]:
    names = [c.column_name.lower() for c in columns]
    regras = [
        "Prefira selecionar apenas as colunas necessárias (evite SELECT *).",
        "Use filtros por período quando aplicável (ex.: datas/partições).",
        "Se não houver agregação explícita, use LIMIT 100.",
        "Ao agregar, confira o grão para evitar duplicação (JOINs podem multiplicar linhas).",
    ]
    if "currency" in names:
        regras.append("Para valores monetários, defina `currency` antes de agregar/Comparar.")
    if any("balance" in n for n in names):
        regras.append("Colunas de saldo (balance) costumam ser snapshots: não some ao longo do tempo sem recorte (último dia por chave).")
    return regras


def render_context(
    *,
    table: TableInfo,
    columns: list[ColumnInfo],
    grao: str,
    colunas_importantes: list[str],
    metricas: dict[str, str],
    regras: list[str],
) -> str:
    by_name = {c.column_name: c for c in columns}
    ordered_names = [c.column_name for c in columns]

    lines: list[str] = []
    lines.append(f"Você é um analista de dados. Tabela: `{table.fqn}`")
    lines.append("")
    if table.comment:
        lines.append(f"Descrição da tabela: {table.comment.strip()}")
        lines.append("")

    lines.append(f"Grão: {grao}")
    lines.append("")

    if colunas_importantes:
        lines.append("Colunas importantes:")
        for n in colunas_importantes:
            original = next((x for x in ordered_names if x.lower() == n.lower()), n)
            c = by_name.get(original)
            type_part = f" ({c.data_type})" if c and c.data_type else ""
            comment_part = f": {c.comment.strip()}" if c and c.comment else ""
            lines.append(f"- {original}{type_part}{comment_part}.")
        lines.append("")

    lines.append("Colunas:")
    if not columns:
        lines.append("- (sem colunas encontradas)")
    else:
        for c in columns:
            type_part = f" ({c.data_type})" if c.data_type else ""
            comment_part = f": {c.comment.strip()}" if c.comment else ""
            lines.append(f"- {c.column_name}{type_part}{comment_part}.")
    lines.append("")

    if metricas:
        lines.append("Métricas (orientação de agregação):")
        for k, v in metricas.items():
            lines.append(f"- {k}: {v}")
        lines.append("")

    lines.append("Regras:")
    for i, r in enumerate(regras, start=1):
        lines.append(f"{i}. {r}")

    return "\n".join(lines).strip() + "\n"


def build_description(*, table: TableInfo) -> str:
    if table.comment and table.comment.strip():
        return table.comment.strip()
    return f"Tabela `{table.fqn}`."


def _default_llm_endpoint() -> str:
    # Preferimos env var para não depender de config/SSM só para default.
    return os.getenv("LLM_ENDPOINT", "databricks-gpt-5-2")


def _build_llm_prompt(*, table: TableInfo, columns: list[ColumnInfo]) -> str:
    col_lines: list[str] = []
    for c in columns:
        type_part = f" ({c.data_type})" if c.data_type else ""
        comment_part = f" - {c.comment.strip()}" if c.comment else ""
        col_lines.append(f"- {c.column_name}{type_part}{comment_part}")

    table_comment = table.comment.strip() if table.comment else ""

    return f"""
Você é um especialista em modelagem de dados e geração de contexto para SQL (Spark SQL / Databricks).

Você receberá APENAS metadados reais (schema). Não invente colunas e não invente tabelas.

Tabela (FQN): {table.fqn}
Comentário da tabela: {table_comment}

Colunas reais (nome, tipo, comentário quando existir):
{chr(10).join(col_lines) if col_lines else "- (sem colunas no information_schema)"}

Tarefa:
Gere um JSON estrito (apenas JSON, sem markdown, sem texto extra) com o seguinte schema:
{{
  "descricao": "string curta (1 linha) para ajudar o roteador a escolher a tabela",
  "grao": "string curta descrevendo o grão ou 'desconhecido'",
  "colunas_importantes": ["lista curta (até 12) de colunas mais importantes (nome exatamente como no schema)"],
  "metricas": {"<coluna_numerica>": "orientação de agregação (SUM/AVG/MAX/snapshot etc.)"},
  "regras": ["lista de regras objetivas (strings curtas), focadas em evitar erros de agregação/duplicação"],
  "tags": ["opcional", "strings curtas"],
  "sinonimos": ["opcional", "termos de negócio relevantes"]
}}

Regras obrigatórias:
- Não invente colunas: toda coluna citada deve existir na lista fornecida.
- Não cite outras tabelas.
- `colunas_importantes` deve conter apenas nomes de colunas reais.
- `metricas` deve usar apenas colunas numéricas reais (quando possível).
- Não cite nomes de outras tabelas.
- Responda APENAS com JSON válido.
""".strip()


def _get_llm(*, endpoint: str, temperature: float):
    # Import lazy para não exigir databricks_langchain quando --use-llm não é usado.
    # pylint: disable=import-outside-toplevel
    from databricks_langchain import ChatDatabricks

    return ChatDatabricks(endpoint=endpoint, temperature=temperature)


def _llm_generate_entry(
    *,
    table: TableInfo,
    columns: list[ColumnInfo],
    llm_endpoint: str,
    llm_temperature: float,
) -> LlmCatalogEntry:
    prompt = _build_llm_prompt(table=table, columns=columns)
    llm = _get_llm(endpoint=llm_endpoint, temperature=llm_temperature)
    raw = llm.invoke(prompt)
    text = getattr(raw, "content", raw)
    if not isinstance(text, str):
        text = str(text)
    data = json.loads(text)
    return LlmCatalogEntry.model_validate(data)


def _validate_llm_entry_against_schema(
    *,
    entry: LlmCatalogEntry,
    table: TableInfo,
    columns: list[ColumnInfo],
) -> None:
    allowed = {c.column_name for c in columns}
    if not allowed:
        return

    # valida colunas_importantes
    for col in entry.colunas_importantes:
        if col not in allowed:
            raise ValueError(f"LLM inventou coluna_importante '{col}' (não existe no schema).")

    # valida metricas (chaves)
    for k in entry.metricas.keys():
        if k not in allowed:
            raise ValueError(f"LLM inventou métrica '{k}' (não existe no schema).")


def generate_catalog(
    *,
    table_catalog: str,
    table_schema: str,
    table_like: str,
    table_regex: str | None,
    id_prefix: str | None,
    use_llm: bool,
    llm_endpoint: str,
    llm_temperature: float,
) -> dict[str, dict[str, Any]]:
    tables = fetch_tables(table_catalog=table_catalog, table_schema=table_schema, table_like=table_like)
    cols_all = fetch_columns(table_catalog=table_catalog, table_schema=table_schema)

    cols_by_table: dict[str, list[ColumnInfo]] = {}
    for c in cols_all:
        cols_by_table.setdefault(c.table_name, []).append(c)

    regex_compiled = re.compile(table_regex) if table_regex else None

    catalog: dict[str, dict[str, Any]] = {}
    for t in tables:
        if regex_compiled and not regex_compiled.search(t.table_name):
            continue

        table_id = t.table_name
        if id_prefix:
            table_id = f"{id_prefix}{table_id}"

        col_list = cols_by_table.get(t.table_name, [])
        grao = infer_grao(table=t, columns=col_list)
        colunas_importantes = infer_colunas_importantes(columns=col_list)
        metricas = infer_metricas(columns=col_list)
        regras = infer_regras(columns=col_list)

        entry: dict[str, Any] = {
            "descricao": build_description(table=t),
            "grao": grao,
            "colunas_importantes": colunas_importantes,
            "metricas": metricas,
            "tags": [],
            "sinonimos": [],
            "contexto": render_context(
                table=t,
                columns=col_list,
                grao=grao,
                colunas_importantes=colunas_importantes,
                metricas=metricas,
                regras=regras,
            ),
        }

        if use_llm:
            try:
                llm_entry = _llm_generate_entry(
                    table=t,
                    columns=col_list,
                    llm_endpoint=llm_endpoint,
                    llm_temperature=llm_temperature,
                )
                _validate_llm_entry_against_schema(entry=llm_entry, table=t, columns=col_list)
                entry["descricao"] = llm_entry.descricao.strip()
                entry["grao"] = llm_entry.grao.strip() if llm_entry.grao else entry["grao"]
                entry["colunas_importantes"] = llm_entry.colunas_importantes or entry["colunas_importantes"]
                entry["metricas"] = llm_entry.metricas or entry["metricas"]
                entry["tags"] = llm_entry.tags
                entry["sinonimos"] = llm_entry.sinonimos
                regras_llm = llm_entry.regras or []
                if regras_llm:
                    regras = [r.strip() for r in regras_llm if r and r.strip()]
                entry["contexto"] = render_context(
                    table=t,
                    columns=col_list,
                    grao=entry["grao"],
                    colunas_importantes=entry["colunas_importantes"],
                    metricas=entry["metricas"],
                    regras=regras,
                )
            except Exception as e:
                # fallback seguro: mantém contexto determinístico e deixa uma tag
                entry["tags"] = ["fallback_schema_only"]
                # fallback não polui o contexto; apenas marca a tag

        catalog[table_id] = entry

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
        "--use-llm",
        action="store_true",
        help="Enriquece 'descricao/contexto' com LLM (com validação + fallback determinístico).",
    )
    parser.add_argument(
        "--llm-endpoint",
        default=_default_llm_endpoint(),
        help="Endpoint do modelo no Databricks (default: env LLM_ENDPOINT).",
    )
    parser.add_argument(
        "--llm-temperature",
        type=float,
        default=0.0,
        help="Temperatura da LLM (default 0.0).",
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
        use_llm=args.use_llm,
        llm_endpoint=args.llm_endpoint,
        llm_temperature=args.llm_temperature,
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


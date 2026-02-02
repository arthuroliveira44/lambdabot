"""
Definitions for Unity Catalog schemas, metadata, and table structures.
"""

from __future__ import annotations

from textwrap import dedent


def _build_router_doc(entry: dict) -> str:
    """
    Texto curto para recuperação/roteamento (Vector Search / Top-K).
    Deve ser pequeno e altamente discriminativo.
    """
    tags = entry.get("tags") or []
    medidas = entry.get("medidas") or []
    medidas_txt = ", ".join(medidas[:6])
    tags_txt = ", ".join(tags[:12])
    parts = [
        f"ID: {entry['id']}",
        f"Descrição: {entry.get('descricao_curta') or entry.get('descricao') or ''}",
        f"Tabela: {entry.get('tabela') or ''}",
        f"Grão: {entry.get('grao') or ''}",
        f"Tempo: {entry.get('tempo_coluna') or ''}",
        f"Medidas: {medidas_txt}" if medidas_txt else "",
        f"Tags: {tags_txt}" if tags_txt else "",
    ]
    return "\n".join([p for p in parts if p]).strip()


def _build_sql_context(entry: dict) -> str:
    """
    Contexto mínimo para geração de SQL: grão, tabela, colunas-chave, medidas e regras.
    Evite listas longas de colunas para não estourar tokens.
    """
    regras = entry.get("regras_sql") or []
    dimensoes = entry.get("dimensoes") or []
    medidas = entry.get("medidas") or []

    regras_txt = "\n".join([f"- {r}" for r in regras[:12]])
    dims_txt = "\n".join([f"- {d}" for d in dimensoes[:12]])
    meds_txt = "\n".join([f"- {m}" for m in medidas[:20]])

    return dedent(
        f"""
        Você é um analista de dados. Use APENAS a tabela `{entry.get('tabela')}`.

        Grão (granularidade): {entry.get('grao')}
        Coluna de tempo principal: {entry.get('tempo_coluna')}

        Medidas/Métricas (como filtrar/usar):
        {meds_txt if meds_txt else "- (não especificado)"}

        Dimensões importantes:
        {dims_txt if dims_txt else "- (não especificado)"}

        Regras SQL:
        {regras_txt if regras_txt else "- (não especificado)"}
        """
    ).strip()


def _make_entry(**kwargs) -> dict:
    entry = dict(kwargs)
    entry.setdefault("descricao_curta", entry.get("descricao", ""))
    entry["router_doc"] = _build_router_doc(entry)
    entry["sql_context"] = _build_sql_context(entry)
    # compat: código antigo espera 'contexto'
    entry["contexto"] = entry["sql_context"]
    return entry


CATALOGO = {
    "kpi_weekly": _make_entry(
        id="kpi_weekly",
        descricao="Métricas semanais da empresa: Receita, GMV, Pedidos e Clientes Únicos.",
        descricao_curta="KPIs semanais (receita, GMV, pedidos e clientes únicos).",
        tabela="dev.diamond.mart_kpi_weekly_core",
        grao="1 linha por week_start_date x kpi_metric x segment_key",
        tempo_coluna="week_start_date",
        medidas=[
            "value_week (double): valor pré-calculado da métrica",
            "kpi_metric (string): 'count_ops'|'gmv'|'gross_revenue'|'unique_customer_day'",
        ],
        dimensoes=[
            "segment_key (string): 'ALL' para total; outros valores = segmentos",
        ],
        regras_sql=[
            "Para totais da empresa, use segment_key = 'ALL'.",
            "NUNCA use SUM(value_week) sem filtrar dimensões para evitar duplicação.",
            "Se a pergunta for sobre 'vendas' ou 'receita', geralmente use kpi_metric = 'gross_revenue'.",
        ],
        tags=["kpi", "semanal", "receita", "gmv", "pedidos", "clientes", "segmento"],
    ),
}

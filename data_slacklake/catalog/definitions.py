"""
Definitions for Unity Catalog schemas, metadata, and table structures.
"""

from __future__ import annotations

from textwrap import dedent


def _stringify(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _truncate(text: str, max_chars: int) -> str:
    t = _stringify(text)
    if len(t) <= max_chars:
        return t
    return t[: max_chars - 1] + "…"


def _cap_total(text: str, max_chars: int) -> str:
    t = _stringify(text)
    if len(t) <= max_chars:
        return t
    return t[: max_chars - 1] + "…"


def _cap_bullets(
    items,
    *,
    max_items: int,
    max_chars_per_item: int,
    max_total_chars: int,
) -> str:
    out = []
    total = 0
    for raw in (items or [])[:max_items]:
        s = _truncate(_stringify(raw), max_chars_per_item)
        line = f"- {s}" if s else "- (vazio)"
        if total + len(line) + 1 > max_total_chars:
            break
        out.append(line)
        total += len(line) + 1
    return "\n".join(out).strip()


def _build_router_doc(entry: dict) -> str:
    """
    Texto curto para recuperação/roteamento (Vector Search / Top-K).
    Deve ser pequeno e altamente discriminativo.
    """
    tags = entry.get("tags") or []
    medidas = entry.get("medidas") or []

    # Limites para evitar crescimento descontrolado (tokens).
    medidas_txt = ", ".join([_truncate(_stringify(m), 80) for m in medidas[:6] if _stringify(m)])
    tags_txt = ", ".join([_truncate(_stringify(t), 40) for t in tags[:12] if _stringify(t)])
    parts = [
        f"ID: {_stringify(entry.get('id'))}",
        f"Descrição: {_truncate(entry.get('descricao_curta') or entry.get('descricao') or '', 240)}",
        f"Tabela: {_stringify(entry.get('tabela') or '')}",
        f"Grão: {_truncate(entry.get('grao') or '', 180)}",
        f"Tempo: {_stringify(entry.get('tempo_coluna') or '')}",
        f"Medidas: {medidas_txt}" if medidas_txt else "",
        f"Tags: {tags_txt}" if tags_txt else "",
    ]
    return _cap_total("\n".join([p for p in parts if p]).strip(), 1200)


def _build_sql_context(entry: dict) -> str:
    """
    Contexto mínimo para geração de SQL: grão, tabela, colunas-chave, medidas e regras.
    Evite listas longas de colunas para não estourar tokens.
    """
    regras = entry.get("regras_sql") or []
    dimensoes = entry.get("dimensoes") or []
    medidas = entry.get("medidas") or []

    # Limites por item + total por seção para evitar context_length_exceeded.
    regras_txt = _cap_bullets(regras, max_items=12, max_chars_per_item=240, max_total_chars=1800)
    dims_txt = _cap_bullets(dimensoes, max_items=12, max_chars_per_item=200, max_total_chars=1400)
    meds_txt = _cap_bullets(medidas, max_items=20, max_chars_per_item=200, max_total_chars=1800)

    ctx = dedent(
        f"""
        Você é um analista de dados. Use APENAS a tabela `{_stringify(entry.get('tabela'))}`.

        Grão (granularidade): {_truncate(entry.get('grao') or '', 220)}
        Coluna de tempo principal: {_stringify(entry.get('tempo_coluna') or '')}

        Medidas/Métricas (como filtrar/usar):
        {meds_txt if meds_txt else "- (não especificado)"}

        Dimensões importantes:
        {dims_txt if dims_txt else "- (não especificado)"}

        Regras SQL:
        {regras_txt if regras_txt else "- (não especificado)"}
        """
    ).strip()
    return _cap_total(ctx, 5000)


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

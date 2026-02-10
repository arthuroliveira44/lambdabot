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
    "kpi_daily_features": _make_entry(
        id="kpi_daily_features",
        descricao=(
            "Features diárias por KPI (médias móveis, z-score e variações vs períodos anteriores), "
            "segmentadas por BU/tipo de negócio/cliente e atributos da operação."
        ),
        descricao_curta="KPIs e features diárias (médias móveis, z-score, WoW/MoM, dia útil/feriado).",
        tabela="dev.diamond.fact_kpi_daily_features",
        grao=(
            "Provável grão: 1 linha por processed_date x kpi_metric x (dimensões de operação: bu, business_type, "
            "customer_type, operation_segment, operation_event_type, platform, in_or_out, country, currency)."
        ),
        tempo_coluna="processed_date",
        medidas=[
            "kpi_metric (string): identificador da métrica (valores variam por domínio)",
            "kpi_value (double): valor diário da métrica no grão da linha",
            "avg_7d/avg_30d/avg_90d (double): médias móveis em dias de calendário",
            "bd_avg_7bd/bd_avg_30bd/bd_avg_90bd (double): médias móveis considerando dias úteis",
            "zscore_7d/zscore_30d/zscore_90d (double): desvio padronizado vs histórico",
            "pct_vs_prev_calendar_day/pct_vs_prev_business_day (double): variação vs dia anterior",
            "pct_wow_calendar/pct_mom_calendar (double): variação WoW/MoM",
            "google_day (boolean) e métricas relacionadas: efeito de Google Day",
        ],
        dimensoes=[
            "bu (string): regra de negócio antiga para dividir operações",
            "business_type (string): segmento derivado de BU + customer_type",
            "customer_type (string): PF ou PJ",
            "operation_segment (string): segmento da operação",
            "operation_event_type (string): Recorrência | Aquisição | Ativação (strings exatas podem variar)",
            "operation_platform_beecambio (string): origem/canal da operação (site/app/etc.)",
            "in_or_out (string): envio ou recebimento (strings exatas podem variar)",
            "country (string) e currency_abbreviation (string): país e moeda",
            "is_business_day/is_holiday/is_weekend/dayofweek: calendários e flags",
        ],
        regras_sql=[
            "Sempre filtre por kpi_metric (ou liste kpi_metric com LIMIT) antes de analisar kpi_value.",
            "Como o grão pode incluir várias dimensões, evite SUM(kpi_value) sem agrupar/filtrar dimensões (risco de duplicação).",
            "Para séries temporais, use processed_date como eixo de tempo; para dias úteis, use is_business_day/is_holiday/is_weekend.",
            "Evite SELECT *: selecione apenas colunas necessárias (tabela tem muitas features) para reduzir custo e evitar excesso de tokens.",
            "Para perguntas de 'anomalia', prefira zscore_30d (ou zscore_30d_ex_google quando fizer sentido) e traga apenas Top N dias/segmentos.",
            "Use LIMIT em consultas exploratórias; para respostas finais, gere SQL agregando para um resultado pequeno.",
        ],
        tags=[
            "kpi",
            "diario",
            "daily",
            "features",
            "media movel",
            "moving average",
            "zscore",
            "anomalia",
            "wow",
            "mom",
            "dia util",
            "feriado",
            "fim de semana",
            "google day",
            "segmento",
            "bu",
            "pf",
            "pj",
            "envio",
            "recebimento",
        ],
    ),
}

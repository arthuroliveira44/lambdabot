"""
Definitions for Unity Catalog schemas, metadata, and table structures.
"""

CATALOGO = {
    "kpi_weekly": {
        # Campos usados hoje pelo Router/SQL (mantidos enxutos para reduzir tokens).
        "descricao": (
            "KPIs semanais por semana e recortes (cliente PF/PJ, envio/recebimento, BU), "
            "com comparativos (WoW, média 4s, MTD) e flags (tendência/risco)."
        ),
        "contexto": (
            "Você é um analista de dados. Tabela: `dev.gold.mart_kpi_weekly_core`\n\n"
            "Grão:\n"
            "- 1 linha por week_start_date + kpi_metric + customer_type + in_or_out + bu.\n\n"
            "Colunas principais:\n"
            "- week_start_date (date)\n"
            "- year_week (string), year (int), week_number (int)\n"
            "- customer_type (string), in_or_out (string), bu (string)\n"
            "- kpi_metric (string)\n"
            "- value_week (double), value_prev_week (double), value_4w_avg (double)\n"
            "- pct_vs_prev_week (double), pct_vs_4w_avg (double)\n"
            "- value_mtd (double), value_mtd_prev_year (double)\n"
            "- zscore_4w (double), sd_4w (double), trend_flag (string), risk_flag (string)\n"
            "- ticket_medio_week (double), mix_share_week (double)\n"
            "- week_streak_max_24w (int), week_growth_consistency_24w (double)\n"
            "- week_kpi_key (string)\n\n"
            "Regras (importante):\n"
            "1) Use week_start_date como chave temporal principal da semana.\n"
            "2) Antes de agregar, valide o recorte (kpi_metric + dimensões). Evite somar value_week sem necessidade.\n"
            "3) Para WoW, use pct_vs_prev_week (ou compare value_week vs value_prev_week).\n"
            "4) Para baseline recente, use value_4w_avg e pct_vs_4w_avg.\n"
            "5) Para anomalias, use zscore_4w/sd_4w e combine com trend_flag/risk_flag.\n"
            "6) Para visão MTD, use value_mtd e compare com value_mtd_prev_year.\n"
        ),

        # Metadados estruturados (não precisam ir para o prompt; ajudam manutenção/expansão do catálogo).
        "tabela_fqn": "dev.gold.mart_kpi_weekly_core",
        "router_hints": {
            "sinonimos": [
                "kpi semanal",
                "indicadores semanais",
                "métricas semanais",
                "wow",
                "variação vs semana anterior",
                "média móvel 4 semanas",
                "anomalia",
                "z-score",
                "tendência",
                "risco",
                "mtd",
            ],
            "tags": ["kpi", "semanal", "comparativos", "anomalia", "tendencia", "risco", "mtd"],
        },
        "modelo_dados": {
            "grao": "week_start_date + kpi_metric + customer_type + in_or_out + bu",
            "chaves": {
                "tempo": ["week_start_date", "year_week"],
                "dimensoes": ["customer_type", "in_or_out", "bu", "kpi_metric"],
                "chave_composta_sugerida": ["week_kpi_key"],
            },
        },
        "guardrails_sql": [
            "Use week_start_date para agrupar/ordenar por semana; year_week é label.",
            "Evite agregar value_week sem confirmar o grão; prefira filtrar dimensões e então agregar apenas se necessário.",
            "Para WoW use pct_vs_prev_week (ou value_week vs value_prev_week).",
            "Para comparação com baseline recente use value_4w_avg e pct_vs_4w_avg.",
            "Para anomalias use zscore_4w/sd_4w e combine com trend_flag/risk_flag.",
            "Para visão de mês use value_mtd e compare com value_mtd_prev_year.",
        ],
    },
}

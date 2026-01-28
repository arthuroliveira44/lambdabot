"""
Definitions for Unity Catalog schemas, metadata, and table structures.
"""

CATALOGO = {
    "kpi_weekly": {
        "descricao": (
            "KPIs semanais por semana e recortes (cliente PF/PJ, envio/recebimento, BU), "
            "com comparativos (WoW, média 4s, MTD) e flags (tendência/risco)."
        ),
        "contexto": (
            "Você é um analista de dados. Tabela: `dev.gold.mart_kpi_weekly_core`\n"
            "Grão:\n"
            "- 1 linha por week_start_date + kpi_metric + customer_type + in_or_out + bu.\n"
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
            "- week_kpi_key (string)\n"
            "Regras (importante):\n"
            "1) Use week_start_date como chave temporal principal da semana.\n"
            "2) Antes de agregar, valide o recorte (kpi_metric + dimensões). Evite somar value_week sem necessidade.\n"
            "3) Para WoW, use pct_vs_prev_week (ou compare value_week vs value_prev_week).\n"
            "4) Para baseline recente, use value_4w_avg e pct_vs_4w_avg.\n"
            "5) Para anomalias, use zscore_4w/sd_4w e combine com trend_flag/risk_flag.\n"
            "6) Para visão MTD, use value_mtd e compare com value_mtd_prev_year.\n"
        ),
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
    },

    # Alias explícito (usuários frequentemente digitam o nome da tabela)
    "mart_kpi_weekly_core": {
        "descricao": "Tabela `dev.gold.mart_kpi_weekly_core`.",
        "contexto": (
            "Você é um analista de dados. Tabela: `dev.gold.mart_kpi_weekly_core`\n"
            "Grão: 1 linha por week_start_date + kpi_metric + customer_type + in_or_out + bu.\n"
            "Use week_start_date como chave temporal principal."
        ),
        "tabela_fqn": "dev.gold.mart_kpi_weekly_core",
        "router_hints": {
            "sinonimos": ["mart_kpi_weekly_core", "mart kpi weekly core"],
            "tags": ["kpi", "semanal"],
        },
    },

    "mart_kpi_weekly_llm_features": {
        "descricao": "Tabela `dev.gold.mart_kpi_weekly_llm_features` (features agregadas para LLM/insights).",
        "contexto": (
            "Você é um analista de dados. Tabela: `dev.gold.mart_kpi_weekly_llm_features`\n"
            "Grão: por week_start_date (confirme dimensões antes de agregar)."
        ),
        "tabela_fqn": "dev.gold.mart_kpi_weekly_llm_features",
        "router_hints": {
            "sinonimos": ["llm features", "features kpi semanal", "mart_kpi_weekly_llm_features"],
            "tags": ["kpi", "features", "llm"],
        },
    },

    "mart_kpi_weekly_outliers": {
        "descricao": "Tabela `dev.gold.mart_kpi_weekly_outliers` (anomalias/outliers semanais por dimensão).",
        "contexto": (
            "Você é um analista de dados. Tabela: `dev.gold.mart_kpi_weekly_outliers`\n"
            "Grão: por week_start_date + kpi_metric + dimension_type + dimension_value (+ recortes).\n"
            "Campos úteis:\n"
            "- week_start_date, year_week, year, week_number\n"
            "- kpi_metric, dimension_type, dimension_value\n"
            "- value_week, value_prev_week, value_4w_avg, pct_vs_4w, zscore_4w\n"
            "- total_current_week, total_prev_week, contribution_to_total_variation\n"
            "- outlier_flag, opportunity_or_risk, relevance_score, rank_within_type\n"
            "Regras:\n"
            "1) Para outliers, filtre outlier_flag e ordene por relevance_score/rank.\n"
            "2) Para explicar variação do total, use contribution_to_total_variation.\n"
            "3) Use week_start_date como tempo principal."
        ),
        "tabela_fqn": "dev.gold.mart_kpi_weekly_outliers",
        "router_hints": {
            "sinonimos": ["outliers", "anomalias", "mart_kpi_weekly_outliers", "mart kpi weekly outliers"],
            "tags": ["kpi", "outliers", "anomalia"],
        },
    },
}

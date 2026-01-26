"""
Definitions for Unity Catalog schemas, metadata, and table structures.
"""

CATALOGO = {
    "kpi_weekly": {
        "descricao": "Métricas semanais da empresa: Receita, GMV, Pedidos e Clientes Únicos.",
        "contexto": """
            Você é um analista de dados. Tabela: `dev.diamond.mart_kpi_weekly_core`
            
            Colunas:
            - week_start_date (date): Início da semana.
            - kpi_metric (string): Valores possíveis: 'count_ops', 'gmv', 'gross_revenue', 'unique_customer_day'.
            - segment_key (string): 'ALL' para total geral da empresa. Outros valores são segmentos.
            - value_week (double): Valor da métrica pré-calculado.

        Regras:
        1. Para totais da empresa, use segment_key = 'ALL'.
        2. NUNCA use SUM() em 'value_week' sem filtrar dimensões para evitar duplicação.
        3. Se a pergunta for sobre "vendas" ou "receita", geralmente se refere a 'gross_revenue'.
    """
    },
}

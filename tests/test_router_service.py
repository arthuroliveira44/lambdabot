"""
Unit tests for router_service.identify_table.
"""

from unittest.mock import MagicMock, patch


def test_router_direct_match_por_id_sem_chamar_llm():
    """
    Se o usuário mencionar explicitamente o ID do catálogo, o roteamento deve ser direto
    e não deve chamar o LLM.
    """
    from data_slacklake.services import router_service

    router_service.CATALOGO = {
        "mart_kpi_weekly_outliers": {"descricao": "Tabela X", "tabela_fqn": "dev.gold.mart_kpi_weekly_outliers"}
    }

    with patch("databricks_langchain.ChatDatabricks") as mock_chat_cls:
        chosen = router_service.identify_table("me fale sobre mart_kpi_weekly_outliers")
        assert chosen == router_service.CATALOGO["mart_kpi_weekly_outliers"]
        mock_chat_cls.assert_not_called()


def test_router_parse_saida_llm_com_crases_e_prefixo_id():
    """
    Se o LLM retornar o id com variações comuns (ID: / crases), o parser deve aceitar.
    """
    from data_slacklake.services import router_service

    router_service.CATALOGO = {
        "mart_kpi_weekly_outliers": {"descricao": "Tabela X", "tabela_fqn": "dev.gold.mart_kpi_weekly_outliers"}
    }

    # Force LLM path by using a question without direct match
    question = "me fale sobre outliers semanais"

    mock_llm = MagicMock()
    with patch("databricks_langchain.ChatDatabricks", return_value=mock_llm), patch(
        "langchain_core.prompts.ChatPromptTemplate.from_template"
    ) as mock_prompt:
        mock_chain = MagicMock()
        mock_chain.invoke.return_value = "ID: `mart_kpi_weekly_outliers`"
        mock_prompt.return_value.__or__.return_value.__or__.return_value = mock_chain

        chosen = router_service.identify_table(question)
        assert chosen == router_service.CATALOGO["mart_kpi_weekly_outliers"]


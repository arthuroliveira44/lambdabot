"""
Unit tests for the AI Service and Main handler logic.
Cleaned up to remove unused variables and focus on assertions.
"""
# pylint: disable=import-outside-toplevel
from unittest.mock import MagicMock, patch


@patch("databricks_langchain.ChatDatabricks")
@patch("data_slacklake.services.ai_service.execute_query")
@patch("data_slacklake.services.ai_service.identify_table")
def test_full_process_success(mock_identify, mock_db, _mock_chat_cls):
    """
    Test: Identify -> Generate SQL -> Execute DB -> Interpret.
    """

    mock_identify.return_value = {"tabela": "vendas", "contexto": "CTX"}
    mock_db.return_value = (["coluna1"], [["valor1"]])

    with patch("langchain_core.prompts.ChatPromptTemplate.from_template") as mock_prompt:

        mock_chain_final = MagicMock()

        mock_chain_final.invoke.side_effect = ["SELECT * FROM vendas", "O total é 10."]

        mock_prompt.return_value.__or__.return_value.__or__.return_value = mock_chain_final

        from data_slacklake.services.ai_service import process_question

        resposta, sql = process_question("Qual o total?")

        assert resposta == "O total é 10."
        assert sql == "SELECT * FROM vendas\nLIMIT 100"

        mock_db.assert_called_once_with("SELECT * FROM vendas\nLIMIT 100")


@patch("data_slacklake.services.ai_service.execute_query")
@patch("data_slacklake.services.ai_service.identify_table")
@patch("data_slacklake.services.ai_service.get_llm")
def test_databricks_error(_mock_get_llm, mock_identify, mock_db):
    """
    Tests whether the code handles exceptions thrown from execute_query.
    """
    mock_identify.return_value = {"tabela": "vendas", "contexto": "CTX"}

    mock_db.side_effect = Exception("Conexão recusada")

    with patch("langchain_core.prompts.ChatPromptTemplate.from_template") as mock_prompt:

        mock_chain = MagicMock()
        mock_chain.invoke.return_value = "SELECT * FROM vendas"
        mock_prompt.return_value.__or__.return_value.__or__.return_value = mock_chain

        from data_slacklake.services.ai_service import process_question

        resposta, sql = process_question("Qual o total?")

        assert "Erro ao executar a query" in resposta
        assert "Conexão recusada" in resposta
        assert sql == "SELECT * FROM vendas\nLIMIT 100"


@patch("data_slacklake.services.ai_service.process_question")
def test_app_mention_success(mock_process):
    """
    Test if the bot responds twice: 'Checking...' and the Final Response.
    """
    mock_process.return_value = ("Resposta Final da IA", "SELECT * FROM debug")

    mock_say = MagicMock()
    event_body = {
        "event": {
            "text": "<@BOT_ID> analyze os dados",
            "user": "USER_ID",
            "channel": "C123",
            "ts": "12345.6789",
        }
    }

    from main import handle_app_mentions

    handle_app_mentions(event_body, mock_say)

    mock_process.assert_called_with("analyze os dados", conversation_key="slack:C123:12345.6789:USER_ID")

    assert mock_say.call_count >= 2

    assert any(call.args and call.args[0] == "Resposta Final da IA" for call in mock_say.call_args_list)

    debug_call = mock_say.call_args_list[-1]
    assert "SELECT * FROM debug" in debug_call[0][0]
    assert debug_call[1]["thread_ts"] == "12345.6789"


@patch("data_slacklake.services.ai_service.process_question")
def test_app_mention_error(mock_process):
    """
    Tests whether the bot notifies the user when the backend crashes.
    """
    mock_process.side_effect = Exception("Erro Catastrófico")

    mock_say = MagicMock()
    body = {"event": {"text": "teste", "user": "U1"}}

    from main import handle_app_mentions

    handle_app_mentions(body, mock_say)

    last_call_args = mock_say.call_args[0][0]
    assert "Erro crítico" in last_call_args or "Erro Catastrófico" in last_call_args


@patch("data_slacklake.services.ai_service.ask_genie")
@patch("data_slacklake.services.ai_service.identify_table")
def test_genie_flow(mock_identify, mock_ask_genie):
    """Garante que o fluxo usa ask_genie e não SQL/DB."""
    mock_identify.return_value = {"id": "kpi_weekly", "contexto": "CTX"}
    mock_ask_genie.return_value = ("Resposta Genie", "SELECT 1", "conv-1")

    with patch("data_slacklake.services.ai_service.GENIE_ENABLED", True), patch(
        "data_slacklake.services.ai_service.GENIE_SPACE_ID", "space-123"
    ):
        from data_slacklake.services.ai_service import process_question
        resposta, sql = process_question("Qual o total?")

    assert resposta == "Resposta Genie"
    assert sql == "SELECT 1"
    mock_ask_genie.assert_called_once_with(space_id="space-123", pergunta="Qual o total?", conversation_id=None)


@patch("data_slacklake.services.ai_service._interpret")
@patch("data_slacklake.services.ai_service.execute_query")
@patch("data_slacklake.services.ai_service._generate_sql")
@patch("data_slacklake.services.ai_service.get_llm")
@patch("data_slacklake.services.ai_service.identify_table")
def test_follow_up_question_uses_conversation_context(
    mock_identify, mock_get_llm, mock_generate_sql, mock_db, mock_interpret
):
    """
    Garante que a segunda pergunta reutiliza o histórico recente no fluxo SQL.
    """
    mock_identify.return_value = {"tabela": "vendas", "contexto": "CTX"}
    mock_get_llm.return_value = MagicMock()
    mock_generate_sql.return_value = "SELECT * FROM vendas"
    mock_db.return_value = (["coluna1"], [["valor1"]])
    mock_interpret.return_value = "Resposta com contexto"

    from data_slacklake.services.ai_service import process_question

    conversation_key = "conv-followup-context-1"
    process_question("Qual foi a receita ontem?", conversation_key=conversation_key)
    process_question("E no mês passado?", conversation_key=conversation_key)

    primeira_pergunta = mock_identify.call_args_list[0].args[0]
    segunda_pergunta = mock_identify.call_args_list[1].args[0]

    assert primeira_pergunta == "Qual foi a receita ontem?"
    assert "Contexto recente da conversa" in segunda_pergunta
    assert "Qual foi a receita ontem?" in segunda_pergunta
    assert "E no mês passado?" in segunda_pergunta


@patch("data_slacklake.services.ai_service.ask_genie")
@patch("data_slacklake.services.ai_service.identify_table")
def test_genie_reuses_conversation_id_across_turns(mock_identify, mock_ask_genie):
    """
    Garante que o conversation_id retornado pelo Genie é reaproveitado no próximo turno.
    """
    mock_identify.return_value = {"id": "kpi_weekly", "contexto": "CTX"}
    mock_ask_genie.side_effect = [
        ("Resposta 1", "SELECT 1", "conv-1"),
        ("Resposta 2", "SELECT 2", "conv-1"),
    ]

    with patch("data_slacklake.services.ai_service.GENIE_ENABLED", True), patch(
        "data_slacklake.services.ai_service.GENIE_SPACE_ID", "space-123"
    ):
        from data_slacklake.services.ai_service import process_question

        conversation_key = "conv-genie-reuse-1"
        process_question("Qual o total?", conversation_key=conversation_key)
        process_question("E no mês passado?", conversation_key=conversation_key)

    primeira_chamada = mock_ask_genie.call_args_list[0].kwargs
    segunda_chamada = mock_ask_genie.call_args_list[1].kwargs

    assert primeira_chamada["conversation_id"] is None
    assert segunda_chamada["conversation_id"] == "conv-1"
    assert segunda_chamada["pergunta"] == "E no mês passado?"

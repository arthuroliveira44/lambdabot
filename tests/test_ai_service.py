"""
Unit tests for the AI Service and Main handler logic.
Cleaned up to remove unused variables and focus on assertions.
"""
from unittest.mock import MagicMock, patch

from data_slacklake.services.ai_service import process_question
from main import handle_app_mentions


@patch("databricks_langchain.ChatDatabricks")
@patch("data_slacklake.services.ai_service.execute_query")
@patch("data_slacklake.services.ai_service.identify_table")
def test_fluxo_completo_sucesso(mock_identify, mock_db, _mock_chat_cls):
    """
    Test: Identify -> Generate SQL -> Execute DB -> Interpret.
    """

    mock_identify.return_value = {"tabela": "vendas", "contexto": "CTX"}
    mock_db.return_value = (["coluna1"], [["valor1"]])

    with patch("langchain_core.prompts.ChatPromptTemplate.from_template") as mock_prompt:

        mock_chain_final = MagicMock()

        mock_chain_final.invoke.side_effect = ["SELECT * FROM vendas", "O total é 10."]

        mock_prompt.return_value.__or__.return_value.__or__.return_value = mock_chain_final

        resposta, sql = process_question("Qual o total?")

        assert resposta == "O total é 10."
        assert sql == "SELECT * FROM vendas"

        mock_db.assert_called_once_with("SELECT * FROM vendas")


@patch("data_slacklake.services.ai_service.execute_query")
@patch("data_slacklake.services.ai_service.identify_table")
@patch("data_slacklake.services.ai_service.get_llm")
def test_erro_banco_dados(_mock_get_llm, mock_identify, mock_db):
    """
    Tests whether the code handles exceptions thrown from execute_query.
    """
    mock_identify.return_value = {"tabela": "vendas", "contexto": "CTX"}

    mock_db.side_effect = Exception("Conexão recusada")

    with patch("langchain_core.prompts.ChatPromptTemplate.from_template") as mock_prompt:

        mock_chain = MagicMock()
        mock_chain.invoke.return_value = "SELECT * FROM vendas"
        mock_prompt.return_value.__or__.return_value.__or__.return_value = mock_chain

        resposta, sql = process_question("Qual o total?")

        assert "Erro ao executar a query" in resposta
        assert "Conexão recusada" in resposta
        assert sql == "SELECT * FROM vendas"


@patch("data_slacklake.services.ai_service.process_question")
def test_app_mention_fluxo_sucesso(mock_process):
    """
    Test if the bot responds twice: 'Checking...' and the Final Response.
    """
    mock_process.return_value = ("Resposta Final da IA", "SELECT * FROM debug")

    mock_say = MagicMock()
    body = {
        "event": {
            "text": "<@BOT_ID> analyze os dados",
            "user": "USER_ID",
            "ts": "12345.6789"
        }
    }

    handle_app_mentions(body, mock_say)

    mock_process.assert_called_with("analyze os dados")

    assert mock_say.call_count >= 2

    mock_say.assert_any_call("Resposta Final da IA", thread_ts="12345.6789")

    calls = mock_say.call_args_list
    debug_call = calls[-1]
    assert "SELECT * FROM debug" in debug_call[0][0]
    assert debug_call[1]['thread_ts'] == "12345.6789"


@patch("data_slacklake.services.ai_service.process_question")
def test_app_mention_erro(mock_process):
    """
    Tests whether the bot notifies the user when the backend crashes.
    """
    mock_process.side_effect = Exception("Erro Catastrófico")

    mock_say = MagicMock()
    body = {"event": {"text": "teste", "user": "U1"}}

    handle_app_mentions(body, mock_say)

    last_call_args = mock_say.call_args[0][0]
    assert "Erro crítico" in last_call_args or "Erro Catastrófico" in last_call_args

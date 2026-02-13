"""
Unit tests for Genie-only routing and Slack mention handling.
"""
# pylint: disable=import-outside-toplevel
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def clear_conversation_state():
    """Limpa estado global de conversa entre testes."""
    from data_slacklake.services import ai_service

    ai_service._CONVERSATION_STATE.clear()  # pylint: disable=protected-access
    yield
    ai_service._CONVERSATION_STATE.clear()  # pylint: disable=protected-access


@patch("data_slacklake.services.ai_service.ask_genie")
def test_process_question_uses_default_genie_space(mock_ask_genie):
    """Usa GENIE_SPACE_ID quando não há alias no início da pergunta."""
    mock_ask_genie.return_value = ("Resposta Genie", "SELECT 1", "conv-1")

    with patch("data_slacklake.services.ai_service.GENIE_SPACE_ID", "space-default"), patch(
        "data_slacklake.services.ai_service.GENIE_BOT_SPACE_MAP", ""
    ):
        from data_slacklake.services.ai_service import process_question

        resposta, sql = process_question("Qual o total?")

    assert resposta == "Resposta Genie"
    assert sql == "SELECT 1"
    mock_ask_genie.assert_called_once_with(space_id="space-default", pergunta="Qual o total?", conversation_id=None)


@patch("data_slacklake.services.ai_service.ask_genie")
def test_process_question_routes_by_alias(mock_ask_genie):
    """Seleciona o space correto quando pergunta começa com !alias."""
    mock_ask_genie.return_value = ("Resposta Remessa", None, "conv-remessa")

    with patch("data_slacklake.services.ai_service.GENIE_SPACE_ID", "space-default"), patch(
        "data_slacklake.services.ai_service.GENIE_BOT_SPACE_MAP",
        '{"!remessagpt":"space-remessa","!marketing":"space-mkt"}',
    ):
        from data_slacklake.services.ai_service import process_question

        resposta, sql = process_question("!RemessaGpt quanto operações tivemos esse ano?")

    assert resposta == "Resposta Remessa"
    assert sql is None
    mock_ask_genie.assert_called_once_with(
        space_id="space-remessa",
        pergunta="quanto operações tivemos esse ano?",
        conversation_id=None,
    )


@patch("data_slacklake.services.ai_service.ask_genie")
def test_process_question_unknown_alias_returns_help(mock_ask_genie):
    """Retorna mensagem orientativa quando alias solicitado não existe."""
    with patch("data_slacklake.services.ai_service.GENIE_SPACE_ID", ""), patch(
        "data_slacklake.services.ai_service.GENIE_BOT_SPACE_MAP",
        '{"!remessagpt":"space-remessa","!marketing":"space-mkt"}',
    ):
        from data_slacklake.services.ai_service import process_question

        resposta, sql = process_question("!financeiro qual foi a receita?")

    assert "Não encontrei a Genie" in resposta
    assert "!remessagpt" in resposta
    assert "!marketing" in resposta
    assert sql is None
    mock_ask_genie.assert_not_called()


@patch("data_slacklake.services.ai_service.ask_genie")
def test_process_question_requires_alias_without_default_space(mock_ask_genie):
    """Exige !alias quando não existe Genie padrão definida."""
    with patch("data_slacklake.services.ai_service.GENIE_SPACE_ID", ""), patch(
        "data_slacklake.services.ai_service.GENIE_BOT_SPACE_MAP",
        '{"!remessagpt":"space-remessa","!marketing":"space-mkt"}',
    ):
        from data_slacklake.services.ai_service import process_question

        resposta, sql = process_question("qual foi a receita?")

    assert "Informe a Genie" in resposta
    assert "!remessagpt" in resposta
    assert sql is None
    mock_ask_genie.assert_not_called()


@patch("data_slacklake.services.ai_service.ask_genie")
def test_genie_reuses_conversation_id_across_turns_same_space(mock_ask_genie):
    """Reaproveita conversation_id no segundo turno para o mesmo space."""
    mock_ask_genie.side_effect = [
        ("Resposta 1", "SELECT 1", "conv-1"),
        ("Resposta 2", "SELECT 2", "conv-1"),
    ]

    with patch("data_slacklake.services.ai_service.GENIE_SPACE_ID", "space-default"), patch(
        "data_slacklake.services.ai_service.GENIE_BOT_SPACE_MAP", ""
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


@patch("data_slacklake.services.ai_service.ask_genie")
def test_genie_conversation_id_is_isolated_per_space(mock_ask_genie):
    """Mantém conversation_id separado por space dentro da mesma thread."""
    mock_ask_genie.side_effect = [
        ("Resp Remessa 1", None, "conv-remessa"),
        ("Resp Marketing 1", None, "conv-marketing"),
        ("Resp Remessa 2", None, "conv-remessa"),
    ]

    with patch("data_slacklake.services.ai_service.GENIE_SPACE_ID", ""), patch(
        "data_slacklake.services.ai_service.GENIE_BOT_SPACE_MAP",
        '{"!remessagpt":"space-remessa","!marketing":"space-mkt"}',
    ):
        from data_slacklake.services.ai_service import process_question

        conversation_key = "conv-space-isolation-1"
        process_question("!remessagpt pergunta 1", conversation_key=conversation_key)
        process_question("!marketing pergunta 2", conversation_key=conversation_key)
        process_question("!remessagpt pergunta 3", conversation_key=conversation_key)

    primeira_chamada = mock_ask_genie.call_args_list[0].kwargs
    segunda_chamada = mock_ask_genie.call_args_list[1].kwargs
    terceira_chamada = mock_ask_genie.call_args_list[2].kwargs

    assert primeira_chamada["space_id"] == "space-remessa"
    assert primeira_chamada["conversation_id"] is None
    assert segunda_chamada["space_id"] == "space-mkt"
    assert segunda_chamada["conversation_id"] is None
    assert terceira_chamada["space_id"] == "space-remessa"
    assert terceira_chamada["conversation_id"] == "conv-remessa"


@patch("data_slacklake.services.ai_service.process_question")
def test_app_mention_success(mock_process):
    """Responde no Slack com mensagem inicial e resposta final da IA."""
    mock_process.return_value = ("Resposta Final da IA", "SELECT * FROM debug")

    mock_say = MagicMock()
    event_body = {
        "event": {
            "text": "<@BOT_ID> !RemessaGpt analyze os dados",
            "user": "USER_ID",
            "channel": "C123",
            "ts": "12345.6789",
        }
    }

    from main import handle_app_mentions

    handle_app_mentions(event_body, mock_say)

    mock_process.assert_called_with("!RemessaGpt analyze os dados", conversation_key="slack:C123:12345.6789:USER_ID")
    assert mock_say.call_count >= 2
    assert any(call.args and call.args[0] == "Resposta Final da IA" for call in mock_say.call_args_list)

    debug_call = mock_say.call_args_list[-1]
    assert "SELECT * FROM debug" in debug_call[0][0]
    assert debug_call[1]["thread_ts"] == "12345.6789"


@patch("data_slacklake.services.ai_service.process_question")
def test_app_mention_error(mock_process):
    """Notifica erro crítico quando processamento levanta exceção."""
    mock_process.side_effect = Exception("Erro Catastrófico")

    mock_say = MagicMock()
    body = {"event": {"text": "teste", "user": "U1"}}

    from main import handle_app_mentions

    handle_app_mentions(body, mock_say)

    last_call_args = mock_say.call_args[0][0]
    assert "Erro crítico" in last_call_args or "Erro Catastrófico" in last_call_args


@patch("data_slacklake.services.ai_service.list_configured_genie_commands", return_value=["!remessagpt", "!marketing"])
def test_app_mention_without_question_shows_usage(_mock_commands):
    """Mostra instruções e comandos quando menção vem sem pergunta."""
    mock_say = MagicMock()
    body = {
        "event": {
            "text": "<@BOT_ID>",
            "user": "U1",
            "channel": "C1",
            "ts": "111.222",
        }
    }

    from main import handle_app_mentions

    handle_app_mentions(body, mock_say)

    message = mock_say.call_args[0][0]
    assert "Comandos configurados" in message
    assert "!remessagpt" in message


def test_build_event_log_summary_redacts_sensitive_data():
    """Resumo de logs não deve vazar token nem assinatura."""
    from main import _build_event_log_summary, _lowercase_headers

    event = {"httpMethod": "POST", "path": "/v1/data-slacklake/bot"}
    headers = _lowercase_headers(
        {
            "User-Agent": "Slackbot 1.0",
            "X-Slack-Request-Timestamp": "1770926438",
            "X-Slack-Signature": "v0=abc123",
            "X-Forwarded-For": "54.91.163.226",
        }
    )
    body_json = {
        "type": "event_callback",
        "event_id": "Ev123",
        "team_id": "TL3PXCH4L",
        "token": "token-ultra-secreto",
        "event": {
            "type": "app_mention",
            "user": "U1",
            "channel": "C1",
            "text": "<@BOT> segredo",
            "ts": "123.456",
        },
    }

    summary = _build_event_log_summary(event, headers, body_json)

    assert summary["headers"]["x-slack-signature"] == "[REDACTED]"
    assert summary["slack_event"]["event_id"] == "Ev123"
    assert summary["slack_event"]["event_type"] == "app_mention"
    assert "token-ultra-secreto" not in str(summary)


def test_is_duplicate_slack_event_detects_retries_by_event_id():
    """Evita reprocessar o mesmo event_id mais de uma vez."""
    from main import _PROCESSED_EVENT_IDS, _is_duplicate_slack_event

    _PROCESSED_EVENT_IDS.clear()  # pylint: disable=protected-access
    body_json = {"type": "event_callback", "event_id": "EvDup123", "event": {"type": "app_mention"}}

    is_duplicate_first, event_id_first = _is_duplicate_slack_event(body_json)
    is_duplicate_second, event_id_second = _is_duplicate_slack_event(body_json)

    assert is_duplicate_first is False
    assert event_id_first == "EvDup123"
    assert is_duplicate_second is True
    assert event_id_second == "EvDup123"

    _PROCESSED_EVENT_IDS.clear()  # pylint: disable=protected-access

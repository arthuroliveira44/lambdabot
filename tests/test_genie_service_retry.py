"""Testes de retry para integração com Databricks Genie."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


def test_ask_genie_retry_em_erro_transitorio_e_sucesso_na_tentativa_seguinte():
    """Deve aplicar retry quando o Genie estiver temporariamente indisponível."""
    from data_slacklake.services import genie_service

    mock_genie_api = MagicMock()
    mock_genie_api.start_conversation_and_wait.side_effect = [
        RuntimeError("service temporarily unavailable"),
        SimpleNamespace(attachments=[], error=None, conversation_id="conv-1"),
    ]
    mock_client = SimpleNamespace(genie=mock_genie_api)

    with (
        patch.object(genie_service, "get_workspace_client", return_value=mock_client),
        patch.object(genie_service, "GENIE_RETRY_ATTEMPTS", 3),
        patch.object(genie_service, "GENIE_RETRY_BASE_DELAY_SECONDS", 0.01),
        patch.object(genie_service, "GENIE_RETRY_MAX_DELAY_SECONDS", 0.01),
        patch.object(genie_service.time, "sleep", return_value=None) as mock_sleep,
    ):
        response_text, sql_debug, conversation_id = genie_service.ask_genie(
            space_id="space-test",
            pergunta="pergunta teste",
            conversation_id=None,
        )

    assert mock_genie_api.start_conversation_and_wait.call_count == 2
    assert mock_sleep.call_count == 1
    assert sql_debug is None
    assert conversation_id == "conv-1"
    assert "resposta textual" in response_text.lower()


def test_ask_genie_nao_retry_para_erro_nao_transitorio():
    """Não deve fazer retry quando erro indica space inexistente/config inválida."""
    from data_slacklake.services import genie_service

    mock_genie_api = MagicMock()
    mock_genie_api.start_conversation_and_wait.side_effect = RuntimeError(
        "Unable to get space [abc]. does not exist"
    )
    mock_client = SimpleNamespace(genie=mock_genie_api)

    with (
        patch.object(genie_service, "get_workspace_client", return_value=mock_client),
        patch.object(genie_service, "GENIE_RETRY_ATTEMPTS", 3),
        patch.object(genie_service.time, "sleep", return_value=None) as mock_sleep,
    ):
        with pytest.raises(RuntimeError, match="Unable to get space"):
            genie_service.ask_genie(space_id="space-test", pergunta="pergunta teste", conversation_id=None)

    assert mock_genie_api.start_conversation_and_wait.call_count == 1
    mock_sleep.assert_not_called()

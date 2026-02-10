"""
Pytest fixtures and configuration for the test suite.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("GENIE_ENABLED", "false")
os.environ.pop("GENIE_SPACE_ID", None)
os.environ.pop("GENIE_SPACE_MAP", None)

os.environ.setdefault("app_env", "test")
os.environ.setdefault("SLACK_BOT_TOKEN", "xoxb-test-token")
os.environ.setdefault("SLACK_SIGNING_SECRET", "test-secret")
os.environ.setdefault("DATABRICKS_TOKEN", "test-db-token")
os.environ.setdefault("DATABRICKS_HOST", "test.databricks.com")
os.environ.setdefault("DATABRICKS_HTTP_PATH", "/sql/1.0/endpoints/test")

os.environ.setdefault("LANGCHAIN_TRACING_V2", "false")
os.environ.setdefault("LANGCHAIN_TRACING", "false")
os.environ.setdefault("LANGSMITH_TRACING", "false")

mock_ssm_client = MagicMock()
mock_ssm_client.get_parameter.return_value = {
    "Parameter": {
        "Value": "dummy_secret_value_for_testing"
    }
}

patcher_boto = patch("boto3.client", return_value=mock_ssm_client)
patcher_boto.start()

try:
    import data_slacklake.config as cfg  # pylint: disable=import-outside-toplevel

    cfg.GENIE_ENABLED = False
    cfg.GENIE_SPACE_ID = ""
    cfg.GENIE_SPACE_MAP = ""
except Exception:
    pass

mock_auth_response = {
    "ok": True,
    "url": "https://test.slack.com/",
    "team": "Test Data",
    "user": "test_bot",
    "team_id": "T12345",
    "user_id": "U12345",
    "bot_id": "B12345",
    "enterprise_id": None,
}

patcher_slack = patch("slack_sdk.web.client.WebClient.auth_test", return_value=mock_auth_response)
patcher_slack.start()



@pytest.fixture(scope="session", autouse=True)
def stop_global_patches():
    """
    Garante que os patches parem ao final dos testes.
    """
    yield
    patcher_boto.stop()
    patcher_slack.stop()

@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """
    Define variáveis de ambiente obrigatórias.
    """
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test-token")
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("DATABRICKS_TOKEN", "test-db-token")
    monkeypatch.setenv("app_env", "test")
    monkeypatch.setenv("DATABRICKS_HOST", "test.databricks.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/endpoints/test")


@pytest.fixture(autouse=True)
def block_real_genie_calls():
    """
    Bloqueia chamadas reais ao Genie durante testes para evitar travamentos por rede.
    O teste específico do Genie mocka `ask_genie`, então continua funcionando.
    """
    with patch("data_slacklake.services.genie_service.ask_genie", side_effect=RuntimeError("Genie bloqueado em testes")):
        yield

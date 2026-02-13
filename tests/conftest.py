"""
Pytest fixtures and configuration for the test suite.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

BASE_ENV_VARS = {
    "app_env": "test",
    "SLACK_BOT_TOKEN": "xoxb-test-token",
    "SLACK_SIGNING_SECRET": "test-secret",
    "DATABRICKS_TOKEN": "test-db-token",
    "DATABRICKS_HOST": "test.databricks.com",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/endpoints/test",
    "GENIE_SPACE_ID": "space-default",
}

TRACING_ENV_VARS = {
    "LANGCHAIN_TRACING_V2": "false",
    "LANGCHAIN_TRACING": "false",
    "LANGSMITH_TRACING": "false",
}

for env_name, env_value in {**BASE_ENV_VARS, **TRACING_ENV_VARS}.items():
    os.environ.setdefault(env_name, env_value)

os.environ.setdefault("GENIE_BOT_SPACE_MAP", "")

mocked_ssm_client = MagicMock()
mocked_ssm_client.get_parameter.return_value = {
    "Parameter": {
        "Value": "dummy_secret_value_for_testing",
    }
}
mocked_ssm_client.get_parameters.return_value = {
    "Parameters": [],
    "InvalidParameters": [],
}

patcher_boto = patch("boto3.client", return_value=mocked_ssm_client)
patcher_boto.start()

try:
    import data_slacklake.config as cfg  # pylint: disable=import-outside-toplevel

    cfg.GENIE_SPACE_ID = "space-default"
    cfg.GENIE_BOT_SPACE_MAP = ""
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
    for base_env_name, base_env_value in BASE_ENV_VARS.items():
        monkeypatch.setenv(base_env_name, base_env_value)


@pytest.fixture(autouse=True)
def block_real_genie_calls():
    """
    Bloqueia chamadas reais ao Genie durante testes para evitar travamentos por rede.
    O teste específico do Genie mocka `ask_genie`, então continua funcionando.
    """
    with patch("data_slacklake.services.genie_service.ask_genie", side_effect=RuntimeError("Genie bloqueado em testes")):
        yield

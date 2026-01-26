"""
Pytest fixtures and configuration for the test suite.
"""

from unittest.mock import MagicMock, patch

import pytest

mock_ssm_client = MagicMock()
mock_ssm_client.get_parameter.return_value = {
    "Parameter": {
        "Value": "dummy_secret_value_for_testing"
    }
}

patcher_boto = patch("boto3.client", return_value=mock_ssm_client)
patcher_boto.start()

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

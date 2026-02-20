"""Testes de autenticação do cliente Databricks Genie."""

from unittest.mock import patch

import pytest


def test_get_workspace_client_prefere_service_principal_sobre_pat():
    """Quando SP e PAT coexistem, deve priorizar Service Principal."""
    from data_slacklake.services import genie_service

    genie_service.get_workspace_client.cache_clear()
    with (
        patch.object(genie_service, "DATABRICKS_HOST", "https://dbc-test.cloud.databricks.com"),
        patch.object(genie_service, "DATABRICKS_CLIENT_ID", "sp-client-id"),
        patch.object(genie_service, "DATABRICKS_CLIENT_SECRET", "sp-client-secret"),
        patch.object(genie_service, "DATABRICKS_TOKEN", "legacy-pat-token"),
        patch.object(genie_service, "WorkspaceClient") as mock_workspace_client,
    ):
        genie_service.get_workspace_client()

    mock_workspace_client.assert_called_once_with(
        host="https://dbc-test.cloud.databricks.com",
        client_id="sp-client-id",
        client_secret="sp-client-secret",
    )


def test_get_workspace_client_usa_pat_quando_sp_nao_esta_configurado():
    """Deve usar token PAT quando client_id/client_secret não existem."""
    from data_slacklake.services import genie_service

    genie_service.get_workspace_client.cache_clear()
    with (
        patch.object(genie_service, "DATABRICKS_HOST", "https://dbc-test.cloud.databricks.com"),
        patch.object(genie_service, "DATABRICKS_CLIENT_ID", ""),
        patch.object(genie_service, "DATABRICKS_CLIENT_SECRET", ""),
        patch.object(genie_service, "DATABRICKS_TOKEN", "pat-token"),
        patch.object(genie_service, "WorkspaceClient") as mock_workspace_client,
    ):
        genie_service.get_workspace_client()

    mock_workspace_client.assert_called_once_with(
        host="https://dbc-test.cloud.databricks.com",
        token="pat-token",
    )


def test_get_workspace_client_falha_quando_credenciais_sp_estao_incompletas():
    """Client ID sem secret (ou vice-versa) deve falhar explicitamente."""
    from data_slacklake.services import genie_service

    genie_service.get_workspace_client.cache_clear()
    with (
        patch.object(genie_service, "DATABRICKS_HOST", "https://dbc-test.cloud.databricks.com"),
        patch.object(genie_service, "DATABRICKS_CLIENT_ID", "sp-client-id"),
        patch.object(genie_service, "DATABRICKS_CLIENT_SECRET", ""),
        patch.object(genie_service, "DATABRICKS_TOKEN", ""),
    ):
        with pytest.raises(ValueError, match="Credenciais Databricks incompletas"):
            genie_service.get_workspace_client()

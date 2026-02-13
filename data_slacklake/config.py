"""
Manages environment variables, SSM secrets, and connection settings for Slack and Databricks.
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache

import boto3

logger = logging.getLogger("DatabricksBot")
logger.setLevel(logging.INFO)

DEFAULT_AWS_REGION = "us-east-1"

APP_ENV = os.getenv("app_env", "dev")
SSM_PREFIX = f"/{APP_ENV}/data-slacklake"

@lru_cache(maxsize=16)
def get_ssm_param(prefix: str, param_name: str, required: bool = True) -> str | None:
    """
    Search for the secret by combining the prefix + parameter name.
    """

    full_path = f"{prefix}/{param_name}"

    try:
        ssm_client = boto3.client("ssm", region_name=DEFAULT_AWS_REGION)

        logger.info("SSM Fetch: %s", full_path)
        response = ssm_client.get_parameter(Name=full_path, WithDecryption=True)
        return response["Parameter"]["Value"]

    except Exception as exc:
        error_message = f"ERRO SSM: Falha ao ler '{full_path}'. Erro: {str(exc)}"
        if required:
            logger.error(error_message)
            raise ValueError(error_message) from exc

        logger.warning(error_message)
        return None


SLACK_BOT_TOKEN = get_ssm_param(SSM_PREFIX, "slack_bot_token")
SLACK_SIGNING_SECRET = get_ssm_param(SSM_PREFIX, "slack_signing_secret", required=False) or get_ssm_param(
    SSM_PREFIX,
    "slack_app_token",
)
DATABRICKS_TOKEN = get_ssm_param(SSM_PREFIX, "databricks_pat_token")
DATABRICKS_HOST = get_ssm_param(SSM_PREFIX, "databricks_url")
DATABRICKS_HTTP_PATH = get_ssm_param(SSM_PREFIX, "databricks_http_path")

# Genie padrão usada quando o usuário não informar comando (!nome).
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f105e3c99e1527b3cb9bd0f5418626")

# Mapeamento de aliases para Space IDs.
# Exemplo: {"!remessagpt": "space-1", "!remessafin": "space-2", "!marketing": "space-3"}
# Mantém fallback para GENIE_SPACE_MAP por compatibilidade de deploy.
GENIE_BOT_SPACE_MAP = os.getenv("GENIE_BOT_SPACE_MAP") or os.getenv("GENIE_SPACE_MAP", "")

if DATABRICKS_HOST:
    os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST

if DATABRICKS_TOKEN:
    os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN

logger.info("Configurações carregadas e ambiente Databricks configurado.")

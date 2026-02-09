"""
Manages environment variables, SSM secrets, and connection settings for Slack and Databricks.
"""
import logging
import os
from functools import lru_cache

import boto3

logger = logging.getLogger("DatabricksBot")
logger.setLevel(logging.INFO)

APP_ENV = os.getenv("app_env", "dev")

SSM_PREFIX = f"/{APP_ENV}/data-slacklake"


@lru_cache(maxsize=16)
def get_ssm_param(prefix, param_name, required=True):
    """
    Search for the secret by combining the prefix + parameter name.
    """

    full_path = f"{prefix}/{param_name}"

    try:
        ssm = boto3.client('ssm', region_name='us-east-1')

        logger.info(f"SSM Fetch: {full_path}")
        response = ssm.get_parameter(Name=full_path, WithDecryption=True)
        return response['Parameter']['Value']

    except Exception as e:
        error_msg = f"ERRO CRÍTICO SSM: Falha ao ler '{full_path}'. Erro: {str(e)}"
        logger.error(error_msg)

        if required:
            raise ValueError(error_msg) from e
        return None


SLACK_BOT_TOKEN = get_ssm_param(SSM_PREFIX, "slack_bot_token")
SLACK_SIGNING_SECRET = get_ssm_param(SSM_PREFIX, "slack_app_token")
DATABRICKS_TOKEN = get_ssm_param(SSM_PREFIX, "databricks_pat_token")
DATABRICKS_HOST = get_ssm_param(SSM_PREFIX, "databricks_url")
DATABRICKS_HTTP_PATH = get_ssm_param(SSM_PREFIX, "databricks_http_path")
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT", "databricks-gpt-5-2")

# Genie (Databricks)
def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


# Default "true" conforme solicitado.
GENIE_ENABLED = _env_bool("GENIE_ENABLED", "true")
GENIE_SPACE_MAP = os.getenv("GENIE_SPACE_MAP", "").strip()


if DATABRICKS_HOST:
    os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST

if DATABRICKS_TOKEN:
    os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN

logger.info("Configurações carregadas e ambiente Databricks configurado.")

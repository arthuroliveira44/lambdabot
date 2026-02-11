"""
Manages environment variables, SSM secrets, and connection settings for Slack and Databricks.
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Final

import boto3

logger = logging.getLogger("DatabricksBot")
logger.setLevel(logging.INFO)

DEFAULT_AWS_REGION: Final[str] = "us-east-1"
TRUTHY_VALUES: Final[set[str]] = {"1", "true", "t", "yes", "y", "on"}
FALSY_VALUES: Final[set[str]] = {"0", "false", "f", "no", "n", "off"}

APP_ENV = os.getenv("app_env", "dev")
SSM_PREFIX = f"/{APP_ENV}/data-slacklake"


def _parse_bool_env(env_value: str | bool | None, *, default: bool) -> bool:
    """Parseia variáveis de ambiente booleanas de forma resiliente."""
    if isinstance(env_value, bool):
        return env_value
    if env_value is None:
        return default

    normalized_value = str(env_value).strip().lower()
    if normalized_value in TRUTHY_VALUES:
        return True
    if normalized_value in FALSY_VALUES:
        return False

    logger.warning(
        "Valor booleano inválido para ambiente: %s. Usando default=%s.",
        env_value,
        default,
    )
    return default


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
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT", "databricks-llama-4-maverick")
GENIE_ENABLED = _parse_bool_env(os.getenv("GENIE_ENABLED"), default=True)
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f105e3c99e1527b3cb9bd0f5418626")
GENIE_SPACE_MAP = os.getenv("GENIE_SPACE_MAP", "")

if DATABRICKS_HOST:
    os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST

if DATABRICKS_TOKEN:
    os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN

logger.info("Configurações carregadas e ambiente Databricks configurado.")

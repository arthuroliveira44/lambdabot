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

DEFAULT_AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

APP_ENV = os.getenv("app_env", "dev").strip()
SSM_PREFIX = f"/{APP_ENV}/data-slacklake"


@lru_cache(maxsize=1)
def _get_ssm_client():
    return boto3.client("ssm", region_name=DEFAULT_AWS_REGION)


@lru_cache(maxsize=32)
def get_ssm_param(prefix: str, param_name: str, required: bool = True) -> str | None:
    """
    Search for the secret by combining the prefix + parameter name.
    """
    full_path = f"{prefix}/{param_name}"

    try:
        ssm_client = _get_ssm_client()

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


def _get_env_value(*env_var_names: str) -> str | None:
    for env_var_name in env_var_names:
        env_value = os.getenv(env_var_name)
        if env_value is None:
            continue

        normalized_value = env_value.strip()
        if normalized_value:
            return normalized_value
    return None


def _get_config_value(
    *,
    env_var_names: tuple[str, ...],
    ssm_param_names: tuple[str, ...],
    required: bool = True,
) -> str | None:
    env_value = _get_env_value(*env_var_names)
    if env_value is not None:
        return env_value

    for index, ssm_param_name in enumerate(ssm_param_names):
        is_last_candidate = index == len(ssm_param_names) - 1
        value = get_ssm_param(SSM_PREFIX, ssm_param_name, required=required and is_last_candidate)
        if value:
            return value
    return None


SLACK_BOT_TOKEN = _get_config_value(
    env_var_names=("SLACK_BOT_TOKEN",),
    ssm_param_names=("slack_bot_token",),
)
SLACK_SIGNING_SECRET = _get_config_value(
    env_var_names=("SLACK_SIGNING_SECRET",),
    ssm_param_names=(
        "slack_signing_secret",
        "slack_app_token",  # Compatibilidade retroativa com nome legado.
    ),
)
DATABRICKS_TOKEN = _get_config_value(
    env_var_names=("DATABRICKS_TOKEN",),
    ssm_param_names=("databricks_pat_token",),
)
DATABRICKS_HOST = _get_config_value(
    env_var_names=("DATABRICKS_HOST",),
    ssm_param_names=("databricks_url",),
)
DATABRICKS_HTTP_PATH = _get_config_value(
    env_var_names=("DATABRICKS_HTTP_PATH",),
    ssm_param_names=("databricks_http_path",),
)

# Genie padrão usada quando o usuário não informar comando (!nome).
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f105e3c99e1527b3cb9bd0f5418626").strip()

# Mapeamento de aliases para Space IDs.
# Exemplo: {"!remessagpt": "space-1", "!remessafin": "space-2", "!marketing": "space-3"}
GENIE_BOT_SPACE_MAP = (os.getenv("GENIE_BOT_SPACE_MAP") or os.getenv("GENIE_SPACE_MAP", "")).strip()

if DATABRICKS_HOST and not os.getenv("DATABRICKS_HOST"):
    os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST

if DATABRICKS_TOKEN and not os.getenv("DATABRICKS_TOKEN"):
    os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN

logger.info("Configurações carregadas e ambiente Databricks configurado para app_env=%s.", APP_ENV)

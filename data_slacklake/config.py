"""
Manages environment variables, SSM secrets, and connection settings for Slack and Databricks.
"""
import logging
import os
import sys
from functools import lru_cache

import boto3

logger = logging.getLogger("DatabricksBot")
logger.setLevel(logging.INFO)

APP_ENV = os.getenv("app_env", "dev")

SSM_PREFIX = f"/{APP_ENV}/data-slacklake"

# Genie (Databricks)
def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


# Durante testes, evite acesso à AWS/SSM por padrão (plugins podem importar config cedo).
SSM_ENABLED = _env_bool("SSM_ENABLED", "false" if "pytest" in sys.modules else "true")
SSM_REGION = os.getenv("SSM_REGION", os.getenv("AWS_REGION", "us-east-1"))


@lru_cache(maxsize=16)
def get_ssm_param(prefix, param_name, required=True):
    """
    Search for the secret by combining the prefix + parameter name.
    """

    full_path = f"{prefix}/{param_name}"

    try:
        ssm = boto3.client("ssm", region_name=SSM_REGION)

        logger.info(f"SSM Fetch: {full_path}")
        response = ssm.get_parameter(Name=full_path, WithDecryption=True)
        return response['Parameter']['Value']

    except Exception as e:
        error_msg = f"ERRO CRÍTICO SSM: Falha ao ler '{full_path}'. Erro: {str(e)}"
        logger.error(error_msg)

        if required:
            raise ValueError(error_msg) from e
        return None


def _get_setting(*, env_var: str, ssm_param: str, required: bool = True) -> str | None:
    """
    Prioriza variável de ambiente; cai para SSM se habilitado.
    """
    value = os.getenv(env_var, "").strip()
    if value:
        return value

    if not SSM_ENABLED:
        if required:
            raise ValueError(f"Config obrigatória ausente: env {env_var} (SSM desabilitado).")
        return None

    return get_ssm_param(SSM_PREFIX, ssm_param, required=required)


SLACK_BOT_TOKEN = _get_setting(env_var="SLACK_BOT_TOKEN", ssm_param="slack_bot_token")
# OBS: Mantém compatibilidade com o nome atual no SSM ("slack_app_token").
SLACK_SIGNING_SECRET = _get_setting(env_var="SLACK_SIGNING_SECRET", ssm_param="slack_app_token")
DATABRICKS_TOKEN = _get_setting(env_var="DATABRICKS_TOKEN", ssm_param="databricks_pat_token")
DATABRICKS_HOST = _get_setting(env_var="DATABRICKS_HOST", ssm_param="databricks_url")
DATABRICKS_HTTP_PATH = _get_setting(env_var="DATABRICKS_HTTP_PATH", ssm_param="databricks_http_path")
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT", "databricks-gpt-5-2")

# Default "true" conforme solicitado.
GENIE_ENABLED = _env_bool("GENIE_ENABLED", "true")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "").strip()
GENIE_SPACE_MAP = os.getenv("GENIE_SPACE_MAP", "").strip()


if DATABRICKS_HOST:
    os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST

if DATABRICKS_TOKEN:
    os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN

logger.info("Configurações carregadas e ambiente Databricks configurado.")

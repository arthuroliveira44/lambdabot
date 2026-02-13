"""
Manages environment variables, SSM secrets, and connection settings for Slack and Databricks.
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Iterable

import boto3

logger = logging.getLogger("DatabricksBot")
logger.setLevel(logging.INFO)

DEFAULT_AWS_REGION = "us-east-1"

APP_ENV = os.getenv("app_env", "dev")
SSM_PREFIX = f"/{APP_ENV}/data-slacklake"


def _normalize_config_value(raw_value: str | None) -> str | None:
    cleaned_value = (raw_value or "").strip()
    return cleaned_value or None


def _get_first_env_value(*env_names: str) -> str | None:
    for env_name in env_names:
        env_value = _normalize_config_value(os.getenv(env_name))
        if env_value:
            return env_value
    return None


@lru_cache(maxsize=1)
def _get_ssm_client():
    return boto3.client("ssm", region_name=DEFAULT_AWS_REGION)


@lru_cache(maxsize=16)
def _fetch_ssm_params(prefix: str, param_names: tuple[str, ...]) -> dict[str, str]:
    unique_param_names = tuple(sorted({name.strip() for name in param_names if name and name.strip()}))
    if not unique_param_names:
        return {}

    full_paths = [f"{prefix}/{name}" for name in unique_param_names]
    try:
        ssm_client = _get_ssm_client()
        logger.info("SSM Fetch em lote: %s", ", ".join(full_paths))
        response = ssm_client.get_parameters(Names=full_paths, WithDecryption=True)
    except Exception as exc:
        error_message = f"ERRO SSM: Falha ao ler parâmetros em lote: {', '.join(full_paths)}. Erro: {str(exc)}"
        logger.error(error_message)
        raise ValueError(error_message) from exc

    loaded_values: dict[str, str] = {}
    for parameter_data in response.get("Parameters", []):
        full_name = str(parameter_data.get("Name", ""))
        param_name = full_name.rsplit("/", 1)[-1]
        param_value = _normalize_config_value(parameter_data.get("Value"))
        if param_name and param_value:
            loaded_values[param_name] = param_value
    return loaded_values


@lru_cache(maxsize=16)
def get_ssm_param(prefix: str, param_name: str, required: bool = True) -> str | None:
    """
    Search for the secret by combining the prefix + parameter name.
    """
    full_path = f"{prefix}/{param_name}"
    fetched_values = _fetch_ssm_params(prefix, (param_name,))
    resolved_value = fetched_values.get(param_name)
    if resolved_value:
        return resolved_value

    error_message = f"ERRO SSM: Falha ao ler '{full_path}'. Erro: ParameterNotFound"
    if required:
        logger.error(error_message)
        raise ValueError(error_message)

    logger.warning(error_message)
    return None


def _resolve_settings_from_env_and_ssm() -> dict[str, str | None]:
    env_values = {
        "slack_bot_token": _get_first_env_value("SLACK_BOT_TOKEN"),
        "slack_signing_secret": _get_first_env_value("SLACK_SIGNING_SECRET"),
        "slack_app_token": _get_first_env_value("SLACK_APP_TOKEN"),
        "databricks_pat_token": _get_first_env_value("DATABRICKS_TOKEN", "DATABRICKS_PAT_TOKEN"),
        "databricks_url": _get_first_env_value("DATABRICKS_HOST", "DATABRICKS_URL"),
        "databricks_http_path": _get_first_env_value("DATABRICKS_HTTP_PATH"),
    }

    missing_params = [
        key
        for key in (
            "slack_bot_token",
            "slack_signing_secret",
            "databricks_pat_token",
            "databricks_url",
            "databricks_http_path",
        )
        if not env_values.get(key)
    ]

    # Só busca app_token quando pode ser necessário para fallback do signing_secret.
    if not env_values.get("slack_signing_secret") and not env_values.get("slack_app_token"):
        missing_params.append("slack_app_token")

    ssm_values = _fetch_ssm_params(SSM_PREFIX, tuple(missing_params)) if missing_params else {}
    resolved_values = {name: env_values.get(name) or ssm_values.get(name) for name in env_values}

    if not resolved_values.get("slack_signing_secret"):
        fallback_app_token = resolved_values.get("slack_app_token")
        if fallback_app_token:
            logger.warning(
                "slack_signing_secret ausente; usando slack_app_token como fallback temporário. "
                "Recomendado criar '%s/slack_signing_secret'.",
                SSM_PREFIX,
            )
            resolved_values["slack_signing_secret"] = fallback_app_token

    return resolved_values


def _require_setting(value: str | None, *, env_names: Iterable[str], ssm_param_name: str) -> str:
    if value:
        return value
    env_hint = " ou ".join(env_names)
    full_ssm_path = f"{SSM_PREFIX}/{ssm_param_name}"
    raise ValueError(f"Configuração obrigatória ausente. Defina {env_hint} ou o parâmetro SSM '{full_ssm_path}'.")


_resolved_settings = _resolve_settings_from_env_and_ssm()

SLACK_BOT_TOKEN = _require_setting(
    _resolved_settings.get("slack_bot_token"),
    env_names=("SLACK_BOT_TOKEN",),
    ssm_param_name="slack_bot_token",
)
SLACK_SIGNING_SECRET = _require_setting(
    _resolved_settings.get("slack_signing_secret"),
    env_names=("SLACK_SIGNING_SECRET", "SLACK_APP_TOKEN"),
    ssm_param_name="slack_signing_secret",
)
DATABRICKS_TOKEN = _require_setting(
    _resolved_settings.get("databricks_pat_token"),
    env_names=("DATABRICKS_TOKEN", "DATABRICKS_PAT_TOKEN"),
    ssm_param_name="databricks_pat_token",
)
DATABRICKS_HOST = _require_setting(
    _resolved_settings.get("databricks_url"),
    env_names=("DATABRICKS_HOST", "DATABRICKS_URL"),
    ssm_param_name="databricks_url",
)
DATABRICKS_HTTP_PATH = _require_setting(
    _resolved_settings.get("databricks_http_path"),
    env_names=("DATABRICKS_HTTP_PATH",),
    ssm_param_name="databricks_http_path",
)

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

"""
Shared helpers for environment variables and SSM settings.
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


def normalize_config_value(raw_value: str | None) -> str | None:
    """Normalizes values loaded from env/SSM."""
    cleaned_value = (raw_value or "").strip()
    return cleaned_value or None


def get_first_env_value(*env_names: str) -> str | None:
    """Returns first non-empty environment variable value."""
    for env_name in env_names:
        env_value = normalize_config_value(os.getenv(env_name))
        if env_value:
            return env_value
    return None


@lru_cache(maxsize=1)
def get_ssm_client():
    """Creates and caches the SSM client."""
    return boto3.client("ssm", region_name=DEFAULT_AWS_REGION)


@lru_cache(maxsize=32)
def fetch_ssm_params(prefix: str, param_names: tuple[str, ...]) -> dict[str, str]:
    """Loads multiple SSM parameters in a single request."""
    unique_param_names = tuple(sorted({name.strip() for name in param_names if name and name.strip()}))
    if not unique_param_names:
        return {}

    full_paths = [f"{prefix}/{name}" for name in unique_param_names]
    try:
        ssm_client = get_ssm_client()
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
        param_value = normalize_config_value(parameter_data.get("Value"))
        if param_name and param_value:
            loaded_values[param_name] = param_value
    return loaded_values


def require_setting(value: str | None, *, env_names: Iterable[str], ssm_param_name: str) -> str:
    """Raises with actionable message when mandatory setting is missing."""
    if value:
        return value
    env_hint = " ou ".join(env_names)
    full_ssm_path = f"{SSM_PREFIX}/{ssm_param_name}"
    raise ValueError(f"Configuração obrigatória ausente. Defina {env_hint} ou o parâmetro SSM '{full_ssm_path}'.")


def resolve_signing_secret(
    env_values: dict[str, str | None],
    *,
    force_ssm_signing_secret: bool,
) -> dict[str, str | None]:
    """Resolves signing_secret and app_token with optional SSM fallback."""
    missing_params: list[str] = []
    if force_ssm_signing_secret or not env_values.get("slack_signing_secret"):
        missing_params.append("slack_signing_secret")
    if not env_values.get("slack_signing_secret") and not env_values.get("slack_app_token"):
        missing_params.append("slack_app_token")

    ssm_values = fetch_ssm_params(SSM_PREFIX, tuple(missing_params)) if missing_params else {}
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

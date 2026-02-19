"""
Worker configuration for Slack + Databricks + Genie.
"""
from __future__ import annotations

import os

from data_slacklake.config_shared import (
    SSM_PREFIX,
    fetch_ssm_params,
    get_first_env_value,
    logger,
    require_setting,
    resolve_signing_secret,
)


def _resolve_worker_settings() -> dict[str, str | None]:
    env_values = {
        "slack_bot_token": get_first_env_value("SLACK_BOT_TOKEN"),
        "slack_signing_secret": get_first_env_value("SLACK_SIGNING_SECRET"),
        "slack_app_token": get_first_env_value("SLACK_APP_TOKEN"),
        "databricks_pat_token": get_first_env_value("DATABRICKS_TOKEN", "DATABRICKS_PAT_TOKEN"),
        "databricks_url": get_first_env_value("DATABRICKS_HOST", "DATABRICKS_URL"),
        "databricks_http_path": get_first_env_value("DATABRICKS_HTTP_PATH"),
    }

    signing_values = resolve_signing_secret(
        {
            "slack_signing_secret": env_values.get("slack_signing_secret"),
            "slack_app_token": env_values.get("slack_app_token"),
        },
        force_ssm_signing_secret=False,
    )
    env_values["slack_signing_secret"] = signing_values.get("slack_signing_secret")
    env_values["slack_app_token"] = signing_values.get("slack_app_token")

    missing_params = [
        key
        for key in (
            "slack_bot_token",
            "databricks_pat_token",
            "databricks_url",
            "databricks_http_path",
        )
        if not env_values.get(key)
    ]

    ssm_values = fetch_ssm_params(SSM_PREFIX, tuple(missing_params)) if missing_params else {}
    resolved_values = {name: env_values.get(name) or ssm_values.get(name) for name in env_values}

    return resolved_values


_resolved_settings = _resolve_worker_settings()

SLACK_BOT_TOKEN = require_setting(
    _resolved_settings.get("slack_bot_token"),
    env_names=("SLACK_BOT_TOKEN",),
    ssm_param_name="slack_bot_token",
)
SLACK_SIGNING_SECRET = require_setting(
    _resolved_settings.get("slack_signing_secret"),
    env_names=("SLACK_SIGNING_SECRET", "SLACK_APP_TOKEN"),
    ssm_param_name="slack_signing_secret",
)
DATABRICKS_TOKEN = require_setting(
    _resolved_settings.get("databricks_pat_token"),
    env_names=("DATABRICKS_TOKEN", "DATABRICKS_PAT_TOKEN"),
    ssm_param_name="databricks_pat_token",
)
DATABRICKS_HOST = require_setting(
    _resolved_settings.get("databricks_url"),
    env_names=("DATABRICKS_HOST", "DATABRICKS_URL"),
    ssm_param_name="databricks_url",
)
DATABRICKS_HTTP_PATH = require_setting(
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

"""
Ingress-only configuration (Slack signature validation).
"""
from __future__ import annotations

from data_slacklake.config_shared import (
    get_first_env_value,
    logger,
    require_setting,
    resolve_signing_secret,
)


def _resolve_ingress_settings() -> dict[str, str | None]:
    env_values = {
        "slack_signing_secret": get_first_env_value("SLACK_SIGNING_SECRET"),
        "slack_app_token": get_first_env_value("SLACK_APP_TOKEN"),
    }
    return resolve_signing_secret(env_values, force_ssm_signing_secret=False)


_resolved_settings = _resolve_ingress_settings()

SLACK_SIGNING_SECRET = require_setting(
    _resolved_settings.get("slack_signing_secret"),
    env_names=("SLACK_SIGNING_SECRET", "SLACK_APP_TOKEN"),
    ssm_param_name="slack_signing_secret",
)

logger.info("Configurações de ingress carregadas.")

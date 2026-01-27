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
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


@lru_cache(maxsize=16)
def get_ssm_param(prefix, param_name, required=True):
    """
    Search for the secret by combining the prefix + parameter name.
    """

    full_path = f"{prefix}/{param_name}"

    try:
        ssm = boto3.client('ssm', region_name=AWS_REGION)

        logger.info(f"SSM Fetch: {full_path}")
        response = ssm.get_parameter(Name=full_path, WithDecryption=True)
        return response['Parameter']['Value']

    except Exception as e:
        error_msg = f"ERRO CRÍTICO SSM: Falha ao ler '{full_path}'. Erro: {str(e)}"
        logger.error(error_msg)

        if required:
            raise ValueError(error_msg) from e
        return None


SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN") or get_ssm_param(SSM_PREFIX, "slack_bot_token")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET") or get_ssm_param(SSM_PREFIX, "slack_app_token")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN") or get_ssm_param(SSM_PREFIX, "databricks_pat_token")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST") or get_ssm_param(SSM_PREFIX, "databricks_url")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH") or get_ssm_param(SSM_PREFIX, "databricks_http_path")
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT", "databricks-gpt-5-2")

# Async processing (optional; used when ASYNC_MODE=true)
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL") or get_ssm_param(SSM_PREFIX, "sqs_queue_url", required=False)
IDEMPOTENCY_TABLE_NAME = os.getenv("IDEMPOTENCY_TABLE_NAME") or get_ssm_param(
    SSM_PREFIX, "idempotency_table_name", required=False
)
ASYNC_MODE = os.getenv("ASYNC_MODE", "false").strip().lower() == "true"
ASYNC_ENABLED = bool(ASYNC_MODE and SQS_QUEUE_URL)


if DATABRICKS_HOST:
    os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST

if DATABRICKS_TOKEN:
    os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN

logger.info("Configurações carregadas e ambiente Databricks configurado.")

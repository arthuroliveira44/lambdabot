"""
Data Access Object (DAO) layer responsible for executing queries against Databricks SQL.
"""

import hashlib

from databricks import sql

from data_slacklake.config import (
    APP_ENV,
    DATABRICKS_HOST,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_TOKEN,
    logger,
)

_NON_PROD_ENVS = {"dev", "test"}


def _is_non_prod() -> bool:
    return (APP_ENV or "dev").lower() in _NON_PROD_ENVS


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _preview_sql(query: str, max_len: int = 140) -> str:
    compact = " ".join((query or "").split())
    if len(compact) <= max_len:
        return compact
    return compact[:max_len] + "…"


def execute_query(query):
    """Connect to Databricks and run the SQL"""
    try:
        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                if _is_non_prod():
                    logger.info("Executando SQL: %s", query)
                else:
                    logger.info(
                        "Executando SQL (redacted) sha256=%s preview=%s",
                        _sha256(query),
                        _preview_sql(query),
                    )
                cursor.execute(query)
                result = cursor.fetchall()
                colunas = [desc[0] for desc in cursor.description]
                return colunas, result
    except Exception as e:
        logger.error(f"Erro Databricks SQL: {e}")
        raise e

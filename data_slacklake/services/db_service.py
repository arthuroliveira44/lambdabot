"""
Data Access Object (DAO) layer responsible for executing queries against Databricks SQL.
"""
from __future__ import annotations

from typing import Any

from databricks import sql

from data_slacklake.config import (
    DATABRICKS_HOST,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_TOKEN,
    logger,
)


def execute_query(query: str) -> tuple[list[str], list[Any]]:
    """Executa SQL no Databricks e retorna colunas + linhas."""
    try:
        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        ) as connection:
            with connection.cursor() as cursor:
                logger.info("Executando SQL: %s", query)
                cursor.execute(query)
                rows = cursor.fetchall()
                column_names = [description[0] for description in cursor.description]
                return column_names, rows
    except Exception as exc:
        logger.error("Erro Databricks SQL: %s", exc)
        raise

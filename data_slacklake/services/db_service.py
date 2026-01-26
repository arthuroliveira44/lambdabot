"""
Data Access Object (DAO) layer responsible for executing queries against Databricks SQL.
"""

from databricks import sql

from data_slacklake.config import (
    DATABRICKS_HOST,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_TOKEN,
    logger,
)


def execute_query(query):
    """Connect to Databricks and run the SQL"""
    try:
        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                logger.info(f"Executando SQL: {query}")
                cursor.execute(query)
                result = cursor.fetchall()
                colunas = [desc[0] for desc in cursor.description]
                return colunas, result
    except Exception as e:
        logger.error(f"Erro Databricks SQL: {e}")
        raise e

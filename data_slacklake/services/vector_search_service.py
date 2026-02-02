"""
Serviço de recuperação Top-K via Databricks Vector Search.

O objetivo é reduzir o catálogo para poucos candidatos antes do LLM,
mantendo o custo de tokens previsível conforme o catálogo cresce.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List

from data_slacklake.config import DATABRICKS_HOST, DATABRICKS_TOKEN, logger


def _is_configured() -> bool:
    return bool(
        os.getenv("VECTOR_SEARCH_ENDPOINT")
        and os.getenv("VECTOR_SEARCH_INDEX")
        and (DATABRICKS_HOST or os.getenv("DATABRICKS_HOST"))
        and (DATABRICKS_TOKEN or os.getenv("DATABRICKS_TOKEN"))
    )


def retrieve_top_k_catalog(
    pergunta: str,
    k: int = 5,
    *,
    columns: List[str] | None = None,
    score_threshold: float | None = None,
) -> List[Dict[str, Any]]:
    """
    Recupera Top-K documentos do catálogo (um doc por definição).

    Espera que o índice tenha pelo menos uma coluna identificadora (ex.: 'id').
    Retorna lista de dicts no formato: {"id": ..., "score": ..., ...}
    """
    if not _is_configured():
        return []

    endpoint = os.getenv("VECTOR_SEARCH_ENDPOINT")
    index_name = os.getenv("VECTOR_SEARCH_INDEX")

    # Evite importar libs no import do módulo (cold start).
    # pylint: disable=import-outside-toplevel
    try:
        from databricks.vector_search.client import VectorSearchClient
    except Exception as e:  # pragma: no cover
        logger.warning("Vector Search não disponível: %s", str(e))
        return []

    try:
        client = VectorSearchClient(
            workspace_url=DATABRICKS_HOST or os.getenv("DATABRICKS_HOST"),
            personal_access_token=DATABRICKS_TOKEN or os.getenv("DATABRICKS_TOKEN"),
        )
        index = client.get_index(endpoint_name=endpoint, index_name=index_name)
        cols = columns or ["id"]
        resp = index.similarity_search(
            columns=cols,
            query_text=pergunta,
            num_results=k,
            score_threshold=score_threshold,
            query_type=os.getenv("VECTOR_SEARCH_QUERY_TYPE", "HYBRID"),
        )
    except Exception as e:
        logger.warning("Falha ao consultar Vector Search (%s/%s): %s", endpoint, index_name, str(e))
        return []

    # Formato típico:
    # resp["result"]["data_array"] = [[v1, v2, ...], ...]
    # resp["manifest"]["columns"] = [{"name": "id", ...}, {"name": "..."}]
    try:
        data = (resp or {}).get("result", {}).get("data_array", []) or []
        manifest_cols = (resp or {}).get("manifest", {}).get("columns", []) or []
        col_names = [c.get("name") for c in manifest_cols if isinstance(c, dict) and c.get("name")]

        # Fallback: se não houver manifest, assume que a ordem é a solicitada.
        if not col_names:
            col_names = cols

        out: List[Dict[str, Any]] = []
        for row in data:
            if not isinstance(row, list):
                continue
            item = {col_names[i]: row[i] for i in range(min(len(col_names), len(row)))}
            # Alguns índices incluem score; se vier separado, ignoramos (não é crítico).
            out.append(item)
        return out
    except Exception as e:  # pragma: no cover
        logger.warning("Falha ao parsear resposta do Vector Search: %s", str(e))
        return []


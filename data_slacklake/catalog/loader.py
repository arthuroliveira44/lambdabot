"""
Carregador de catálogo com merge:

- Catálogo curado (manual) em `data_slacklake.catalog.definitions.CATALOGO`
- Catálogo gerado (JSON) opcional via env `GENERATED_CATALOG_PATH`

Regra de merge:
- Manual sobrescreve gerado (manual > gerado)
"""

from __future__ import annotations

import json
import os
from typing import Any

from data_slacklake.catalog.definitions import CATALOGO as CATALOGO_CURADO


def load_catalog() -> dict[str, dict[str, Any]]:
    generated_path = os.getenv("GENERATED_CATALOG_PATH", "data_slacklake/catalog/generated_catalog.json")

    generated: dict[str, dict[str, Any]] = {}
    try:
        with open(generated_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                generated = data  # type: ignore[assignment]
    except Exception:
        generated = {}

    # merge: gerado primeiro, curado por cima
    merged: dict[str, dict[str, Any]] = {}
    merged.update(generated)
    merged.update(CATALOGO_CURADO)
    return merged


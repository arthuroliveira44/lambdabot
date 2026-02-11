"""
Router service designed to classify user intents.
"""
# pylint: disable=import-outside-toplevel
from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from data_slacklake.catalog.definitions import CATALOGO
from data_slacklake.config import LLM_ENDPOINT, logger
from data_slacklake.prompts import ROUTER_TEMPLATE

_WORD_RE = re.compile(r"[a-zA-ZÀ-ÿ0-9_]+", re.UNICODE)
_STOPWORDS_PT = {
    "a", "o", "os", "as", "um", "uma", "uns", "umas",
    "de", "da", "do", "das", "dos", "em", "no", "na", "nos", "nas",
    "e", "ou", "com", "para", "por", "sobre", "entre",
    "qual", "quais", "quanto", "quantos", "quando", "onde", "como", "porque",
    "me", "minha", "meu", "seu", "sua", "suas", "seus",
    "isso", "essa", "esse", "essas", "esses", "isto", "aquele", "aquela",
    "hoje", "ontem", "amanha", "amanhã",
}


@lru_cache(maxsize=4)
def _get_router_llm():
    from databricks_langchain import ChatDatabricks

    return ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)


def _tokenize(text: str) -> set[str]:
    words = [w.lower() for w in _WORD_RE.findall(text or "")]
    return {w for w in words if len(w) >= 3 and w not in _STOPWORDS_PT}


def _truncate_text(text: str, max_chars: int) -> str:
    normalized_text = (text or "").strip()
    if len(normalized_text) <= max_chars:
        return normalized_text
    return normalized_text[: max_chars - 1] + "…"


def _score_entry(question_tokens: set[str], catalog_entry: dict[str, Any]) -> int:
    """
    Heurística simples de matching para reduzir opções (evita explodir tokens).
    """
    tags = catalog_entry.get("tags") or []
    description = catalog_entry.get("descricao_curta") or catalog_entry.get("descricao") or ""
    catalog_tokens = _tokenize(" ".join(tags) + " " + description)
    return len(question_tokens.intersection(catalog_tokens))


def _rank_catalog_entries(question_tokens: set[str]) -> list[tuple[str, int]]:
    scored_entries: list[tuple[str, int]] = []
    for catalog_id, catalog_entry in CATALOGO.items():
        scored_entries.append((catalog_id, _score_entry(question_tokens, catalog_entry)))
    scored_entries.sort(key=lambda item: (-item[1], item[0]))
    return scored_entries


def _select_candidate_ids(scored_entries: list[tuple[str, int]]) -> list[str]:
    best_score = scored_entries[0][1] if scored_entries else 0
    candidate_limit = 8 if best_score > 0 else 20
    return [catalog_id for catalog_id, _score in scored_entries[:candidate_limit]]


def _build_options_text(candidate_ids: list[str], *, max_total_chars: int = 4000, max_desc_chars: int = 140) -> str:
    """
    Monta lista de opções com orçamento rígido de caracteres.
    """
    lines: list[str] = []
    total = 0
    for cid in candidate_ids:
        entry = CATALOGO.get(cid)
        if not entry:
            continue
        desc = entry.get("descricao_curta") or entry.get("descricao") or ""
        line = f"- ID: {cid} | Descrição: {_truncate_text(desc, max_desc_chars)}"
        if total + len(line) + 1 > max_total_chars:
            break
        lines.append(line)
        total += len(line) + 1
    return "\n".join(lines).strip()


def _normalize_router_output(raw_output: str) -> str:
    return raw_output.replace("ID:", "").strip()


def identify_table(pergunta_usuario: str) -> dict[str, Any] | None:
    """Returns the dictionary for the chosen table or None"""
    llm = _get_router_llm()
    question_tokens = _tokenize(pergunta_usuario)
    ranked_entries = _rank_catalog_entries(question_tokens)
    candidate_ids = _select_candidate_ids(ranked_entries)
    options_text = _build_options_text(candidate_ids)

    prompt = ChatPromptTemplate.from_template(ROUTER_TEMPLATE)
    chain = prompt | llm | StrOutputParser()

    try:
        suggested_table_id = chain.invoke(
            {
                "pergunta": pergunta_usuario,
                "opcoes": options_text,
            }
        ).strip()
        normalized_table_id = _normalize_router_output(suggested_table_id)

        if normalized_table_id.upper() == "NONE":
            return None

        logger.info("Roteador escolheu: %s", normalized_table_id)

        if normalized_table_id in CATALOGO:
            return CATALOGO[normalized_table_id]

        logger.warning("Tabela sugerida '%s' não existe no catálogo.", normalized_table_id)
        return None

    except Exception as exc:
        logger.error("Erro no Router: %s", exc)
        return None

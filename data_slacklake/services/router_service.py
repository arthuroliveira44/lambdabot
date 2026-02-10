"""
Router service designed to classify user intents.
"""
# pylint: disable=import-outside-toplevel
from __future__ import annotations

import re

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


def _tokenize(text: str) -> set[str]:
    words = [w.lower() for w in _WORD_RE.findall(text or "")]
    return {w for w in words if len(w) >= 3 and w not in _STOPWORDS_PT}


def _truncate(text: str, max_chars: int) -> str:
    t = (text or "").strip()
    if len(t) <= max_chars:
        return t
    return t[: max_chars - 1] + "…"


def _score_entry(pergunta_tokens: set[str], entry: dict) -> int:
    """
    Heurística simples de matching para reduzir opções (evita explodir tokens).
    """
    tags = entry.get("tags") or []
    desc = entry.get("descricao_curta") or entry.get("descricao") or ""
    entry_tokens = _tokenize(" ".join(tags) + " " + desc)
    return len(pergunta_tokens.intersection(entry_tokens))


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
        line = f"- ID: {cid} | Descrição: {_truncate(desc, max_desc_chars)}"
        if total + len(line) + 1 > max_total_chars:
            break
        lines.append(line)
        total += len(line) + 1
    return "\n".join(lines).strip()

def identify_table(pergunta_usuario):
    """Returns the dictionary for the chosen table or None"""

    from databricks_langchain import ChatDatabricks

    llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)

    # Pré-filtro local (sem Vector Search) para enviar poucas opções ao LLM.
    pergunta_tokens = _tokenize(pergunta_usuario)
    scored = []
    for cid, entry in CATALOGO.items():
        scored.append((cid, _score_entry(pergunta_tokens, entry)))

    scored.sort(key=lambda x: (-x[1], x[0]))
    best_score = scored[0][1] if scored else 0

    # Se houver sinal, manda Top-K; se não, manda todas (ainda com cap rígido).
    if best_score > 0:
        candidate_ids = [cid for cid, _s in scored[:8]]
    else:
        candidate_ids = [cid for cid, _s in scored[:20]]

    lista_texto = _build_options_text(candidate_ids)

    prompt = ChatPromptTemplate.from_template(ROUTER_TEMPLATE)
    chain = prompt | llm | StrOutputParser()

    try:
        tabela_id = chain.invoke({
            "pergunta": pergunta_usuario,
            "opcoes": lista_texto
        }).strip()

        tabela_id = tabela_id.replace("ID:", "").strip()
        if tabela_id.upper() == "NONE":
            return None

        logger.info(f"Roteador escolheu: {tabela_id}")

        if tabela_id in CATALOGO:
            return CATALOGO[tabela_id]

        logger.warning(f"Tabela sugerida '{tabela_id}' não existe no catálogo.")
        return None

    except Exception as e:
        logger.error(f"Erro no Router: {e}")
        return None

"""
Router service designed to classify user intents.
"""
# pylint: disable=import-outside-toplevel
import re

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from data_slacklake.catalog.definitions import CATALOGO
from data_slacklake.config import LLM_ENDPOINT, logger
from data_slacklake.prompts import ROUTER_TEMPLATE


def _normalize(text: str) -> str:
    return (text or "").strip().lower()


def _clean_table_id(raw: str) -> str:
    """
    Cleans common LLM output variants into a catalog key candidate.
    """
    s = (raw or "").strip()
    s = s.replace("ID:", "").replace("id:", "").strip()
    s = s.strip("`").strip("'").strip('"').strip()
    # Sometimes the model returns additional text; keep the first token-like id
    s = re.split(r"\s+", s, maxsplit=1)[0].strip()
    return s


def _iter_match_terms(catalog_key: str, info: dict):
    """
    Yields normalized terms that, if present in the user's question, imply this table.
    """
    yield _normalize(catalog_key)

    fqn = info.get("tabela_fqn") or ""
    if fqn:
        yield _normalize(fqn)

    router_hints = info.get("router_hints") or {}
    sinonimos = list(router_hints.get("sinonimos") or [])
    sinonimos.extend(info.get("sinonimos") or [])
    for s in sinonimos:
        yield _normalize(s)


def _direct_match(pergunta_usuario: str):
    """
    Deterministic routing without calling the LLM.
    - If user mentions the catalog key or table FQN, route directly.
    - If user mentions a known synonym, route directly.
    """
    q = _normalize(pergunta_usuario)
    if not q:
        return None

    chosen = None
    for key, info in CATALOGO.items():
        for term in _iter_match_terms(key, info):
            if term and term in q:
                chosen = info
                break
        if chosen:
            break

    return chosen


def _build_catalog_options_text() -> str:
    """
    Builds a compact catalog description list for the router prompt.
    """
    lines = []
    for key, info in CATALOGO.items():
        descricao = info.get("descricao", "")
        fqn = info.get("tabela_fqn", "")

        router_hints = info.get("router_hints") or {}
        sinonimos = router_hints.get("sinonimos") or info.get("sinonimos") or []
        tags = router_hints.get("tags") or info.get("tags") or []

        extras = []
        if fqn:
            extras.append(f"FQN: {fqn}")
        if sinonimos:
            extras.append(f"Sinônimos: {', '.join(sinonimos[:10])}")
        if tags:
            extras.append(f"Tags: {', '.join(tags[:10])}")

        extras_txt = f" | {' | '.join(extras)}" if extras else ""
        lines.append(f"- ID: {key} | Descrição: {descricao}{extras_txt}")

    return "\n".join(lines) + ("\n" if lines else "")


def _lookup_catalog_by_id(candidate_id: str):
    """
    Resolves a candidate id string into a catalog entry (best-effort).
    """
    cleaned = _clean_table_id(candidate_id)
    cleaned_norm = _normalize(cleaned)

    chosen = None

    if cleaned_norm and cleaned_norm not in ("none", "null"):
        if cleaned in CATALOGO:
            chosen = CATALOGO[cleaned]
        else:
            for key, info in CATALOGO.items():
                key_norm = _normalize(key)
                if key_norm == cleaned_norm:
                    chosen = info
                    break
                if cleaned_norm and (cleaned_norm in key_norm or key_norm in cleaned_norm):
                    chosen = info
                    break

    return chosen, cleaned


def _identify_with_llm(pergunta_usuario: str):
    """
    Uses the LLM router to choose a catalog entry.
    """
    from databricks_langchain import ChatDatabricks

    llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)
    options_text = _build_catalog_options_text()

    prompt = ChatPromptTemplate.from_template(ROUTER_TEMPLATE)
    chain = prompt | llm | StrOutputParser()

    try:
        raw = chain.invoke({"pergunta": pergunta_usuario, "opcoes": options_text}).strip()
        chosen, cleaned = _lookup_catalog_by_id(raw)
        logger.info(f"Roteador escolheu: {cleaned}")

        if not chosen:
            logger.warning(f"Tabela sugerida '{cleaned}' não existe no catálogo.")

        return chosen
    except Exception as e:
        logger.error(f"Erro no Router: {e}")
        return None


def identify_table(pergunta_usuario):
    """Returns the dictionary for the chosen table or None"""

    # Fast-path: direct routing when the user already named the table/key.
    chosen = _direct_match(pergunta_usuario)
    if chosen:
        logger.info("Roteamento direto (sem LLM) aplicado.")
    else:
        chosen = _identify_with_llm(pergunta_usuario)

    return chosen

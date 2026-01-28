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


def identify_table(pergunta_usuario):
    """Returns the dictionary for the chosen table or None"""

    # Fast-path: direct routing when the user already named the table/key.
    direct = _direct_match(pergunta_usuario)
    if direct:
        logger.info("Roteamento direto (sem LLM) aplicado.")
        return direct

    from databricks_langchain import ChatDatabricks

    llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)

    lista_texto = ""
    for k, v in CATALOGO.items():
        descricao = v.get("descricao", "")
        fqn = v.get("tabela_fqn", "")
        router_hints = v.get("router_hints") or {}
        sinonimos = router_hints.get("sinonimos") or v.get("sinonimos") or []
        tags = router_hints.get("tags") or v.get("tags") or []

        extras = []
        if fqn:
            extras.append(f"FQN: {fqn}")
        if sinonimos:
            extras.append(f"Sinônimos: {', '.join(sinonimos[:10])}")
        if tags:
            extras.append(f"Tags: {', '.join(tags[:10])}")

        extras_txt = f" | {' | '.join(extras)}" if extras else ""
        lista_texto += f"- ID: {k} | Descrição: {descricao}{extras_txt}\n"

    prompt = ChatPromptTemplate.from_template(ROUTER_TEMPLATE)
    chain = prompt | llm | StrOutputParser()

    try:
        tabela_id = chain.invoke({
            "pergunta": pergunta_usuario,
            "opcoes": lista_texto
        }).strip()

        tabela_id = _clean_table_id(tabela_id)

        logger.info(f"Roteador escolheu: {tabela_id}")

        # Exact match
        if tabela_id in CATALOGO:
            return CATALOGO[tabela_id]

        # Case-insensitive / substring fallback
        tabela_id_norm = _normalize(tabela_id)
        if tabela_id_norm in ("none", "null", ""):
            return None

        for key, info in CATALOGO.items():
            if _normalize(key) == tabela_id_norm:
                return info
            if tabela_id_norm and tabela_id_norm in _normalize(key):
                return info
            if tabela_id_norm and _normalize(key) in tabela_id_norm:
                return info

        logger.warning(f"Tabela sugerida '{tabela_id}' não existe no catálogo.")
        return None

    except Exception as e:
        logger.error(f"Erro no Router: {e}")
        return None

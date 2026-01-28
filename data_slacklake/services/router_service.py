"""
Router service designed to classify user intents.
"""
# pylint: disable=import-outside-toplevel
import os
import re

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from data_slacklake.catalog.loader import load_catalog
from data_slacklake.catalog.live_catalog import build_live_table_context
from data_slacklake.config import LLM_ENDPOINT, logger
from data_slacklake.prompts import ROUTER_TEMPLATE


def _router_max_options() -> int:
    try:
        return int(os.getenv("ROUTER_MAX_OPTIONS", "40"))
    except Exception:
        return 40


def _truncate(text: str, max_len: int) -> str:
    text = text or ""
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


_STOPWORDS_PT = {
    "a", "o", "os", "as", "um", "uma", "uns", "umas",
    "de", "da", "do", "das", "dos", "em", "no", "na", "nos", "nas",
    "para", "por", "com", "sem", "sobre", "entre",
    "e", "ou", "que", "qual", "quais", "como", "quando", "quanto", "quantos", "quanta", "quantas",
    "me", "minha", "meu", "seu", "sua", "seus", "suas", "dele", "dela",
    "é", "ser", "está", "estao", "estão", "foi", "são",
    "dos", "das", "ao", "à", "às",
}


def _tokens(text: str) -> set[str]:
    parts = re.split(r"[^a-z0-9_]+", (text or "").lower())
    return {p for p in parts if p and p not in _STOPWORDS_PT and len(p) >= 2}


def _score_entry(pergunta: str, table_id: str, entry: dict) -> int:
    q = _tokens(pergunta)
    if not q:
        return 0

    score = 0
    desc = str(entry.get("descricao") or "")
    tags = entry.get("tags") or []
    sinonimos = entry.get("sinonimos") or []

    hay = " ".join([table_id, desc, " ".join(map(str, tags)), " ".join(map(str, sinonimos))]).lower()

    # bônus por match direto (substring) de termos longos
    for tok in q:
        if tok in hay:
            score += 10

    # bônus extra se token aparece em tags/sinônimos explicitamente
    tags_set = {str(t).lower() for t in tags}
    sin_set = {str(s).lower() for s in sinonimos}
    for tok in q:
        if tok in tags_set:
            score += 15
        if tok in sin_set:
            score += 20

    # leve boost por match no id da tabela
    if any(tok in table_id.lower() for tok in q):
        score += 10

    return score


def _build_router_options_text(pergunta: str, catalogo: dict) -> str:
    """
    Reduz o prompt: ranqueia e envia só top-N opções.
    """
    max_opts = _router_max_options()
    scored = []
    for k, v in catalogo.items():
        s = _score_entry(pergunta, k, v)
        scored.append((s, k, v))
    scored.sort(key=lambda x: x[0], reverse=True)

    top = scored[:max_opts]
    # Se tudo score 0, ainda assim manda um pequeno subset determinístico
    if top and top[0][0] == 0:
        top = scored[: min(max_opts, 30)]

    lines = []
    for _s, k, v in top:
        desc = _truncate(str(v.get("descricao", "")), 220)
        tags = v.get("tags") or []
        sinonimos = v.get("sinonimos") or []
        extra = ""
        if tags:
            extra += f" | Tags: {', '.join(map(str, tags[:10]))}"
        if sinonimos:
            extra += f" | Sinônimos: {', '.join(map(str, sinonimos[:8]))}"
        lines.append(f"- ID: {k} | Descrição: {desc}{extra}")
    return "\n".join(lines) + ("\n" if lines else "")


def identify_table(pergunta_usuario):
    """Returns the dictionary for the chosen table or None"""

    from databricks_langchain import ChatDatabricks

    llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)

    catalogo = load_catalog()
    lista_texto = _build_router_options_text(pergunta_usuario, catalogo)

    prompt = ChatPromptTemplate.from_template(ROUTER_TEMPLATE)
    chain = prompt | llm | StrOutputParser()

    try:
        tabela_id = chain.invoke({
            "pergunta": pergunta_usuario,
            "opcoes": lista_texto
        }).strip()

        tabela_id = tabela_id.replace("ID:", "").strip()

        logger.info(f"Roteador escolheu: {tabela_id}")

        if tabela_id in catalogo:
            return catalogo[tabela_id]

        logger.warning(f"Tabela sugerida '{tabela_id}' não existe no catálogo.")
        # fallback: busca "ao vivo" no datalake (sem gerar catálogo inteiro em runtime)
        live = build_live_table_context(pergunta=pergunta_usuario, llm=llm, router_prompt_template=ROUTER_TEMPLATE)
        if live:
            return live
        return None

    except Exception as e:
        logger.error(f"Erro no Router: {e}")
        # em erro no router principal, ainda podemos tentar fallback ao vivo
        try:
            live = build_live_table_context(pergunta=pergunta_usuario, llm=llm, router_prompt_template=ROUTER_TEMPLATE)
            if live:
                return live
        except Exception:
            pass
        return None

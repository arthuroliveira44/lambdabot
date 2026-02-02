"""
Router service designed to classify user intents.
"""
# pylint: disable=import-outside-toplevel
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from data_slacklake.catalog.definitions import CATALOGO
from data_slacklake.config import LLM_ENDPOINT, logger
from data_slacklake.prompts import ROUTER_TEMPLATE
from data_slacklake.services.vector_search_service import retrieve_top_k_catalog

def identify_table(pergunta_usuario):
    """Returns the dictionary for the chosen table or None"""

    from databricks_langchain import ChatDatabricks

    llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)

    # 1) Recuperação Top-K (Vector Search) para não enviar o catálogo inteiro ao LLM.
    candidates = retrieve_top_k_catalog(pergunta_usuario, k=5, columns=["id"])
    candidate_ids = [c.get("id") for c in candidates if c.get("id")]

    # Fallback (sem Vector Search): usa todas as opções, mas ainda com descrição curta.
    if candidate_ids:
        opcoes = []
        for cid in candidate_ids:
            if cid in CATALOGO:
                v = CATALOGO[cid]
                opcoes.append(f"- ID: {cid} | Descrição: {v.get('descricao_curta') or v.get('descricao')}")
        lista_texto = "\n".join(opcoes).strip()
    else:
        lista_texto = ""
        for k, v in CATALOGO.items():
            lista_texto += f"- ID: {k} | Descrição: {v.get('descricao_curta') or v.get('descricao')}\n"

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

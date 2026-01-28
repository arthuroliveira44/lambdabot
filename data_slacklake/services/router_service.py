"""
Router service designed to classify user intents.
"""
# pylint: disable=import-outside-toplevel
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from data_slacklake.catalog.loader import load_catalog
from data_slacklake.config import LLM_ENDPOINT, logger
from data_slacklake.prompts import ROUTER_TEMPLATE

def identify_table(pergunta_usuario):
    """Returns the dictionary for the chosen table or None"""

    from databricks_langchain import ChatDatabricks

    llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)

    catalogo = load_catalog()
    lista_texto = ""
    for k, v in catalogo.items():
        desc = v.get("descricao", "")
        tags = v.get("tags") or []
        sinonimos = v.get("sinonimos") or []
        extra = ""
        if tags:
            extra += f" | Tags: {', '.join(tags)}"
        if sinonimos:
            extra += f" | Sinônimos: {', '.join(sinonimos[:12])}"
        lista_texto += f"- ID: {k} | Descrição: {desc}{extra}\n"

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
        return None

    except Exception as e:
        logger.error(f"Erro no Router: {e}")
        return None

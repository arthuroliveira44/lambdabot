"""
Service responsible for orchestrating AI calls and Natural Language Processing (NLP) workflows.
"""
from functools import lru_cache

from data_slacklake.config import LLM_ENDPOINT
from data_slacklake.prompts import INTERPRET_TEMPLATE, SQL_GEN_TEMPLATE
from data_slacklake.services.db_service import execute_query
from data_slacklake.services.router_service import identify_table


@lru_cache(maxsize=16)
def get_llm():
    """
    Lazy Loader: Only imports and connects to Databricks when it's actually going to be used.
    Uses lru_cache to guarantee the Singleton pattern (connects only once).
    """
    # pylint: disable=import-outside-toplevel
    from databricks_langchain import ChatDatabricks

    return ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)


def process_question(pergunta):
    """Fluxo: Router -> SQL -> DB -> Resposta"""

    # pylint: disable=import-outside-toplevel
    from langchain_core.output_parsers import StrOutputParser
    from langchain_core.prompts import ChatPromptTemplate

    llm = get_llm()

    tabela_info = identify_table(pergunta)
    if not tabela_info:
        return "Desculpe, não encontrei uma tabela no meu catálogo que responda isso.", None

    prompt_sql = ChatPromptTemplate.from_template(SQL_GEN_TEMPLATE)
    chain_sql = prompt_sql | llm | StrOutputParser()

    sql_query = chain_sql.invoke({
        "contexto_tabela": tabela_info['contexto'],
        "pergunta": pergunta
    })

    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

    try:
        colunas, dados = execute_query(sql_query)
    except Exception as e:
        return f"Erro ao executar a query: {str(e)}", sql_query

    prompt_interpret = ChatPromptTemplate.from_template(INTERPRET_TEMPLATE)
    chain_interpret = prompt_interpret | llm | StrOutputParser()

    resposta_final = chain_interpret.invoke({
        "pergunta": pergunta,
        "colunas": colunas,
        "dados": dados
    })

    return resposta_final, sql_query

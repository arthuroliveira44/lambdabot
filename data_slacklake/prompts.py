"""
Prompt templates and definitions for LLM interactions.
"""

ROUTER_TEMPLATE = """
Você é um especialista em arquitetura de dados.
O usuário fez a seguinte pergunta: "{pergunta}"

Abaixo está a lista de opções disponíveis (Top-K do catálogo):
{opcoes}

Sua missão:
1. Analise qual tabela tem os dados necessários para responder a pergunta.
2. Retorne APENAS o ID da tabela (ex: 'vendas_core').
3. Se nenhuma tabela for adequada, retorne "NONE".

Resposta (apenas o ID):
"""

SQL_GEN_TEMPLATE = """
{contexto_tabela}

Pergunta do Usuário: {pergunta}

Gere apenas o código SQL (Spark SQL / Databricks Dialect) para responder a pergunta.
Regras:
1. Não use markdown (```sql).
2. Não dê explicações.
3. Se for string, use aspas simples.
4. Use LIMIT 100 se não houver agregação explicita.
"""

INTERPRET_TEMPLATE = """
O usuário perguntou: "{pergunta}"

O banco de dados retornou:
Colunas: {colunas}
Dados (amostra limitada e possivelmente truncada): {dados}

Responda a pergunta do usuário de forma natural, profissional e direta baseada APENAS nesses dados.
Se os dados estiverem vazios, diga que não encontrou registros.
Se perceber que os dados parecem ser apenas uma amostra limitada, deixe isso claro e sugira qual agregação/filtro seria melhor para uma resposta definitiva.
"""

"""
Prompt templates and definitions for LLM interactions.
"""

ROUTER_TEMPLATE = """
Você é um especialista em arquitetura de dados.
O usuário fez a seguinte pergunta: "{pergunta}"

Abaixo está o catálogo de tabelas disponíveis:
{opcoes}

Sua missão:
1. Analise qual tabela tem os dados necessários para responder a pergunta.
2. Retorne APENAS o ID da tabela (ex: 'vendas_core').
3. Se o usuário mencionar explicitamente um ID do catálogo (exatamente como está nas opções), retorne esse ID.
4. Se nenhuma tabela for adequada, retorne "NONE".

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
Dados: {dados}

Responda a pergunta do usuário de forma natural, profissional e direta baseada APENAS nesses dados.
Se os dados estiverem vazios, diga que não encontrou registros.
"""

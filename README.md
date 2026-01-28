# lambdabot

## Gerar contextos (catálogo) a partir do schema do datalake

O projeto usa um catálogo (`CATALOGO`) com `descricao` (para roteamento) e `contexto` (para geração de SQL).
Para escalar isso para muitas tabelas, você pode gerar automaticamente um catálogo a partir do
`system.information_schema` do Databricks.

Exemplo (gera um JSON no formato do `CATALOGO`):

```bash
python -m data_slacklake.catalog.generate_contexts \
  --table-catalog dev \
  --table-schema diamond \
  --table-like "mart_%" \
  --output data_slacklake/catalog/generated_catalog.json
```

Para gerar um contexto mais “eficiente” usando LLM (com validação para não inventar colunas/tabelas):

```bash
python -m data_slacklake.catalog.generate_contexts \
  --table-catalog dev \
  --table-schema diamond \
  --table-like "mart_%" \
  --use-llm \
  --llm-endpoint "databricks-gpt-5-2" \
  --output data_slacklake/catalog/generated_catalog.json
```

Opções úteis:
- `--table-regex "^mart_.*_core$"`: filtro adicional com regex.
- `--id-prefix "diamond_"`: prefixo para os IDs no catálogo.
- `--stdout`: imprime o JSON no terminal.

## Rodar no Databricks Notebook

Se você prefere rodar dentro do Databricks (usando `spark.sql`), use o notebook em `notebooks/generate_catalog_contexts.py`
(formato “Databricks notebook source”). Basta importar para um Databricks Repo/Workspace e executar as células,
ajustando `TABLE_CATALOG`, `TABLE_SCHEMA`, `TABLE_LIKE` e `OUTPUT_DBFS_PATH`.

Observação: o notebook usa `SHOW TABLES` + `DESCRIBE` para garantir que todas as tabelas visíveis no schema sejam incluídas,
mesmo quando o `system.information_schema` não listar 100% dos objetos.

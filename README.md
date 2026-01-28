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

Opções úteis:
- `--table-regex "^mart_.*_core$"`: filtro adicional com regex.
- `--id-prefix "diamond_"`: prefixo para os IDs no catálogo.
- `--stdout`: imprime o JSON no terminal.

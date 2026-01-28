# Databricks notebook source
# MAGIC %md
# MAGIC ## Gerar catálogo de contextos a partir do `information_schema` (com opção de LLM)
# MAGIC
# MAGIC Este notebook:
# MAGIC - Lê metadados reais do Databricks via `system.information_schema.tables/columns`
# MAGIC - Gera um JSON no formato do `CATALOGO` (id -> `{descricao, contexto, tags, sinonimos}`)
# MAGIC - Opcionalmente enriquece com **LLM** (com validações para não inventar colunas/tabelas) e fallback determinístico

# COMMAND ----------
import json
import re
from typing import Any, Dict, List, Optional

# COMMAND ----------
# MAGIC %md
# MAGIC ### Parâmetros

# COMMAND ----------
TABLE_CATALOG = "dev"          # ex: dev
TABLE_SCHEMA = "diamond"       # ex: diamond
TABLE_LIKE = "mart_%"          # ex: mart_%
TABLE_REGEX = None             # ex: r"^mart_.*_core$"
ID_PREFIX = None               # ex: "diamond_"

USE_LLM = False
LLM_ENDPOINT = "databricks-gpt-5-2"
LLM_TEMPERATURE = 0.0

OUTPUT_DBFS_PATH = "dbfs:/tmp/generated_catalog.json"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Helpers: leitura do schema via Spark

# COMMAND ----------
def sql_escape_literal(value: str) -> str:
    return value.replace("'", "''")


def fetch_tables(table_catalog: str, table_schema: str, table_like: str) -> List[Dict[str, Any]]:
    q = f"""
    SELECT table_catalog, table_schema, table_name, comment
    FROM system.information_schema.tables
    WHERE table_catalog = '{sql_escape_literal(table_catalog)}'
      AND table_schema  = '{sql_escape_literal(table_schema)}'
      AND table_type    = 'BASE TABLE'
      AND table_name LIKE '{sql_escape_literal(table_like)}'
    ORDER BY table_name
    """.strip()
    return [r.asDict(recursive=True) for r in spark.sql(q).collect()]


def fetch_columns(table_catalog: str, table_schema: str) -> List[Dict[str, Any]]:
    q = f"""
    SELECT table_name, column_name, data_type, comment, ordinal_position
    FROM system.information_schema.columns
    WHERE table_catalog = '{sql_escape_literal(table_catalog)}'
      AND table_schema  = '{sql_escape_literal(table_schema)}'
    ORDER BY table_name, ordinal_position
    """.strip()
    return [r.asDict(recursive=True) for r in spark.sql(q).collect()]


def build_fqn(table_catalog: str, table_schema: str, table_name: str) -> str:
    return f"{table_catalog}.{table_schema}.{table_name}"


def build_context_deterministic(fqn: str, table_comment: Optional[str], columns: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    lines.append(f"Você é um analista de dados. Tabela: `{fqn}`")
    lines.append("")
    if table_comment:
        lines.append(f"Descrição da tabela: {str(table_comment).strip()}")
        lines.append("")

    lines.append("Colunas:")
    if not columns:
        lines.append("- (sem colunas encontradas no information_schema)")
    else:
        for c in columns:
            cn = c["column_name"]
            dt = c.get("data_type")
            cm = c.get("comment")
            type_part = f" ({dt})" if dt else ""
            comment_part = f": {str(cm).strip()}" if cm else ""
            lines.append(f"- {cn}{type_part}{comment_part}.")

    lines.append("")
    lines.append("Regras:")
    lines.append("1. Prefira selecionar apenas as colunas necessárias (evite SELECT *).")
    lines.append("2. Use filtros por período quando aplicável (ex.: datas/partições).")
    lines.append("3. Se não houver agregação explícita, use LIMIT 100.")
    lines.append("4. Ao agregar, confira o grão para evitar duplicação (JOINs podem multiplicar linhas).")
    return "\n".join(lines).strip() + "\n"


def build_description(table_comment: Optional[str], fqn: str) -> str:
    if table_comment and str(table_comment).strip():
        return str(table_comment).strip()
    return f"Tabela `{fqn}`."

# COMMAND ----------
# MAGIC %md
# MAGIC ### Helpers: LLM (2 opções)
# MAGIC
# MAGIC 1) `databricks_langchain.ChatDatabricks` (se o cluster tiver lib instalada)  
# MAGIC 2) chamada REST no Model Serving (`/api/2.0/serving-endpoints/<endpoint>/invocations`)

# COMMAND ----------
def build_llm_prompt(fqn: str, table_comment: Optional[str], columns: List[Dict[str, Any]]) -> str:
    col_lines = []
    for c in columns:
        cn = c["column_name"]
        dt = c.get("data_type")
        cm = c.get("comment")
        type_part = f" ({dt})" if dt else ""
        comment_part = f" - {str(cm).strip()}" if cm else ""
        col_lines.append(f"- {cn}{type_part}{comment_part}")

    tc = str(table_comment).strip() if table_comment else ""
    cols_text = "\n".join(col_lines) if col_lines else "- (sem colunas no information_schema)"

    return f"""
Você é um especialista em modelagem de dados e geração de contexto para SQL (Spark SQL / Databricks).
Você receberá APENAS metadados reais (schema). Não invente colunas e não invente tabelas.

Tabela (FQN): {fqn}
Comentário da tabela: {tc}

Colunas reais (nome, tipo, comentário quando existir):
{cols_text}

Tarefa:
Gere um JSON estrito (apenas JSON, sem markdown, sem texto extra) com o seguinte schema:
{{
  "descricao": "string curta (1 linha) para ajudar o roteador a escolher a tabela",
  "contexto": "texto em PT-BR com: 1) a frase inicial 'Você é um analista de dados. Tabela: `<FQN>`' 2) uma seção 'Colunas:' listando SOMENTE colunas reais 3) uma seção 'Regras:' com orientações práticas",
  "tags": ["opcional", "strings curtas"],
  "sinonimos": ["opcional", "termos de negócio relevantes"]
}}

Regras obrigatórias:
- Em `contexto`, cite a tabela exatamente como `{fqn}` dentro de crases: `{fqn}`.
- Na seção `Colunas:`, liste apenas colunas que estão na lista fornecida.
- Não cite nomes de outras tabelas.
- Responda APENAS com JSON válido.
""".strip()


def call_llm_langchain(endpoint: str, prompt: str, temperature: float) -> str:
    from databricks_langchain import ChatDatabricks  # type: ignore
    llm = ChatDatabricks(endpoint=endpoint, temperature=temperature)
    resp = llm.invoke(prompt)
    return getattr(resp, "content", resp) if not isinstance(resp, str) else resp


def call_llm_serving_rest(endpoint: str, prompt: str, temperature: float) -> str:
    import requests

    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = ctx.apiUrl().get()               # ex: https://adb-xxxx.azuredatabricks.net
    token = ctx.apiToken().get()            # token do usuário/cluster

    url = f"{host}/api/2.0/serving-endpoints/{endpoint}/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # maioria dos endpoints “chat” do Databricks aceita esse formato
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
    }

    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=120)
    r.raise_for_status()
    data = r.json()

    # tenta extrair no padrão OpenAI-like
    try:
        return data["choices"][0]["message"]["content"]
    except Exception:
        # fallback: devolve o JSON inteiro como string
        return json.dumps(data, ensure_ascii=False)


def llm_generate_entry(prompt: str, endpoint: str, temperature: float) -> Dict[str, Any]:
    # tenta via langchain; se não tiver instalado, cai pro REST
    try:
        text = call_llm_langchain(endpoint=endpoint, prompt=prompt, temperature=temperature)
    except Exception:
        text = call_llm_serving_rest(endpoint=endpoint, prompt=prompt, temperature=temperature)

    entry = json.loads(text)
    if not isinstance(entry, dict):
        raise ValueError("LLM retornou algo que não é um JSON objeto.")
    return entry


def validate_llm_entry(entry: Dict[str, Any], fqn: str, allowed_columns: List[str]) -> None:
    if "descricao" not in entry or not str(entry["descricao"]).strip():
        raise ValueError("LLM sem campo 'descricao'.")
    if "contexto" not in entry or not str(entry["contexto"]).strip():
        raise ValueError("LLM sem campo 'contexto'.")

    contexto = str(entry["contexto"])

    expected = f"`{fqn}`"
    if expected not in contexto:
        raise ValueError(f"Contexto da LLM não contém a tabela esperada: {expected}")

    # não permitir outras referências em crases
    for m in re.finditer(r"`([^`]+)`", contexto):
        if m.group(1) != fqn:
            raise ValueError(f"LLM citou outra referência em crases: `{m.group(1)}`")

    allowed = set(allowed_columns)
    if not allowed:
        return

    in_cols = False
    seen = set()
    for line in contexto.splitlines():
        s = line.strip()
        if s == "Colunas:":
            in_cols = True
            continue
        if s == "Regras:":
            in_cols = False
            continue
        if in_cols and s.startswith("- "):
            token = s[2:].strip()
            token = re.split(r"[\s(:]", token, maxsplit=1)[0].strip()
            if token and token not in allowed:
                raise ValueError(f"LLM inventou coluna '{token}'.")
            if token:
                seen.add(token)
    if not seen:
        raise ValueError("LLM não listou colunas na seção 'Colunas:'.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Geração do catálogo

# COMMAND ----------
tables = fetch_tables(TABLE_CATALOG, TABLE_SCHEMA, TABLE_LIKE)
cols_all = fetch_columns(TABLE_CATALOG, TABLE_SCHEMA)

cols_by_table: Dict[str, List[Dict[str, Any]]] = {}
for c in cols_all:
    cols_by_table.setdefault(c["table_name"], []).append(c)

regex_compiled = re.compile(TABLE_REGEX) if TABLE_REGEX else None

catalog: Dict[str, Dict[str, Any]] = {}
for t in tables:
    table_name = t["table_name"]
    if regex_compiled and not regex_compiled.search(table_name):
        continue

    fqn = build_fqn(t["table_catalog"], t["table_schema"], table_name)
    table_id = f"{ID_PREFIX}{table_name}" if ID_PREFIX else table_name

    columns = cols_by_table.get(table_name, [])
    allowed_cols = [c["column_name"] for c in columns]

    entry: Dict[str, Any] = {
        "descricao": build_description(t.get("comment"), fqn),
        "contexto": build_context_deterministic(fqn, t.get("comment"), columns),
        "tags": [],
        "sinonimos": [],
    }

    if USE_LLM:
        try:
            prompt = build_llm_prompt(fqn, t.get("comment"), columns)
            llm_entry = llm_generate_entry(prompt, endpoint=LLM_ENDPOINT, temperature=LLM_TEMPERATURE)
            validate_llm_entry(llm_entry, fqn=fqn, allowed_columns=allowed_cols)
            entry["descricao"] = str(llm_entry["descricao"]).strip()
            entry["contexto"] = str(llm_entry["contexto"]).strip() + "\n"
            entry["tags"] = llm_entry.get("tags", []) or []
            entry["sinonimos"] = llm_entry.get("sinonimos", []) or []
        except Exception as e:
            entry["tags"] = ["fallback_schema_only"]
            entry["contexto"] = entry["contexto"] + "\n" + f"(Aviso: fallback sem LLM. Motivo: {str(e)})\n"

    catalog[table_id] = entry

print(f"OK: {len(catalog)} contextos gerados.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Salvar em DBFS

# COMMAND ----------
payload = json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
dbutils.fs.put(OUTPUT_DBFS_PATH, payload, overwrite=True)
print(f"Salvo em: {OUTPUT_DBFS_PATH}")


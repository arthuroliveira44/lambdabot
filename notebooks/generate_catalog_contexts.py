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
import fnmatch
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
STRICT_LLM_ENDPOINT_CHECK = True  # se True, valida se o endpoint existe no workspace antes de rodar

OUTPUT_DBFS_PATH = "dbfs:/tmp/generated_catalog.json"

# COMMAND ----------
# MAGIC %md
# MAGIC ### Helpers: leitura do schema via Spark
# MAGIC
# MAGIC **Importante:** Em alguns workspaces, `system.information_schema` pode não listar 100% das tabelas visíveis em
# MAGIC `SHOW TABLES` (por diferenças de metastore/UC/permissões). Para garantir que você gere para todas as tabelas
# MAGIC do schema, este notebook usa `SHOW TABLES` + `DESCRIBE` para metadados.

# COMMAND ----------
def sql_escape_literal(value: str) -> str:
    return value.replace("'", "''")


def _like_to_glob(like_pattern: str) -> str:
    # SQL LIKE: % (qualquer), _ (1 char)  -> glob: * e ?
    return like_pattern.replace("%", "*").replace("_", "?")


def fetch_tables(table_catalog: str, table_schema: str, table_like: str) -> List[Dict[str, Any]]:
    schema_fqn = f"{table_catalog}.{table_schema}"
    rows = [r.asDict(recursive=True) for r in spark.sql(f"SHOW TABLES IN {schema_fqn}").collect()]

    glob_pat = _like_to_glob(table_like)
    result: List[Dict[str, Any]] = []
    for r in rows:
        # `SHOW TABLES` retorna: database, tableName, isTemporary
        # Em alguns ambientes, o Databricks cria views temporárias internas (ex.: `_sqldf`).
        # Elas podem aparecer no SHOW TABLES e não existem no metastore -> ignore.
        if bool(r.get("isTemporary")):
            continue
        table_name = r.get("tableName") or r.get("tableName".lower()) or r.get("table_name") or r.get("tablename")
        if not table_name:
            continue
        table_name = str(table_name)
        if table_name.startswith("_"):
            continue
        if not fnmatch.fnmatchcase(str(table_name), glob_pat):
            continue
        result.append(
            {
                "table_catalog": table_catalog,
                "table_schema": table_schema,
                "table_name": table_name,
                "comment": None,
            }
        )

    result.sort(key=lambda x: x["table_name"])
    return result


def fetch_table_comment(fqn: str) -> Optional[str]:
    # `DESCRIBE DETAIL` costuma trazer `description`/`comment` quando existe
    try:
        d = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0].asDict(recursive=True)
        for k in ("description", "comment"):
            v = d.get(k)
            if v and str(v).strip():
                return str(v).strip()
    except Exception:
        pass
    return None


def fetch_columns_for_table(fqn: str) -> List[Dict[str, Any]]:
    # `DESCRIBE <table>` retorna col_name, data_type, comment (e também linhas de metadados que começam com '#')
    try:
        rows = [r.asDict(recursive=True) for r in spark.sql(f"DESCRIBE {fqn}").collect()]
    except Exception:
        return []
    cols: List[Dict[str, Any]] = []
    ordinal = 1
    for r in rows:
        col_name = r.get("col_name") or r.get("col_name".upper())
        data_type = r.get("data_type") or r.get("data_type".upper())
        comment = r.get("comment") or r.get("comment".upper())

        if not col_name:
            continue
        col_name = str(col_name).strip()
        if not col_name or col_name.startswith("#"):
            continue

        cols.append(
            {
                "column_name": col_name,
                "data_type": (str(data_type).strip() if data_type else None),
                "comment": (str(comment).strip() if comment else None),
                "ordinal_position": ordinal,
            }
        )
        ordinal += 1
    return cols


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

    # Heurísticas leves para deixar o contexto mais “eficiente” mesmo sem LLM
    col_names = [str(c.get("column_name", "")).lower() for c in columns]
    time_cols = [n for n in col_names if any(k in n for k in ("date", "day", "week", "month", "year"))]
    if time_cols:
        lines.append("5. Identifique a coluna de tempo correta (ex.: date/week/month) e evite misturar granularidades.")

    # Para tabelas com saldo/amount: evitar somar snapshots sem critério
    if any("balance" in n for n in col_names):
        lines.append(
            "6. Colunas de saldo (ex.: balance/current_balance) normalmente são snapshots: "
            "não some ao longo do tempo sem definir o recorte (ex.: último dia por cliente/moeda)."
        )
    if any("amount" in n for n in col_names):
        lines.append("7. Para valores monetários (amount), defina moeda (`currency`) e período antes de agregar.")
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


def call_llm_system_ai_sql(model: str, prompt: str, temperature: float) -> str:
    """
    Usa Foundation Models via `system.ai` (SQL), que é o caminho correto quando você tem EXECUTE no `system.ai`
    mas não consegue chamar `/serving-endpoints/.../invocations`.

    Observação: nem todos os workspaces suportam parâmetros (ex.: temperature) via SQL; por isso ignoramos
    temperature aqui e usamos prompt determinístico (temperature 0 no comportamento desejado).
    """
    m = sql_escape_literal(model)
    p = sql_escape_literal(prompt)

    # Tentamos variações comuns para compatibilidade
    candidates = [
        f"SELECT system.ai.ai_query('{m}', '{p}') AS content",
        f"SELECT ai_query('{m}', '{p}') AS content",
    ]
    last_err: Exception | None = None
    for q in candidates:
        try:
            row = spark.sql(q).collect()[0]
            content = row["content"]
            return content if isinstance(content, str) else str(content)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Falha ao invocar via system.ai ai_query. Último erro: {last_err}")


def call_llm_serving_rest(endpoint: str, prompt: str, temperature: float) -> str:
    import requests

    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    # `apiUrl()` pode retornar o "control plane" (ex: região) em alguns ambientes.
    # Para chamar endpoints do workspace, use `browserHostName()`.
    host = f"https://{ctx.browserHostName().get()}"
    token = ctx.apiToken().get()            # token do usuário/cluster

    url = f"{host}/api/2.0/serving-endpoints/{endpoint}/invocations"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # maioria dos endpoints “chat” do Databricks aceita esse formato
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
    }

    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=120)
    if r.status_code == 404:
        # Se o endpoint aparece na listagem mas 404 no invocations, é tipicamente permissão (Can Query)
        try:
            eps = list_serving_endpoints()
            if endpoint in eps:
                raise PermissionError(
                    "Endpoint existe no workspace, mas a invocação retornou 404. "
                    "Isso geralmente indica falta de permissão para consultar o endpoint (Can Query) "
                    "ou política de ocultação de recurso. "
                    f"Endpoint: {endpoint}"
                )
        except Exception:
            # Se falhar ao listar, segue com erro original
            pass
    r.raise_for_status()
    data = r.json()

    # tenta extrair no padrão OpenAI-like
    try:
        return data["choices"][0]["message"]["content"]
    except Exception:
        # fallback: devolve o JSON inteiro como string
        return json.dumps(data, ensure_ascii=False)


def llm_generate_entry(prompt: str, endpoint: str, temperature: float) -> Dict[str, Any]:
    # tenta via langchain; se não tiver instalado/configurado, tenta system.ai; por fim tenta REST do Serving
    try:
        text = call_llm_langchain(endpoint=endpoint, prompt=prompt, temperature=temperature)
    except Exception:
        try:
            text = call_llm_system_ai_sql(model=endpoint, prompt=prompt, temperature=temperature)
        except Exception:
            text = call_llm_serving_rest(endpoint=endpoint, prompt=prompt, temperature=temperature)

    # Alguns endpoints retornam envelope; tentamos extrair texto, se necessário
    try:
        entry = json.loads(text)
    except Exception:
        raise ValueError(f"LLM não retornou JSON puro. Retorno: {text[:3000]}")
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


def list_serving_endpoints() -> List[str]:
    """
    Lista endpoints de Model Serving no workspace (para você escolher um `LLM_ENDPOINT` válido).
    """
    import requests

    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    host = f"https://{ctx.browserHostName().get()}"
    token = ctx.apiToken().get()
    url = f"{host}/api/2.0/serving-endpoints"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    data = r.json()
    eps = [e.get("name") for e in data.get("endpoints", []) if e.get("name")]
    return sorted(set(eps))


def test_llm_invocation(endpoint: str) -> None:
    """
    Teste rápido (falha cedo) para confirmar se você consegue invocar o endpoint.
    """
    prompt = """
Retorne APENAS o JSON: {"ok": true}
""".strip()
    # tenta primeiro via system.ai (mais comum em workspaces sem invocations liberado)
    try:
        _ = call_llm_system_ai_sql(model=endpoint, prompt=prompt, temperature=0.0)
        return
    except Exception:
        _ = call_llm_serving_rest(endpoint=endpoint, prompt=prompt, temperature=0.0)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Geração do catálogo

# COMMAND ----------
tables = fetch_tables(TABLE_CATALOG, TABLE_SCHEMA, TABLE_LIKE)

regex_compiled = re.compile(TABLE_REGEX) if TABLE_REGEX else None

if USE_LLM and STRICT_LLM_ENDPOINT_CHECK:
    try:
        available = list_serving_endpoints()
        if LLM_ENDPOINT not in available:
            raise ValueError(
                "LLM_ENDPOINT não encontrado em Model Serving. "
                f"Defina LLM_ENDPOINT para um destes: {available}"
            )
        # garante que você consegue invocar (permite falhar cedo com mensagem clara)
        test_llm_invocation(LLM_ENDPOINT)
    except Exception as e:
        raise RuntimeError(
            "Não foi possível validar `LLM_ENDPOINT` via API de serving. "
            "Se você não usa Model Serving, defina `USE_LLM=False` ou ajuste o endpoint. "
            f"Erro: {str(e)}"
        )

catalog: Dict[str, Dict[str, Any]] = {}
for t in tables:
    table_name = t["table_name"]
    if regex_compiled and not regex_compiled.search(table_name):
        continue

    fqn = build_fqn(t["table_catalog"], t["table_schema"], table_name)
    table_id = f"{ID_PREFIX}{table_name}" if ID_PREFIX else table_name

    table_comment = fetch_table_comment(fqn) or t.get("comment")
    columns = fetch_columns_for_table(fqn)
    allowed_cols = [c["column_name"] for c in columns]

    entry: Dict[str, Any] = {
        "descricao": build_description(table_comment, fqn),
        "contexto": build_context_deterministic(fqn, table_comment, columns),
        "tags": [],
        "sinonimos": [],
        "llm_status": "disabled" if not USE_LLM else "pending",
        "llm_error": None,
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
            entry["llm_status"] = "ok"
        except Exception as e:
            entry["tags"] = ["fallback_schema_only"]
            entry["llm_status"] = "fallback"
            entry["llm_error"] = str(e)

    catalog[table_id] = entry

print(f"OK: {len(catalog)} contextos gerados.")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Salvar em DBFS

# COMMAND ----------
payload = json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
dbutils.fs.put(OUTPUT_DBFS_PATH, payload, overwrite=True)
print(f"Salvo em: {OUTPUT_DBFS_PATH}")


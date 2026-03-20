# AGENTS.md

## Cursor Cloud specific instructions

### Overview
This is a **Slack chatbot** (`data_slacklake`) that integrates with **Databricks Genie** to answer data questions. It runs as two AWS Lambda functions (ingress + worker) and is written in Python 3.12 with Poetry for dependency management. Codebase comments and messages are in Brazilian Portuguese.

### Development commands
- **Install dependencies:** `python3 -m poetry install`
- **Lint:** `python3 -m poetry run pylint data_slacklake tests`
- **Test:** `python3 -m poetry run pytest -v`

### Known pre-existing test failures
5 tests fail due to test-vs-code mismatches (tests assert behaviors the production code does not implement). These are **not** environment issues:
- `test_url_verification_requer_assinatura_valida` — expects signature validation before URL verification, but code handles `url_verification` first.
- `test_ingress_enfileira_message_im_no_worker` — expects DM (`message.im`) forwarding, but `_is_app_mention_event` only matches `app_mention`.
- `test_handler_retorna_400_quando_json_do_body_e_invalido` — expects 400 for malformed JSON, but `_parse_json_body` returns `None` and handler returns 200.
- `test_handler_retorna_400_quando_body_nao_e_string` — expects 400 for dict body, but `json.loads` raises `TypeError` (unhandled).
- `test_worker_retorna_500_quando_falha` — expects body `"Internal Server Error"`, but code returns `str(exc)`.

### Gotchas
- `poetry` is invoked as `python3 -m poetry` (not bare `poetry`) because pip-installed Poetry may not be on `PATH`.
- Config module (`data_slacklake/config.py`) calls AWS SSM at import time. Tests mock `boto3.client` globally in `conftest.py` before any app imports.
- This is a serverless app (AWS Lambda); there is no local dev server or `docker-compose`. Local validation is done entirely through `pytest` with mocked AWS/Slack/Databricks dependencies.

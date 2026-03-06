# AGENTS.md

## Cursor Cloud specific instructions

This is a Python 3.12 project using **Poetry** as the package manager. It implements a Slack bot deployed as AWS Lambda functions that bridges Slack with Databricks Genie.

### Project structure

- `main.py` — Lambda Ingress handler (receives Slack webhooks)
- `worker.py` — Lambda Worker handler (processes questions via Genie)
- `data_slacklake/` — Core package (config, services)
- `tests/` — Unit tests with mocked AWS/Slack/Databricks calls

### Running common dev tasks

| Task | Command |
|------|---------|
| Install deps | `python3 -m poetry install` |
| Lint | `python3 -m poetry run pylint data_slacklake tests` |
| Test | `python3 -m poetry run pytest -v` |

### Non-obvious notes

- Poetry is installed via `pip install poetry` and invoked as `python3 -m poetry` (no standalone binary in PATH).
- Tests mock AWS SSM, Slack API, and Databricks Genie calls in `tests/conftest.py` — no real credentials needed to run tests.
- The application is designed for AWS Lambda deployment and cannot be started as a local server. Development validation is done via lint + tests.
- There are 5 pre-existing test failures (tests out of sync with implementation): `test_url_verification_requer_assinatura_valida`, `test_ingress_enfileira_message_im_no_worker`, `test_handler_retorna_400_quando_json_do_body_e_invalido`, `test_handler_retorna_400_quando_body_nao_e_string`, `test_worker_retorna_500_quando_falha`. These are not environment issues.

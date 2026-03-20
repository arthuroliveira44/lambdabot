# AGENTS.md

## Cursor Cloud specific instructions

### Overview

This is a Python 3.12 serverless application (AWS Lambda) that acts as a Slack bot powered by Databricks Genie. It uses **Poetry** for dependency management. There are no running services to start locally — the application is exercised entirely through `pytest` (all external dependencies are mocked in `tests/conftest.py`).

### Key commands

| Action | Command |
|--------|---------|
| Install deps | `poetry install` |
| Run tests | `poetry run pytest -v` |
| Lint | `poetry run pylint data_slacklake/ main.py worker.py` |

### Non-obvious notes

- `data_slacklake/config.py` reads AWS SSM parameters **at import time**. You cannot import `main` or `worker` modules directly without mocking `boto3.client` first (see `tests/conftest.py` for the pattern).
- The test suite has **5 pre-existing failures** (tests written for not-yet-implemented behaviour). 16 tests pass. This is the known baseline.
- Pylint score baseline is **9.89/10** with only convention/refactor warnings.
- Poetry may not be on `PATH` after `pip install poetry`; use `python3 -m poetry` or add `~/.local/bin` to `PATH`.
- There is no web server or dev server to start. The "hello world" for this app is invoking the Lambda handler with a mocked Slack `url_verification` event (see README for architecture).

# AGENTS.md

## Cursor Cloud specific instructions

### Project overview

**data_slacklake** is a Slack bot powered by Databricks Genie, deployed as two AWS Lambda functions (ingress + worker). It is a pure Python 3.12 project managed by Poetry. See `README.md` for the architecture diagram.

### Dependencies

```bash
pip install poetry
python3 -m poetry install
```

Poetry may not be on `PATH` after pip install; use `python3 -m poetry` to invoke it.

### Lint

```bash
python3 -m poetry run pylint data_slacklake tests
```

Configuration is in `.pylintrc`.

### Tests

```bash
python3 -m poetry run pytest -v
```

All external services (AWS SSM, Slack, Databricks Genie) are fully mocked in `tests/conftest.py`. No credentials or network access are needed to run the test suite.

**Known pre-existing failures (5 of 21 tests):** Several tests expect features not yet implemented in `main.py` and `worker.py` (DM routing, body validation, url_verification + signature check ordering). These are not environment issues.

### Running the application

This is a serverless project (AWS Lambda). There is no local `dev server` to start. The development workflow is: install deps -> lint -> test. Deployment is handled by the Jenkins pipeline defined in `Jenkinsfile`.

### Configuration

All secrets are loaded from AWS SSM Parameter Store at import time in `data_slacklake/config.py`. The SSM prefix is `/{app_env}/data-slacklake`. The `app_env` environment variable defaults to `dev`.

### Genie Space routing

Alias-to-space mappings are stored in the SSM parameter `genie_bot_space_map` as a JSON object (e.g., `{"!remessagpt":"space-id1","!marketing":"space-id2"}`). The default space is `genie_space_id`. Routing logic is in `data_slacklake/services/ai_service.py`.

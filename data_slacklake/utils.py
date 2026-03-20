import hashlib
from data_slacklake.config import APP_ENV

_NON_PROD_ENVS = {"dev", "test"}


def _is_non_prod() -> bool:
    env = (APP_ENV or "").strip().lower()
    return env in _NON_PROD_ENVS


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _preview(text: str, max_len: int = 80) -> str:
    compact = " ".join((text or "").split())
    if len(compact) <= max_len:
        return compact
    return compact[:max_len] + "…"


def _preview_sql(query: str, max_len: int = 140) -> str:
    compact = " ".join((query or "").split())
    if len(compact) <= max_len:
        return compact
    return compact[:max_len] + "…"


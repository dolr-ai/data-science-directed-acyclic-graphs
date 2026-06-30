import logging

import clickhouse_connect
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


def _as_bool(value, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def get_clickhouse_client():
    conn = BaseHook.get_connection("clickhouse_yral_prod")
    extra = conn.extra_dejson or {}
    secure = _as_bool(extra.get("secure"), True)
    verify = _as_bool(extra.get("verify"), False)
    default_port = 8443 if secure else 8123
    return clickhouse_connect.get_client(
        host=conn.host,
        port=int(conn.port) if conn.port else default_port,
        username=conn.login,
        password=conn.password,
        database=conn.schema or "yral",
        secure=secure,
        verify=verify,
    )


def clickhouse_command(query: str, client=None):
    _client = client or get_clickhouse_client()
    return _client.command(query)

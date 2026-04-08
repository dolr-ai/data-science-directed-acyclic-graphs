"""
ClickHouse write utility for Airflow DAGs.
Copied into each DAG folder to avoid cross-folder import assumptions.
"""

import datetime
import logging
from typing import Any, Dict, List, Optional

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
    """Create a ClickHouse client from the Airflow connection."""
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


def add_updated_at(rows: List[Dict]) -> List[Dict]:
    """Add the ReplacingMergeTree version column to each row."""
    ts = datetime.datetime.now(datetime.timezone.utc)
    for row in rows:
        row["_updated_at"] = ts
    return rows


def clickhouse_insert(table: str, data: List[Dict], client: Optional[object] = None) -> int:
    """Bulk insert a list of dicts into ClickHouse."""
    if not data:
        logger.warning("clickhouse_insert: empty data for table %s", table)
        return 0

    _client = client or get_clickhouse_client()
    columns = list(data[0].keys())
    rows = [[row.get(column) for column in columns] for row in data]

    _client.insert(
        table="yral.{table}".format(table=table),
        data=rows,
        column_names=columns,
    )
    logger.info("clickhouse_insert: inserted %s rows into yral.%s", len(rows), table)
    return len(rows)


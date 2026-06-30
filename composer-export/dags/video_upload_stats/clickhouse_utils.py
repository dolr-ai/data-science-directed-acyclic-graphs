import datetime
import logging
from typing import Dict, List, Optional

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


def clickhouse_scalar(query: str, client=None):
    _client = client or get_clickhouse_client()
    result = _client.query(query)
    if not result.result_rows:
        return None
    return result.result_rows[0][0]


def clickhouse_max_timestamp_ms(table: str, column: str, client=None, final: bool = True) -> Optional[int]:
    suffix = " FINAL" if final else ""
    value = clickhouse_scalar(
        "SELECT toUnixTimestamp64Milli(max({column})) FROM yral.{table}{suffix}".format(
            column=column,
            table=table,
            suffix=suffix,
        ),
        client=client,
    )
    return int(value) if value is not None else None


def add_updated_at(rows: List[Dict]) -> List[Dict]:
    ts = datetime.datetime.now(datetime.timezone.utc)
    for row in rows:
        row["_updated_at"] = ts
    return rows


def clickhouse_insert(table: str, data: List[Dict], client: Optional[object] = None) -> int:
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

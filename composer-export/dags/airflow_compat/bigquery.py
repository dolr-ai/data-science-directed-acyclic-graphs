"""BigQuery operator compatibility for provider versions used by Airflow 2.10.5."""

from __future__ import annotations

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


class BigQueryExecuteQueryOperator(BigQueryInsertJobOperator):
    """Minimal compatibility shim for the removed BigQueryExecuteQueryOperator.

    The exported Composer DAGs only use the legacy operator with `sql` and
    `use_legacy_sql`. Map that call shape to BigQueryInsertJobOperator while
    keeping common BigQuery query job options available if these DAGs grow later.
    """

    def __init__(
        self,
        *,
        sql,
        use_legacy_sql=True,
        destination_dataset_table=None,
        write_disposition="WRITE_EMPTY",
        create_disposition="CREATE_IF_NEEDED",
        allow_large_results=False,
        flatten_results=None,
        priority=None,
        labels=None,
        query_params=None,
        maximum_bytes_billed=None,
        time_partitioning=None,
        cluster_fields=None,
        bigquery_conn_id=None,
        **kwargs,
    ):
        query_config = {
            "query": sql,
            "useLegacySql": use_legacy_sql,
        }

        if destination_dataset_table is not None:
            query_config["destinationTable"] = destination_dataset_table
            query_config["writeDisposition"] = write_disposition
            query_config["createDisposition"] = create_disposition
        if allow_large_results:
            query_config["allowLargeResults"] = allow_large_results
        if flatten_results is not None:
            query_config["flattenResults"] = flatten_results
        if priority is not None:
            query_config["priority"] = priority
        if query_params is not None:
            query_config["queryParameters"] = query_params
        if maximum_bytes_billed is not None:
            query_config["maximumBytesBilled"] = maximum_bytes_billed
        if time_partitioning is not None:
            query_config["timePartitioning"] = time_partitioning
        if cluster_fields is not None:
            query_config["clustering"] = {"fields": cluster_fields}
        if bigquery_conn_id is not None and "gcp_conn_id" not in kwargs:
            kwargs["gcp_conn_id"] = bigquery_conn_id

        configuration = {"query": query_config}
        if labels is not None:
            configuration["labels"] = labels

        super().__init__(configuration=configuration, **kwargs)

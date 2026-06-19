from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


def main():
    """Read latest 100 events from BigQuery and write them back to a processed table."""
    spark = (
        SparkSession.builder
        .appName("AnalyticsEventsIngestion")
        # The Dataproc image already includes the Spark-BigQuery connector. Adding another
        # connector JAR duplicates classes and causes provider conflicts, so we avoid
        # specifying spark.jars.packages here.
        .getOrCreate()
    )

    # Configuration – adjust project or table names if needed
    project_id = "hot-or-not-feed-intelligence"
    # BigQuery table identifiers must use a colon between the project ID and the
    # dataset when the project is explicitly specified ("project:dataset.table").
    source_table = "yral_ds.analytics_events"
    destination_table = "yral_ds.analytics_events_processed"

    # Helper: check if a BigQuery table already exists so we can decide whether to append or create
    def _table_exists(project: str, table_fqn: str) -> bool:
        """Return True if the fully-qualified BigQuery table exists (dataset.table)."""
        try:
            from google.cloud import bigquery  # type: ignore
            from google.api_core import exceptions  # type: ignore

            dataset_id, table_id = table_fqn.split(".", 1)
            client = bigquery.Client(project=project)
            client.get_table(f"{project}.{dataset_id}.{table_id}")  # raises NotFound if absent
            return True
        except ImportError:
            # Fallback to Spark BigQuery connector if the Python client is unavailable
            try:
                spark.read.format("bigquery").option("table", f"{project}:{table_fqn}").load().limit(1)
                return True
            except Exception:
                return False
        except exceptions.NotFound:
            return False

    def _json_columns(project: str, table_fqn: str):
        """Return list of column names whose type is JSON in BigQuery schema."""
        try:
            from google.cloud import bigquery  # type: ignore
            from google.api_core import exceptions  # type: ignore

            dataset_id, table_id = table_fqn.split(".", 1)
            client = bigquery.Client(project=project)
            table_obj = client.get_table(f"{project}.{dataset_id}.{table_id}")
            return [field.name for field in table_obj.schema if field.field_type.upper() == "JSON"]
        except Exception:
            # If we cannot inspect the schema, assume no JSON columns
            return []

    # Decide whether we should append to or create the destination table
    table_already_exists = _table_exists(project_id, destination_table)
    write_mode = "append" if table_already_exists else "overwrite"

    # Identify JSON columns in the source table so we can cast them to STRING (avoids connector limitation)
    json_cols = _json_columns(project_id, source_table)

    try:
        # Read source data from BigQuery
        df = (
            spark.read.format("bigquery")
            .option("table", f"{project_id}:{source_table}")
            .load()
        )

        # Determine an ordering column. Prefer timestamp/event_timestamp if present.
        if "timestamp" in df.columns:
            order_column = "timestamp"
        elif "event_timestamp" in df.columns:
            order_column = "event_timestamp"
        else:
            # Fallback to the first column (may not guarantee recency ordering)
            order_column = df.columns[0]

        # Select the latest 100 rows based on the ordering column
        latest_df = df.orderBy(col(order_column).desc()).limit(100)

        # Cast JSON columns (if any) to STRING so the BigQuery connector can write them
        for json_col in json_cols:
            if json_col in latest_df.columns:
                latest_df = latest_df.withColumn(json_col, col(json_col).cast("string"))

        # Write processed data back to BigQuery, replacing existing data if the table is new, otherwise appending
        (
            latest_df.write.format("bigquery")
            .option("table", f"{project_id}:{destination_table}")
            .mode(write_mode)
            .save()
        )

        action = "appended to" if table_already_exists else "created/overwritten"
        print(f"Successfully {action} {destination_table} with the latest 100 events.")

    finally:
        spark.stop()


if __name__ == "__main__":
   main() 
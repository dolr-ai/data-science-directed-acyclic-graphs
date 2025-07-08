from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


def main():
    """Read latest 100 events from BigQuery and write them back to a processed table."""
    spark = (
        SparkSession.builder
        .appName("AnalyticsEventsIngestion")
        .config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0",
        )
        .getOrCreate()
    )

    # Configuration – adjust project or table names if needed
    project_id = "hot-or-not-feed-intelligence"
    source_table = "yral_ds.analytics_events"
    destination_table = "yral_ds.analytics_events_processed"

    try:
        # Read source data from BigQuery
        df = (
            spark.read.format("bigquery")
            .option("table", f"{project_id}.{source_table}")
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

        # Write processed data back to BigQuery, replacing existing data
        (
            latest_df.write.format("bigquery")
            .option("table", f"{project_id}.{destination_table}")
            .mode("overwrite")
            .save()
        )

        print("Successfully processed and wrote 100 latest events to BigQuery.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main() 
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from schemas import TELEMETRY_SCHEMA, EXPENSE_SCHEMA
from transformations import clean_telemetry, build_fleet_metrics


KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092",
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ride_hailing")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ride_admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ride_password")

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def write_to_postgres(table_name):
  
    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        row_count = batch_df.count()

        (
            batch_df.write
            .format("jdbc")
            .mode("append")
            .option("url", JDBC_URL)
            .option("dbtable", table_name)
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", 1000)
            .option("isolationLevel", "READ_COMMITTED")
            .save()
        )

        print(
            f"POSTGRES_WRITE "
            f"table={table_name} "
            f"batch_id={batch_id} "
            f"rows={row_count}"
        )

    return write_batch


def write_expenses_to_postgres(batch_df, batch_id):
 
    if batch_df.isEmpty():
        return

    expense_batch = (
        batch_df
        .select(
            "expense_id",
            "vehicle_id",
            "fuel_cost",
            "maintenance_cost",
            "distance_covered",
            "service_flag",
            "expense_date",
            "generated_at",
        )
        .dropDuplicates(
            ["vehicle_id", "expense_date"]
        )
    )

    row_count = expense_batch.count()

    if row_count == 0:
        return

    staging_table = (
        f"vehicle_expenses_stage_{batch_id}"
    )

    # Write current micro-batch to PostgreSQL staging.
    (
        expense_batch.write
        .format("jdbc")
        .mode("overwrite")
        .option("url", JDBC_URL)
        .option("dbtable", staging_table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 1000)
        .option("isolationLevel", "READ_COMMITTED")
        .save()
    )

    spark = expense_batch.sparkSession
    jvm = spark.sparkContext._jvm

    connection = None
    statement = None

    try:
        properties = jvm.java.util.Properties()

        properties.setProperty(
            "user",
            POSTGRES_USER,
        )

        properties.setProperty(
            "password",
            POSTGRES_PASSWORD,
        )

        connection = (
            jvm.java.sql.DriverManager
            .getConnection(
                JDBC_URL,
                properties,
            )
        )

        connection.setAutoCommit(False)

        statement = connection.createStatement()

        upsert_sql = f"""
            INSERT INTO vehicle_expenses (
                expense_id,
                vehicle_id,
                fuel_cost,
                maintenance_cost,
                distance_covered,
                service_flag,
                expense_date,
                generated_at
            )
            SELECT
                expense_id,
                vehicle_id,
                fuel_cost,
                maintenance_cost,
                distance_covered,
                service_flag,
                expense_date,
                generated_at
            FROM {staging_table}
            ON CONFLICT (vehicle_id, expense_date)
            DO UPDATE SET
                expense_id = EXCLUDED.expense_id,
                fuel_cost = EXCLUDED.fuel_cost,
                maintenance_cost = EXCLUDED.maintenance_cost,
                distance_covered = EXCLUDED.distance_covered,
                service_flag = EXCLUDED.service_flag,
                generated_at = EXCLUDED.generated_at
        """

        statement.executeUpdate(upsert_sql)

        connection.commit()

        print(
            f"POSTGRES_UPSERT "
            f"table=vehicle_expenses "
            f"batch_id={batch_id} "
            f"rows={row_count}"
        )

    except Exception:
        if connection is not None:
            connection.rollback()

        raise

    finally:
        if statement is not None:
            statement.close()

        if connection is not None:
            connection.close()

        # Clean up staging table.
        cleanup_connection = None
        cleanup_statement = None

        try:
            cleanup_connection = (
                jvm.java.sql.DriverManager
                .getConnection(
                    JDBC_URL,
                    properties,
                )
            )

            cleanup_statement = (
                cleanup_connection.createStatement()
            )

            cleanup_statement.executeUpdate(
                f"DROP TABLE IF EXISTS {staging_table}"
            )

        finally:
            if cleanup_statement is not None:
                cleanup_statement.close()

            if cleanup_connection is not None:
                cleanup_connection.close()


def main():
    spark = (
        SparkSession.builder
        .appName(
            "RideHailingStructuredStreaming"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    
    # STREAM 1: REAL-TIME TELEMETRY
   

    telemetry_raw = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP,
        )
        .option(
            "subscribe",
            "ride_telemetry",
        )
        .option(
            "startingOffsets",
            "earliest",
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
    )

    telemetry_json = (
        telemetry_raw
        .select(
            F.from_json(
                F.col("value").cast("string"),
                TELEMETRY_SCHEMA,
            ).alias("event")
        )
        .select("event.*")
    )

    telemetry = clean_telemetry(
        telemetry_json
    )

    telemetry_query = (
        telemetry.writeStream
        .outputMode("append")
        .foreachBatch(
            write_to_postgres(
                "telemetry_events"
            )
        )
        .option(
            "checkpointLocation",
            "/tmp/spark-checkpoints/telemetry",
        )
        .queryName(
            "telemetry_to_postgres"
        )
        .start()
    )

  
    # STREAM 2: FIVE-MINUTE FLEET METRICS


    metrics = build_fleet_metrics(
        telemetry
    )

    metrics_query = (
        metrics.writeStream
        .outputMode("append")
        .foreachBatch(
            write_to_postgres(
                "fleet_metrics"
            )
        )
        .option(
            "checkpointLocation",
            "/tmp/spark-checkpoints/fleet-metrics",
        )
        .queryName(
            "fleet_metrics_to_postgres"
        )
        .start()
    )


    # STREAM 3: DAILY VEHICLE EXPENSES

    expenses_raw = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP,
        )
        .option(
            "subscribe",
            "vehicle_expenses",
        )
        .option(
            "startingOffsets",
            "earliest",
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
    )

    expenses_json = (
        expenses_raw
        .select(
            F.from_json(
                F.col("value").cast("string"),
                EXPENSE_SCHEMA,
            ).alias("expense")
        )
        .select("expense.*")
    )

    expenses = (
        expenses_json
        .withColumn(
            "expense_date",
            F.to_date("expense_date"),
        )
        .withColumn(
            "generated_at",
            F.to_timestamp("generated_at"),
        )
        .filter(
            F.col("expense_id").isNotNull()
        )
        .filter(
            F.col("vehicle_id").isNotNull()
        )
        .filter(
            F.col("expense_date").isNotNull()
        )
        .filter(
            F.col("fuel_cost") >= 0
        )
        .filter(
            F.col("maintenance_cost") >= 0
        )
        .filter(
            F.col("distance_covered") >= 0
        )
        .select(
            "expense_id",
            "vehicle_id",
            "fuel_cost",
            "maintenance_cost",
            "distance_covered",
            "service_flag",
            "expense_date",
            "generated_at",
        )
    )

    expenses_query = (
        expenses.writeStream
        .outputMode("append")
        .foreachBatch(
            write_expenses_to_postgres
        )
        .option(
            "checkpointLocation",
            "/tmp/spark-checkpoints/expenses",
        )
        .queryName(
            "expenses_to_postgres"
        )
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
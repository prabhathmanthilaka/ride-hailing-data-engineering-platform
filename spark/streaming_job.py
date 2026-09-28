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
            f"POSTGRES_WRITE table={table_name} "
            f"batch_id={batch_id} rows={batch_df.count()}"
        )

    return write_batch


def main():
    spark = (
        SparkSession.builder
        .appName("RideHailingStructuredStreaming")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

  
    # Stream 1: real-time telemetry
   
    telemetry_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "ride_telemetry")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
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

    telemetry = clean_telemetry(telemetry_json)

    telemetry_query = (
        telemetry.writeStream
        .outputMode("append")
        .foreachBatch(write_to_postgres("telemetry_events"))
        .option(
            "checkpointLocation",
            "/tmp/spark-checkpoints/telemetry",
        )
        .queryName("telemetry_to_postgres")
        .start()
    )

  
    # Stream 2: five-minute fleet metrics
    
    metrics = build_fleet_metrics(telemetry)

    metrics_query = (
        metrics.writeStream
        .outputMode("append")
        .foreachBatch(write_to_postgres("fleet_metrics"))
        .option(
            "checkpointLocation",
            "/tmp/spark-checkpoints/fleet-metrics",
        )
        .queryName("fleet_metrics_to_postgres")
        .start()
    )

    # Stream 3: daily vehicle expenses
  
    expenses_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "vehicle_expenses")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
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
        .withColumn("expense_date", F.to_date("expense_date"))
        .withColumn("generated_at", F.to_timestamp("generated_at"))
        .filter(F.col("expense_id").isNotNull())
        .filter(F.col("vehicle_id").isNotNull())
        .filter(F.col("expense_date").isNotNull())
        .filter(F.col("fuel_cost") >= 0)
        .filter(F.col("maintenance_cost") >= 0)
        .filter(F.col("distance_covered") >= 0)
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
        .foreachBatch(write_to_postgres("vehicle_expenses"))
        .option(
            "checkpointLocation",
            "/tmp/spark-checkpoints/expenses",
        )
        .queryName("expenses_to_postgres")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
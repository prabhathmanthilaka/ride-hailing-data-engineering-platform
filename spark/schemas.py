from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


TELEMETRY_SCHEMA = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("vehicle_id", StringType(), False),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("status", StringType(), False),
    StructField("fare", DoubleType(), True),
    StructField("timestamp", StringType(), False),
])


EXPENSE_SCHEMA = StructType([
    StructField("expense_id", StringType(), False),
    StructField("vehicle_id", StringType(), False),
    StructField("fuel_cost", DoubleType(), False),
    StructField("maintenance_cost", DoubleType(), False),
    StructField("distance_covered", DoubleType(), False),
    StructField("service_flag", StringType(), False),
    StructField("expense_date", StringType(), False),
    StructField("generated_at", StringType(), True),
])
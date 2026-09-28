from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALID_STATUSES = ["idle", "enroute", "on_trip"]


def assign_zone(latitude, longitude):

    return (
        F.when(
            (latitude >= 6.9271) & (longitude >= 79.8612),
            F.lit("Colombo_Northeast"),
        )
        .when(
            (latitude >= 6.9271) & (longitude < 79.8612),
            F.lit("Colombo_Northwest"),
        )
        .when(
            (latitude < 6.9271) & (longitude >= 79.8612),
            F.lit("Colombo_Southeast"),
        )
        .otherwise(F.lit("Colombo_Southwest"))
    )


def assign_time_of_day(hour):
    return (
        F.when((hour >= 6) & (hour < 10), F.lit("morning_peak"))
        .when((hour >= 10) & (hour < 16), F.lit("midday"))
        .when((hour >= 16) & (hour < 20), F.lit("evening_peak"))
        .otherwise(F.lit("night"))
    )


def clean_telemetry(df: DataFrame) -> DataFrame:

    cleaned = (
        df
        .withColumn("event_timestamp", F.to_timestamp("timestamp"))
        .withColumn("status", F.lower(F.trim("status")))
        .withColumn("vehicle_id", F.trim("vehicle_id"))
        .withColumn("driver_id", F.trim("driver_id"))
        .withColumn("fare", F.coalesce(F.col("fare"), F.lit(0.0)))
        .withColumn("speed", F.coalesce(F.col("speed"), F.lit(0.0)))
        .filter(F.col("vehicle_id").isNotNull())
        .filter(F.length("vehicle_id") > 0)
        .filter(F.col("event_timestamp").isNotNull())
        .filter(F.col("status").isin(VALID_STATUSES))
        .filter(F.col("lat").between(-90, 90))
        .filter(F.col("lon").between(-180, 180))
        .filter(F.col("speed") >= 0)
        .filter(F.col("fare") >= 0)
        .withColumn("zone", assign_zone(F.col("lat"), F.col("lon")))
        .withColumn(
            "time_of_day",
            assign_time_of_day(F.hour("event_timestamp")),
        )
        .withColumn(
            "event_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("vehicle_id"),
                    F.col("timestamp"),
                    F.coalesce(F.col("trip_id"), F.lit("")),
                    F.col("status"),
                ),
                256,
            ),
        )
        .select(
            "event_id",
            "trip_id",
            "driver_id",
            "vehicle_id",
            F.col("lat").alias("latitude"),
            F.col("lon").alias("longitude"),
            "speed",
            "status",
            "fare",
            "zone",
            "time_of_day",
            "event_timestamp",
        )
    )

    return cleaned


def build_fleet_metrics(df: DataFrame) -> DataFrame:

    windowed = df.withWatermark("event_timestamp", "10 minutes")

    trip_earnings = (
        windowed
        .filter(F.col("trip_id").isNotNull())
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            "zone",
            "time_of_day",
            "trip_id",
        )
        .agg(
            F.max("fare").alias("trip_earnings")
        )
    )

    earnings = (
        trip_earnings
        .groupBy("window", "zone", "time_of_day")
        .agg(
            F.sum("trip_earnings").alias("earnings"),
            F.count("*").alias("trips"),
        )
    )


    vehicle_states = (
        windowed
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            "zone",
            "time_of_day",
            "vehicle_id",
        )
        .agg(
            F.last(
                "status",
                ignorenulls=True,
            ).alias("last_status")
        )
    )

    states = (
        vehicle_states
        .groupBy("window", "zone", "time_of_day")
        .agg(
            F.count("*").alias("active_vehicles"),
            F.sum(
                F.when(
                    F.col("last_status") == "idle",
                    1,
                ).otherwise(0)
            ).alias("idle_vehicles"),
            F.sum(
                F.when(
                    F.col("last_status") == "enroute",
                    1,
                ).otherwise(0)
            ).alias("enroute_vehicles"),
            F.sum(
                F.when(
                    F.col("last_status") == "on_trip",
                    1,
                ).otherwise(0)
            ).alias("on_trip_vehicles"),
        )
    )


    # Combine fleet state and earnings
  
    return (
        states
        .join(
            earnings,
            on=["window", "zone", "time_of_day"],
            how="left",
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone",
            "time_of_day",
            "active_vehicles",
            "idle_vehicles",
            "enroute_vehicles",
            "on_trip_vehicles",
            F.coalesce(
                F.col("trips"),
                F.lit(0),
            ).alias("trips"),
            F.coalesce(
                F.col("earnings"),
                F.lit(0.0),
            ).alias("earnings"),
        )
    )

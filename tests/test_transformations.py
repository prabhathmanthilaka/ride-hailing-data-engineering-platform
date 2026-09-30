"""
Tests for the Spark cleaning and aggregation logic.

These need PySpark (and Java), so they are skipped on a plain Python
install. Run them inside the Spark image - see the README.
"""

from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from schemas import TELEMETRY_SCHEMA  # noqa: E402
from transformations import build_fleet_metrics, clean_telemetry  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("ride-hailing-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def event(**overrides):
    """A valid raw telemetry event; override fields per test."""

    base = {
        "trip_id": "TRIP-1",
        "driver_id": "DRV-001",
        "vehicle_id": "VH-001",
        "lat": 6.95,
        "lon": 79.87,
        "speed": 30.0,
        "status": "on_trip",
        "fare": 4.5,
        "timestamp": "2026-10-01T08:30:00+00:00",
    }
    base.update(overrides)
    return base


def clean(spark, *events):
    frame = spark.createDataFrame(list(events), schema=TELEMETRY_SCHEMA)
    return clean_telemetry(frame).collect()


def test_valid_event_is_kept_and_enriched(spark):
    [row] = clean(spark, event())

    assert row.zone == "Colombo_Northeast"
    assert row.time_of_day == "morning_peak"
    assert row.latitude == 6.95 and row.longitude == 79.87
    assert len(row.event_id) == 64  # sha256 hex


@pytest.mark.parametrize(
    "bad",
    [
        {"status": "parked"},
        {"vehicle_id": ""},
        {"lat": 123.0},
        {"lon": -200.0},
        {"speed": -5.0},
        {"fare": -1.0},
        {"timestamp": "not-a-time"},
    ],
)
def test_invalid_events_are_dropped(spark, bad):
    assert clean(spark, event(**bad)) == []


def test_status_is_normalised_and_missing_fare_defaults_to_zero(spark):
    [row] = clean(spark, event(status="  IDLE ", fare=None, trip_id=None, speed=None))

    assert row.status == "idle"
    assert row.fare == 0.0
    assert row.speed == 0.0


@pytest.mark.parametrize(
    "lat, lon, zone",
    [
        (6.95, 79.87, "Colombo_Northeast"),
        (6.95, 79.85, "Colombo_Northwest"),
        (6.90, 79.87, "Colombo_Southeast"),
        (6.90, 79.85, "Colombo_Southwest"),
    ],
)
def test_zone_assignment(spark, lat, lon, zone):
    [row] = clean(spark, event(lat=lat, lon=lon))

    assert row.zone == zone


@pytest.mark.parametrize(
    "hour, period",
    [(5, "night"), (6, "morning_peak"), (10, "midday"), (16, "evening_peak"), (20, "night")],
)
def test_time_of_day_buckets(spark, hour, period):
    [row] = clean(spark, event(timestamp=f"2026-10-01T{hour:02d}:15:00+00:00"))

    assert row.time_of_day == period


def test_event_id_is_deterministic(spark):
    first = clean(spark, event())[0].event_id
    second = clean(spark, event())[0].event_id
    other = clean(spark, event(vehicle_id="VH-002"))[0].event_id

    assert first == second != other


def test_fleet_metrics_count_vehicles_and_final_fare_per_trip(spark):
    events = [
        # VH-001 on a trip whose cumulative fare reaches 7.0
        event(vehicle_id="VH-001", trip_id="TRIP-A", fare=2.0, timestamp="2026-10-01T08:30:00+00:00"),
        event(vehicle_id="VH-001", trip_id="TRIP-A", fare=7.0, timestamp="2026-10-01T08:31:00+00:00"),
        # VH-002 idle in the same zone and window
        event(vehicle_id="VH-002", trip_id=None, status="idle", fare=0.0, speed=0.0,
              timestamp="2026-10-01T08:32:00+00:00"),
    ]

    frame = clean_telemetry(spark.createDataFrame(events, schema=TELEMETRY_SCHEMA))
    [row] = build_fleet_metrics(frame).collect()

    assert row.window_start == datetime(2026, 10, 1, 8, 30)
    assert row.zone == "Colombo_Northeast"
    assert row.active_vehicles == 2
    assert row.idle_vehicles == 1
    assert row.on_trip_vehicles == 1
    assert row.trips == 1
    assert row.earnings == 7.0  # final fare, not 2.0 + 7.0

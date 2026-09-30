"""Tests for the simulated data sources and the shared simulation clock."""

import csv
import random
from datetime import date, datetime, timedelta, timezone

import pytest

import simulation_clock as sc
import telemetry_producer as tp
from producers import expense_batch_producer as ebp
from simulation_clock import SimulationClock


EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)
REQUIRED_EVENT_FIELDS = {
    "trip_id", "driver_id", "vehicle_id", "lat", "lon",
    "speed", "status", "fare", "timestamp",
}


# ------------------------------------------------------------------
# Simulation clock
# ------------------------------------------------------------------

def test_one_simulated_day_takes_configured_real_minutes():
    clock = SimulationClock(epoch=EPOCH)

    one_day_later = EPOCH + timedelta(minutes=sc.SIMULATED_DAY_MINUTES)

    assert clock.simulated_at(one_day_later) == (
        sc.SIMULATION_START_DATE + timedelta(days=1)
    )


def test_clock_never_runs_before_start_date():
    clock = SimulationClock(epoch=EPOCH)

    assert clock.simulated_at(EPOCH - timedelta(hours=1)) == sc.SIMULATION_START_DATE


def test_previous_date_is_yesterday():
    clock = SimulationClock(epoch=EPOCH)
    now = clock.now()

    assert clock.previous_date() == now.date() - timedelta(days=1)


def test_epoch_file_is_shared_between_processes(tmp_path, monkeypatch):
    monkeypatch.delenv("SIMULATION_EPOCH", raising=False)
    monkeypatch.setattr(sc, "EPOCH_FILE", tmp_path / "simulation_epoch.txt")

    first = sc.resolve_epoch()
    second = sc.resolve_epoch()

    assert (tmp_path / "simulation_epoch.txt").exists()
    assert first == second


def test_epoch_environment_variable_overrides_file(tmp_path, monkeypatch):
    (tmp_path / "simulation_epoch.txt").write_text("2020-01-01T00:00:00+00:00")
    monkeypatch.setattr(sc, "EPOCH_FILE", tmp_path / "simulation_epoch.txt")
    monkeypatch.setenv("SIMULATION_EPOCH", "2026-05-05T00:00:00+00:00")

    assert sc.resolve_epoch() == datetime(2026, 5, 5, tzinfo=timezone.utc)


# ------------------------------------------------------------------
# Daily expense batch
# ------------------------------------------------------------------

def test_expense_batch_has_one_record_per_vehicle():
    records = ebp.generate_expense_records(expense_date=date(2026, 9, 28))

    assert len(records) == ebp.VEHICLE_COUNT
    assert len({record["vehicle_id"] for record in records}) == ebp.VEHICLE_COUNT


def test_expense_batch_is_deterministic_per_day():
    day = date(2026, 9, 28)

    assert ebp.generate_expense_records(expense_date=day) == (
        ebp.generate_expense_records(expense_date=day)
    )
    assert ebp.generate_expense_records(expense_date=day) != (
        ebp.generate_expense_records(expense_date=day + timedelta(days=1))
    )


def test_expense_batch_defaults_to_yesterday():
    clock = SimulationClock(epoch=EPOCH)

    records = ebp.generate_expense_records(clock=clock)

    assert records[0]["expense_date"] == clock.previous_date().isoformat()


def test_expense_values_are_valid():
    for record in ebp.generate_expense_records(expense_date=date(2026, 10, 1)):
        assert record["fuel_cost"] > 0
        assert 80 <= record["distance_covered"] <= 260
        assert record["maintenance_cost"] >= 0
        assert record["service_flag"] in ("OK", "SERVICE_REQUIRED")
        # A maintenance cost always comes with a service flag.
        assert (record["maintenance_cost"] > 0) == (
            record["service_flag"] == "SERVICE_REQUIRED"
        )


def test_write_csv_drops_one_file_per_day(tmp_path, monkeypatch):
    monkeypatch.setattr(ebp, "INCOMING_DIR", tmp_path)

    records = ebp.generate_expense_records(expense_date=date(2026, 9, 28))
    path = ebp.write_csv(records)

    assert path.name == "vehicle_expenses_2026-09-28.csv"

    with path.open(newline="", encoding="utf-8") as file:
        rows = list(csv.DictReader(file))

    assert len(rows) == ebp.VEHICLE_COUNT
    assert rows[0]["expense_id"] == "EXP-2026-09-28-VH-001"


# ------------------------------------------------------------------
# Telemetry simulator
# ------------------------------------------------------------------

@pytest.fixture
def simulator():
    random.seed(42)
    return tp.FleetSimulator(vehicle_count=5)


def test_events_have_the_required_schema(simulator):
    events = simulator.generate_events()

    assert len(events) == 5

    for event in events:
        assert set(event) == REQUIRED_EVENT_FIELDS
        assert event["status"] in tp.STATUSES
        datetime.fromisoformat(event["timestamp"])


def test_trip_lifecycle_is_consistent(simulator):
    fares_by_trip = {}

    for _ in range(300):
        for event in simulator.generate_events():
            assert event["fare"] >= 0
            assert event["speed"] >= 0

            if event["status"] == "idle":
                assert event["trip_id"] is None
                assert event["speed"] == 0
            else:
                assert event["trip_id"] is not None

                # Fare is cumulative: it never decreases within a trip.
                previous = fares_by_trip.get(event["trip_id"], 0)
                assert event["fare"] >= previous
                fares_by_trip[event["trip_id"]] = event["fare"]

    assert fares_by_trip, "no trips were simulated"


def test_vehicles_stay_inside_the_operating_area(simulator):
    for _ in range(300):
        for event in simulator.generate_events():
            assert abs(event["lat"] - tp.BASE_LATITUDE) <= tp.ZONE_RADIUS + 1e-6
            assert abs(event["lon"] - tp.BASE_LONGITUDE) <= tp.ZONE_RADIUS + 1e-6


def test_extended_idle_keeps_vehicle_idle(monkeypatch):
    random.seed(7)
    monkeypatch.setattr(tp, "LONG_IDLE_PROBABILITY", 1.0)

    simulator = tp.FleetSimulator(vehicle_count=1)
    vehicle = simulator.vehicles[0]

    # Run until the first trip completes and an extended idle starts.
    for _ in range(200):
        simulator.generate_events()
        if vehicle.extended_idle > 0:
            break
    else:
        pytest.fail("no extended idle was started")

    minimum_idle_events = tp.LONG_IDLE_EVENTS[0]

    for _ in range(minimum_idle_events):
        assert simulator.generate_events()[0]["status"] == "idle"

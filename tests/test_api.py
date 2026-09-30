"""
Tests for the FastAPI serving layer.

PostgreSQL is replaced by a fake connection whose cursor returns
prepared rows, so the endpoints, response mapping, thresholds and
Prometheus metrics can be tested without a running database.
"""

from datetime import date, datetime, timezone

import pytest
from fastapi.testclient import TestClient

import main as api


NOW = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)


class FakeCursor:

    def __init__(self, respond):
        self._respond = respond
        self._rows = []

    def execute(self, query, params=None):
        self._rows = self._respond(query, params)

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:

    def __init__(self, respond):
        self._respond = respond

    def cursor(self, *args, **kwargs):
        return FakeCursor(self._respond)

    def close(self):
        pass


def use_database(monkeypatch, respond):
    """Route every get_connection() call to a fake database."""

    monkeypatch.setattr(api, "get_connection", lambda: FakeConnection(respond))


def failing_database(monkeypatch):
    def fail():
        raise RuntimeError("connection refused")

    monkeypatch.setattr(api, "get_connection", fail)


@pytest.fixture
def client():
    # No "with" block: the startup hook (a real DB connect) is not run.
    return TestClient(api.app)


# Idle rows: vehicle_id, zone, status, last_seen_at, last_active_at, idle_minutes
IDLE_ROWS = [
    ("VH-001", "Colombo_Northeast", "idle", NOW, NOW, 250.0),
    ("VH-002", "Colombo_Southwest", "idle", NOW, NOW, 90.0),
    ("VH-003", "Colombo_Northwest", "on_trip", NOW, NOW, 0),
]

# Profitability rows: vehicle_id, report_date, earnings, operating_cost,
# profit, profit_margin, service_flag, profitability_status
PROFIT_ROWS = [
    ("VH-025", date(2026, 10, 4), 64.24, 180.93, -116.69, -181.65, "SERVICE_REQUIRED", "UNPROFITABLE"),
    ("VH-015", date(2026, 10, 4), 62.39, 59.22, 3.17, 5.08, "SERVICE_REQUIRED", "AT_RISK"),
    ("VH-001", date(2026, 10, 4), 63.60, 20.21, 43.39, 68.22, "OK", "PROFITABLE"),
]


def respond_all(query, params):
    """A fake database that answers every query the /metrics refresh runs."""

    if "last_active" in query:
        return IDLE_ROWS
    if "FROM vehicle_profitability" in query and "MAX(report_date)" in query:
        return PROFIT_ROWS
    if "trip_final" in query:
        return [("Colombo_Northeast", "midday", 10, 85.5)]
    if "MAX(processed_at)" in query:
        return [(4.2, 480)]
    if "MAX(calculated_at)" in query:
        return [(120.0,)]
    if "DISTINCT ON (vehicle_id)" in query:
        return [("Colombo_Northeast", 3, 1, 1, 1)]
    return []


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------

def test_health_ok(client, monkeypatch):
    use_database(monkeypatch, lambda q, p: [(NOW,)])

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_health_reports_database_outage(client, monkeypatch):
    failing_database(monkeypatch)

    response = client.get("/health")

    assert response.status_code == 503
    assert response.json()["detail"]["database"] == "unavailable"
    assert "ride_api_database_status 0.0" in client.get("/metrics").text


# ------------------------------------------------------------------
# Business endpoints
# ------------------------------------------------------------------

def test_fleet_earnings_maps_rows(client, monkeypatch):
    use_database(monkeypatch, lambda q, p: [("Colombo_Northeast", "midday", 12, 101.456)])

    body = client.get("/fleet/earnings").json()

    assert body == [
        {"zone": "Colombo_Northeast", "time_of_day": "midday", "trips": 12, "earnings": 101.46}
    ]


def test_idle_alert_uses_default_threshold(client, monkeypatch):
    use_database(monkeypatch, lambda q, p: IDLE_ROWS)

    body = client.get("/alerts/idle-vehicles").json()

    assert body["threshold_minutes"] == api.IDLE_ALERT_MINUTES == 180
    assert [vehicle["vehicle_id"] for vehicle in body["vehicles"]] == ["VH-001"]


def test_idle_alert_threshold_can_be_overridden(client, monkeypatch):
    use_database(monkeypatch, lambda q, p: IDLE_ROWS)

    body = client.get("/alerts/idle-vehicles", params={"threshold_minutes": 60}).json()

    assert body["vehicles_idle_too_long"] == 2


def test_profitability_limit_is_validated(client):
    assert client.get("/vehicles/profitability", params={"limit": 500}).status_code == 422


def test_daily_report_summarises_latest_day(client, monkeypatch):
    use_database(monkeypatch, lambda q, p: PROFIT_ROWS)

    body = client.get("/reports/daily/latest").json()

    assert body["report_date"] == "2026-10-04"
    assert body["status_counts"] == {"PROFITABLE": 1, "AT_RISK": 1, "UNPROFITABLE": 1}
    assert body["unprofitable_vehicles"] == ["VH-025"]
    assert body["fleet_profit"] == pytest.approx(64.24 + 62.39 + 63.60 - 180.93 - 59.22 - 20.21, abs=0.01)


def test_daily_report_404_before_first_reconciliation(client, monkeypatch):
    use_database(monkeypatch, lambda q, p: [])

    assert client.get("/reports/daily/latest").status_code == 404


def test_database_error_returns_500(client, monkeypatch):
    failing_database(monkeypatch)

    assert client.get("/fleet/earnings").status_code == 500


# ------------------------------------------------------------------
# Prometheus metrics
# ------------------------------------------------------------------

def test_metrics_expose_business_and_freshness_gauges(client, monkeypatch):
    use_database(monkeypatch, respond_all)

    text = client.get("/metrics").text

    assert 'ride_vehicle_idle_minutes{vehicle_id="VH-001",zone="Colombo_Northeast"} 250.0' in text
    assert "ride_vehicles_idle_too_long 1.0" in text
    assert 'ride_vehicles_by_profitability_status{status="UNPROFITABLE"} 1.0' in text
    assert 'ride_fleet_earnings_total{time_of_day="midday",zone="Colombo_Northeast"} 85.5' in text
    assert "ride_telemetry_last_event_age_seconds 4.2" in text
    assert "ride_telemetry_events_last_minute 480.0" in text
    assert "ride_profitability_last_run_age_seconds 120.0" in text

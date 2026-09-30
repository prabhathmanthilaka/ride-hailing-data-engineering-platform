-- Cleaned streaming telemetry
CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id TEXT,
    trip_id TEXT,
    driver_id TEXT,
    vehicle_id TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    status TEXT NOT NULL,
    fare DOUBLE PRECISION DEFAULT 0,
    zone TEXT,
    time_of_day TEXT,
    event_timestamp TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_telemetry_vehicle_time
    ON telemetry_events (vehicle_id, event_timestamp);

CREATE INDEX IF NOT EXISTS idx_telemetry_zone_time
    ON telemetry_events (zone, event_timestamp);


-- Windowed fleet metrics
CREATE TABLE IF NOT EXISTS fleet_metrics (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    zone TEXT NOT NULL,
    time_of_day TEXT NOT NULL,
    active_vehicles BIGINT,
    idle_vehicles BIGINT,
    enroute_vehicles BIGINT,
    on_trip_vehicles BIGINT,
    trips BIGINT,
    earnings DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end, zone, time_of_day)
);


-- Daily vehicle expenses
CREATE TABLE IF NOT EXISTS vehicle_expenses (
    expense_id TEXT PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    fuel_cost DOUBLE PRECISION NOT NULL,
    maintenance_cost DOUBLE PRECISION NOT NULL,
    distance_covered DOUBLE PRECISION NOT NULL,
    service_flag TEXT NOT NULL,
    expense_date DATE NOT NULL,
    generated_at TIMESTAMPTZ,

    -- Required by the Spark upsert: ON CONFLICT (vehicle_id, expense_date)
    CONSTRAINT uq_vehicle_expense_day UNIQUE (vehicle_id, expense_date)
);

CREATE INDEX IF NOT EXISTS idx_expenses_vehicle_date
    ON vehicle_expenses (vehicle_id, expense_date);


-- Basic pipeline health history
CREATE TABLE IF NOT EXISTS pipeline_health (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
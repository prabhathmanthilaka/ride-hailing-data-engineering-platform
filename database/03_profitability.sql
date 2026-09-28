CREATE TABLE IF NOT EXISTS vehicle_profitability (
    vehicle_id TEXT NOT NULL,
    report_date DATE NOT NULL,

    earnings DOUBLE PRECISION NOT NULL DEFAULT 0,
    fuel_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    maintenance_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    operating_cost DOUBLE PRECISION NOT NULL DEFAULT 0,

    profit DOUBLE PRECISION NOT NULL DEFAULT 0,
    profit_margin DOUBLE PRECISION,

    distance_covered DOUBLE PRECISION NOT NULL DEFAULT 0,

    service_flag TEXT NOT NULL DEFAULT 'OK',
    profitability_status TEXT NOT NULL,

    calculated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (vehicle_id, report_date)
);

CREATE INDEX IF NOT EXISTS idx_vehicle_profitability_date
    ON vehicle_profitability (report_date);

CREATE INDEX IF NOT EXISTS idx_vehicle_profitability_status
    ON vehicle_profitability (profitability_status);

-- Ride-Hailing Data Engineering Platform
-- Initial Database Schema


CREATE TABLE IF NOT EXISTS pipeline_health (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
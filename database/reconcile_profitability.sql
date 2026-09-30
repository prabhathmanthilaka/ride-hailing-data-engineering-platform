WITH trip_revenue AS (
    SELECT
        vehicle_id,
        trip_id,
        MAX(fare) AS trip_earnings
    FROM telemetry_events
    WHERE trip_id IS NOT NULL
      AND trip_id <> ''
      AND event_timestamp >= %(report_date)s::date
      AND event_timestamp <  %(report_date)s::date + INTERVAL '1 day'
    GROUP BY
        vehicle_id,
        trip_id
),

daily_revenue AS (
    SELECT
        vehicle_id,
        SUM(trip_earnings) AS earnings
    FROM trip_revenue
    GROUP BY vehicle_id
),

daily_expenses AS (
    SELECT
        vehicle_id,
        expense_date AS report_date,
        SUM(fuel_cost) AS fuel_cost,
        SUM(maintenance_cost) AS maintenance_cost,
        SUM(distance_covered) AS distance_covered,
        CASE
            WHEN BOOL_OR(service_flag = 'SERVICE_REQUIRED')
                THEN 'SERVICE_REQUIRED'
            ELSE 'OK'
        END AS service_flag
    FROM vehicle_expenses
    WHERE expense_date = %(report_date)s::date
    GROUP BY
        vehicle_id,
        expense_date
),

reconciliation AS (
    SELECT
        e.vehicle_id,
        e.report_date,

        COALESCE(r.earnings, 0) AS earnings,

        e.fuel_cost,
        e.maintenance_cost,

        e.fuel_cost + e.maintenance_cost AS operating_cost,

        COALESCE(r.earnings, 0)
            - e.fuel_cost
            - e.maintenance_cost AS profit,

        CASE
            WHEN COALESCE(r.earnings, 0) > 0 THEN
                (
                    (
                        COALESCE(r.earnings, 0)
                        - e.fuel_cost
                        - e.maintenance_cost
                    )
                    / COALESCE(r.earnings, 0)
                ) * 100
            ELSE NULL
        END AS profit_margin,

        e.distance_covered,
        e.service_flag

    FROM daily_expenses e
    LEFT JOIN daily_revenue r
        ON r.vehicle_id = e.vehicle_id
)

INSERT INTO vehicle_profitability (
    vehicle_id,
    report_date,
    earnings,
    fuel_cost,
    maintenance_cost,
    operating_cost,
    profit,
    profit_margin,
    distance_covered,
    service_flag,
    profitability_status,
    calculated_at
)
SELECT
    vehicle_id,
    report_date,
    earnings,
    fuel_cost,
    maintenance_cost,
    operating_cost,
    profit,
    profit_margin,
    distance_covered,
    service_flag,

    CASE
        WHEN profit < 0 THEN 'UNPROFITABLE'
        WHEN profit_margin < 10 THEN 'AT_RISK'
        ELSE 'PROFITABLE'
    END AS profitability_status,

    CURRENT_TIMESTAMP

FROM reconciliation

ON CONFLICT (vehicle_id, report_date)
DO UPDATE SET
    earnings = EXCLUDED.earnings,
    fuel_cost = EXCLUDED.fuel_cost,
    maintenance_cost = EXCLUDED.maintenance_cost,
    operating_cost = EXCLUDED.operating_cost,
    profit = EXCLUDED.profit,
    profit_margin = EXCLUDED.profit_margin,
    distance_covered = EXCLUDED.distance_covered,
    service_flag = EXCLUDED.service_flag,
    profitability_status = EXCLUDED.profitability_status,
    calculated_at = CURRENT_TIMESTAMP;
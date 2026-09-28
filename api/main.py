from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query

from database import fetch_all, fetch_one
from models import (
    FleetUtilizationResponse,
    HealthResponse,
    VehicleProfitabilityResponse,
)


app = FastAPI(
    title="Ride-Hailing Fleet Operations API",
    description=(
        "Serving API for real-time fleet utilization, earnings, "
        "and daily vehicle profitability."
    ),
    version="1.0.0",
)


@app.get("/health", response_model=HealthResponse)
def health():
    try:
        result = fetch_one("SELECT 1 AS health")

        database_status = (
            "healthy" if result["health"] == 1 else "unhealthy"
        )

        return HealthResponse(
            status="healthy",
            database=database_status,
            timestamp=datetime.now(timezone.utc),
        )

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Database health check failed: {exc}",
        )


@app.get(
    "/fleet/utilization",
    response_model=list[FleetUtilizationResponse],
)
def fleet_utilization(
    limit: int = Query(default=20, ge=1, le=500),
):
    query = """
        SELECT
            window_start,
            window_end,
            zone,
            time_of_day,
            active_vehicles,
            idle_vehicles,
            enroute_vehicles,
            on_trip_vehicles,
            CASE
                WHEN active_vehicles > 0
                THEN ROUND(
                    idle_vehicles::numeric / active_vehicles,
                    4
                )
                ELSE 0
            END AS idle_ratio,
            trips,
            earnings
        FROM fleet_metrics
        ORDER BY window_start DESC, zone
        LIMIT %s;
    """

    rows = fetch_all(query, (limit,))

    return [
        FleetUtilizationResponse(**dict(row))
        for row in rows
    ]


@app.get("/fleet/earnings")
def fleet_earnings(
    limit: int = Query(default=20, ge=1, le=500),
):
    query = """
        SELECT
            window_start,
            window_end,
            zone,
            time_of_day,
            trips,
            ROUND(earnings::numeric, 2) AS earnings
        FROM fleet_metrics
        ORDER BY window_start DESC, earnings DESC
        LIMIT %s;
    """

    return fetch_all(query, (limit,))


@app.get(
    "/vehicles/profitability",
    response_model=list[VehicleProfitabilityResponse],
)
def vehicle_profitability(
    status: str | None = Query(
        default=None,
        description=(
            "Filter by PROFITABLE, AT_RISK, or UNPROFITABLE"
        ),
    ),
    limit: int = Query(default=50, ge=1, le=500),
):
    if status:
        status = status.upper()

        allowed_statuses = {
            "PROFITABLE",
            "AT_RISK",
            "UNPROFITABLE",
        }

        if status not in allowed_statuses:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid status. Use PROFITABLE, AT_RISK, "
                    "or UNPROFITABLE."
                ),
            )

        query = """
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
                profitability_status
            FROM vehicle_profitability
            WHERE profitability_status = %s
            ORDER BY profit ASC
            LIMIT %s;
        """

        rows = fetch_all(query, (status, limit))

    else:
        query = """
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
                profitability_status
            FROM vehicle_profitability
            ORDER BY profit ASC
            LIMIT %s;
        """

        rows = fetch_all(query, (limit,))

    return [
        VehicleProfitabilityResponse(**dict(row))
        for row in rows
    ]


@app.get("/alerts")
def alerts():
    query = """
        SELECT
            vehicle_id,
            report_date,
            service_flag,
            profitability_status,
            profit,
            profit_margin
        FROM vehicle_profitability
        WHERE
            service_flag = 'SERVICE_REQUIRED'
            OR profitability_status IN (
                'AT_RISK',
                'UNPROFITABLE'
            )
        ORDER BY
            CASE
                WHEN profitability_status = 'UNPROFITABLE'
                    THEN 1
                WHEN profitability_status = 'AT_RISK'
                    THEN 2
                ELSE 3
            END,
            profit ASC;
    """

    rows = fetch_all(query)

    return {
        "count": len(rows),
        "alerts": rows,
    }

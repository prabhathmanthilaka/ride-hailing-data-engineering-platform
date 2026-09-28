from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    database: str
    timestamp: datetime


class FleetUtilizationResponse(BaseModel):
    window_start: datetime
    window_end: datetime
    zone: str
    time_of_day: str
    active_vehicles: int
    idle_vehicles: int
    enroute_vehicles: int
    on_trip_vehicles: int
    idle_ratio: float
    trips: int
    earnings: float


class VehicleProfitabilityResponse(BaseModel):
    vehicle_id: str
    report_date: date
    earnings: float
    fuel_cost: float
    maintenance_cost: float
    operating_cost: float
    profit: float
    profit_margin: Optional[float]
    distance_covered: float
    service_flag: str
    profitability_status: str
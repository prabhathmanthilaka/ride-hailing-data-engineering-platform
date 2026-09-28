import json
import math
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv()


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = os.getenv(
    "KAFKA_TELEMETRY_TOPIC",
    "ride_telemetry",
)

INTERVAL_SECONDS = float(
    os.getenv(
        "TELEMETRY_INTERVAL_SECONDS",
        "3",
    )
)

SIMULATED_DAY_MINUTES = float(
    os.getenv(
        "SIMULATED_DAY_MINUTES",
        "5",
    )
)


# Colombo-centered simulation area.
BASE_LATITUDE = 6.9271
BASE_LONGITUDE = 79.8612

ZONE_RADIUS = 0.045

STATUSES = (
    "idle",
    "enroute",
    "on_trip",
)


@dataclass
class VehicleState:
    vehicle_id: str
    driver_id: str
    latitude: float
    longitude: float
    status: str
    speed: float
    fare: float
    trip_id: str | None
    trip_age: int
    idle_age: int


class FleetSimulator:
    """
    Stateful ride-hailing fleet simulator.

    Each vehicle maintains its own operational state so that
    telemetry events represent a coherent lifecycle rather than
    independent random records.
    """

    def __init__(self, vehicle_count: int = 25):
        self.vehicles: list[VehicleState] = []

        for index in range(1, vehicle_count + 1):
            self.vehicles.append(
                VehicleState(
                    vehicle_id=f"VH-{index:03d}",
                    driver_id=f"DRV-{index:03d}",
                    latitude=BASE_LATITUDE
                    + random.uniform(-ZONE_RADIUS, ZONE_RADIUS),
                    longitude=BASE_LONGITUDE
                    + random.uniform(-ZONE_RADIUS, ZONE_RADIUS),
                    status="idle",
                    speed=0.0,
                    fare=0.0,
                    trip_id=None,
                    trip_age=0,
                    idle_age=random.randint(0, 10),
                )
            )

        self.simulated_minutes = 0.0

    def _move_vehicle(self, vehicle: VehicleState) -> None:
        """
        Move the vehicle using a small random walk while keeping
        the simulated vehicle inside the operating area.
        """

        if vehicle.status == "idle":
            vehicle.speed = 0.0
            return

        if vehicle.status == "enroute":
            vehicle.speed = random.uniform(20.0, 45.0)

        elif vehicle.status == "on_trip":
            vehicle.speed = random.uniform(15.0, 55.0)

        # Approximate movement per event.
        movement = vehicle.speed * (INTERVAL_SECONDS / 3600.0)

        angle = random.uniform(0, 2 * math.pi)

        latitude_delta = movement * math.cos(angle) / 111.0
        longitude_delta = (
            movement * math.sin(angle)
            / (111.0 * math.cos(math.radians(vehicle.latitude)))
        )

        vehicle.latitude += latitude_delta
        vehicle.longitude += longitude_delta

        vehicle.latitude = max(
            BASE_LATITUDE - ZONE_RADIUS,
            min(BASE_LATITUDE + ZONE_RADIUS, vehicle.latitude),
        )

        vehicle.longitude = max(
            BASE_LONGITUDE - ZONE_RADIUS,
            min(BASE_LONGITUDE + ZONE_RADIUS, vehicle.longitude),
        )

    def _transition_vehicle(self, vehicle: VehicleState) -> None:
        """
        Advance the vehicle through a realistic operational lifecycle.
        """

        if vehicle.status == "idle":
            vehicle.idle_age += 1
            vehicle.trip_age = 0

            # Vehicles normally become available for a new trip
            # after spending some time idle.
            if vehicle.idle_age >= random.randint(2, 8):
                vehicle.status = "enroute"
                vehicle.trip_id = f"TRIP-{uuid.uuid4().hex[:10].upper()}"
                vehicle.fare = 0.0
                vehicle.trip_age = 0
                vehicle.idle_age = 0

        elif vehicle.status == "enroute":
            vehicle.trip_age += 1

            # Driver reaches pickup location.
            if vehicle.trip_age >= random.randint(1, 3):
                vehicle.status = "on_trip"

        elif vehicle.status == "on_trip":
            vehicle.trip_age += 1

            # Fare grows while the trip is active.
            vehicle.fare += random.uniform(0.8, 2.5)

            # Complete trip after a realistic number of events.
            if vehicle.trip_age >= random.randint(5, 15):
                vehicle.status = "idle"
                vehicle.trip_id = None
                vehicle.trip_age = 0
                vehicle.idle_age = 0
                vehicle.speed = 0.0

    def generate_events(self) -> list[dict]:
        """
        Advance every vehicle and produce one telemetry event
        per vehicle.
        """

        events = []

        for vehicle in self.vehicles:

            self._transition_vehicle(vehicle)
            self._move_vehicle(vehicle)

            event = {
                "trip_id": vehicle.trip_id,
                "driver_id": vehicle.driver_id,
                "vehicle_id": vehicle.vehicle_id,
                "lat": round(vehicle.latitude, 6),
                "lon": round(vehicle.longitude, 6),
                "speed": round(vehicle.speed, 2),
                "status": vehicle.status,
                "fare": round(vehicle.fare, 2),
                "timestamp": datetime.now(
                    timezone.utc
                ).isoformat(),
            }

            events.append(event)

        self.simulated_minutes += (
            INTERVAL_SECONDS
            / max(SIMULATED_DAY_MINUTES, 0.1)
            * 1440
            / 1440
        )

        return events


def create_producer() -> KafkaProducer:
    """
    Create a Kafka producer configured for JSON events.
    """

    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode(
            "utf-8"
        ),
        acks="all",
        retries=5,
        linger_ms=10,
    )


running = True


def shutdown_handler(signum, frame):
    global running
    running = False


def main():
    global running

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    print("=" * 70)
    print("Ride-Hailing Telemetry Producer")
    print("=" * 70)
    print(f"Kafka: {BOOTSTRAP_SERVERS}")
    print(f"Topic: {TOPIC}")
    print(f"Interval: {INTERVAL_SECONDS}s")
    print("Fleet size: 25 vehicles")
    print("=" * 70)

    producer = create_producer()
    simulator = FleetSimulator(vehicle_count=25)

    total_events = 0

    try:
        while running:

            events = simulator.generate_events()

            for event in events:

                future = producer.send(
                    TOPIC,
                    key=event["vehicle_id"],
                    value=event,
                )

                future.get(timeout=10)

                total_events += 1

                print(
                    f"[{event['timestamp']}] "
                    f"{event['vehicle_id']} | "
                    f"{event['status']:8} | "
                    f"speed={event['speed']:5.1f} | "
                    f"fare={event['fare']:6.2f} | "
                    f"trip={event['trip_id']}"
                )

            producer.flush()

            print(
                f"--- batch complete | "
                f"events={len(events)} | "
                f"total={total_events} ---"
            )

            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping telemetry producer...")

    finally:
        producer.flush()
        producer.close()

        print(
            f"Producer stopped. Total events: {total_events}"
        )


if __name__ == "__main__":
    main()
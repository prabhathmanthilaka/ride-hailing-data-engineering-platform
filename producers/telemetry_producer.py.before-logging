import json
import math
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass

from dotenv import load_dotenv
from kafka import KafkaProducer

from simulation_clock import (
    SIMULATED_DAY_MINUTES,
    SIMULATED_SECONDS_PER_REAL_SECOND,
    SimulationClock,
)


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


LONG_IDLE_PROBABILITY = float(
    os.getenv(
        "LONG_IDLE_PROBABILITY",
        "0.02",
    )
)

# extended idle length in producer events (20-40 events of 3 s
# real = 1-2 real minutes = ~4.8-9.6 simulated hours).
LONG_IDLE_EVENTS = (20, 40)


# Colombo-centered simulation area
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
    extended_idle: int = 0


class FleetSimulator:


    def __init__(self, vehicle_count: int = 25):
        self.vehicles: list[VehicleState] = []

        self.clock = SimulationClock()

        for index in range(1, vehicle_count + 1):
            self.vehicles.append(
                VehicleState(
                    vehicle_id=f"VH-{index:03d}",
                    driver_id=f"DRV-{index:03d}",
                    latitude=BASE_LATITUDE
                    + random.uniform(
                        -ZONE_RADIUS,
                        ZONE_RADIUS,
                    ),
                    longitude=BASE_LONGITUDE
                    + random.uniform(
                        -ZONE_RADIUS,
                        ZONE_RADIUS,
                    ),
                    status="idle",
                    speed=0.0,
                    fare=0.0,
                    trip_id=None,
                    trip_age=0,
                    idle_age=random.randint(0, 10),
                )
            )

    def simulated_timestamp(self):
    
        return self.clock.now()

    def _move_vehicle(
        self,
        vehicle: VehicleState,
    ) -> None:
   

        if vehicle.status == "idle":
            vehicle.speed = 0.0
            return

        if vehicle.status == "enroute":
            vehicle.speed = random.uniform(
                20.0,
                45.0,
            )

        elif vehicle.status == "on_trip":
            vehicle.speed = random.uniform(
                15.0,
                55.0,
            )

        simulated_hours = (
            INTERVAL_SECONDS
            * SIMULATED_SECONDS_PER_REAL_SECOND
            / 3600.0
        )

        movement = (
            vehicle.speed
            * simulated_hours
        )

        angle = random.uniform(
            0,
            2 * math.pi,
        )

        latitude_delta = (
            movement
            * math.cos(angle)
            / 111.0
        )

        longitude_delta = (
            movement
            * math.sin(angle)
            / (
                111.0
                * math.cos(
                    math.radians(
                        vehicle.latitude
                    )
                )
            )
        )

        vehicle.latitude += latitude_delta
        vehicle.longitude += longitude_delta

        # Keep vehicles inside the synthetic Colombo operating area
        vehicle.latitude = max(
            BASE_LATITUDE - ZONE_RADIUS,
            min(
                BASE_LATITUDE + ZONE_RADIUS,
                vehicle.latitude,
            ),
        )

        vehicle.longitude = max(
            BASE_LONGITUDE - ZONE_RADIUS,
            min(
                BASE_LONGITUDE + ZONE_RADIUS,
                vehicle.longitude,
            ),
        )

    def _transition_vehicle(
        self,
        vehicle: VehicleState,
    ) -> None:
    

        if vehicle.status == "idle":
            vehicle.idle_age += 1
            vehicle.trip_age = 0

            if vehicle.extended_idle > 0:
                vehicle.extended_idle -= 1
                return

            if vehicle.idle_age >= random.randint(2, 8):
                vehicle.status = "enroute"

                vehicle.trip_id = (
                    f"TRIP-{uuid.uuid4().hex[:10].upper()}"
                )

                vehicle.fare = 0.0
                vehicle.trip_age = 0
                vehicle.idle_age = 0

        elif vehicle.status == "enroute":
            vehicle.trip_age += 1

            # Driver reaches pickup location
            if vehicle.trip_age >= random.randint(1, 3):
                vehicle.status = "on_trip"

        elif vehicle.status == "on_trip":
            vehicle.trip_age += 1

            # Fare grows while the trip is active.
            vehicle.fare += random.uniform(
                0.8,
                2.5,
            )

            # Complete trip after a realistic number of events
            if vehicle.trip_age >= random.randint(5, 15):
                vehicle.status = "idle"
                vehicle.trip_id = None
                vehicle.trip_age = 0
                vehicle.idle_age = 0
                vehicle.speed = 0.0

                # occasionally the driver goes on an extended idle
                if random.random() < LONG_IDLE_PROBABILITY:
                    vehicle.extended_idle = random.randint(
                        *LONG_IDLE_EVENTS
                    )

                    print(
                        f"LONG_IDLE_START "
                        f"vehicle={vehicle.vehicle_id} "
                        f"events={vehicle.extended_idle}"
                    )

    def generate_events(self) -> list[dict]:


        events = []

        event_timestamp = self.simulated_timestamp()

        for vehicle in self.vehicles:

            self._transition_vehicle(vehicle)

            self._move_vehicle(vehicle)

            event = {
                "trip_id": vehicle.trip_id,
                "driver_id": vehicle.driver_id,
                "vehicle_id": vehicle.vehicle_id,
                "lat": round(
                    vehicle.latitude,
                    6,
                ),
                "lon": round(
                    vehicle.longitude,
                    6,
                ),
                "speed": round(
                    vehicle.speed,
                    2,
                ),
                "status": vehicle.status,
                "fare": round(
                    vehicle.fare,
                    2,
                ),
                "timestamp": event_timestamp.isoformat(),
            }

            events.append(event)

        return events


def create_producer() -> KafkaProducer:


    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode(
            "utf-8"
        ),
        value_serializer=lambda value: json.dumps(
            value
        ).encode(
            "utf-8"
        ),
        acks="all",
        retries=5,
        linger_ms=10,
    )


running = True


def shutdown_handler(
    signum,
    frame,
):
    global running
    running = False


def main():
    global running

    signal.signal(
        signal.SIGINT,
        shutdown_handler,
    )

    signal.signal(
        signal.SIGTERM,
        shutdown_handler,
    )

    print("=" * 70)
    print("Ride-Hailing Telemetry Producer")
    print("=" * 70)

    print(
        f"Kafka: {BOOTSTRAP_SERVERS}"
    )

    print(
        f"Topic: {TOPIC}"
    )

    print(
        f"Real interval: {INTERVAL_SECONDS}s"
    )

    print(
        f"Simulated day: "
        f"{SIMULATED_DAY_MINUTES} real minutes"
    )

    print(
        f"Simulation speed: "
        f"{SIMULATED_SECONDS_PER_REAL_SECOND:.2f}x"
    )

    print(
        "Fleet size: 25 vehicles"
    )

    # NEW
    print(
        f"Long idle probability: "
        f"{LONG_IDLE_PROBABILITY:.0%} per completed trip"
    )

    print("=" * 70)

    producer = create_producer()

    simulator = FleetSimulator(
        vehicle_count=25
    )

    total_events = 0

    try:

        while running:

            events = (
                simulator.generate_events()
            )

            for event in events:

                future = producer.send(
                    TOPIC,
                    key=event[
                        "vehicle_id"
                    ],
                    value=event,
                )

                future.get(
                    timeout=10
                )

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

            time.sleep(
                INTERVAL_SECONDS
            )

    except KeyboardInterrupt:

        print(
            "\nStopping telemetry producer..."
        )

    finally:

        producer.flush()
        producer.close()

        print(
            f"Producer stopped. "
            f"Total events: {total_events}"
        )


if __name__ == "__main__":
    main()
import csv
import json
import os
import random
import uuid
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

from simulation_clock import SimulationClock


load_dotenv()


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = os.getenv(
    "KAFKA_EXPENSE_TOPIC",
    "vehicle_expenses",
)

PROJECT_ROOT = (
    Path(__file__).resolve().parent.parent
)

INCOMING_DIR = (
    PROJECT_ROOT
    / "data"
    / "incoming"
)

CSV_FILE = (
    INCOMING_DIR
    / "vehicle_expenses.csv"
)

VEHICLE_COUNT = 25


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


def generate_expense_records() -> list[dict]:


    clock = SimulationClock()

    simulated_now = clock.now()

    simulated_date = clock.date()

    records = []

    for index in range(
        1,
        VEHICLE_COUNT + 1,
    ):

        vehicle_id = (
            f"VH-{index:03d}"
        )

        distance = round(
            random.uniform(
                80.0,
                260.0,
            ),
            2,
        )

        fuel_cost = round(
            distance
            * random.uniform(
                0.11,
                0.18,
            ),
            2,
        )


        has_maintenance = (
            random.random() < 0.20
        )

        maintenance_cost = (
            round(
                random.uniform(
                    25.0,
                    150.0,
                ),
                2,
            )
            if has_maintenance
            else 0.0
        )

        service_flag = (
            "SERVICE_REQUIRED"
            if has_maintenance
            else "OK"
        )

        record = {
            "expense_id": (
                f"EXP-"
                f"{uuid.uuid4().hex[:10].upper()}"
            ),
            "vehicle_id": vehicle_id,
            "fuel_cost": fuel_cost,
            "maintenance_cost": maintenance_cost,
            "distance_covered": distance,
            "service_flag": service_flag,
            "expense_date": (
                simulated_date.isoformat()
            ),
            "generated_at": (
                simulated_now.isoformat()
            ),
        }

        records.append(record)

    return records


def write_csv(
    records: list[dict],
) -> None:


    INCOMING_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    fieldnames = [
        "expense_id",
        "vehicle_id",
        "fuel_cost",
        "maintenance_cost",
        "distance_covered",
        "service_flag",
        "expense_date",
        "generated_at",
    ]

    with CSV_FILE.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
        )

        writer.writeheader()

        writer.writerows(
            records
        )


def publish_to_kafka(
    producer: KafkaProducer,
    records: list[dict],
) -> None:


    for record in records:

        producer.send(
            TOPIC,
            key=record[
                "vehicle_id"
            ],
            value=record,
        )

    producer.flush()


def main():

    print("=" * 70)
    print(
        "Ride-Hailing Daily Expense Batch Producer"
    )
    print("=" * 70)

    print(
        f"Kafka: {BOOTSTRAP_SERVERS}"
    )

    print(
        f"Topic: {TOPIC}"
    )

    print(
        f"CSV:   {CSV_FILE}"
    )

    print(
        f"Vehicles: {VEHICLE_COUNT}"
    )

    clock = SimulationClock()

    print(
        f"Simulated date: "
        f"{clock.date().isoformat()}"
    )

    print(
        f"Simulated timestamp: "
        f"{clock.now().isoformat()}"
    )

    print("=" * 70)

    records = (
        generate_expense_records()
    )

    write_csv(records)

    print(
        f"Generated {len(records)} "
        f"daily expense records."
    )

    producer = create_producer()

    try:

        publish_to_kafka(
            producer,
            records,
        )

        print(
            f"Published {len(records)} "
            f"records to Kafka topic "
            f"'{TOPIC}'."
        )

    finally:

        producer.close()

    print(
        f"CSV written to: {CSV_FILE}"
    )


if __name__ == "__main__":
    main()
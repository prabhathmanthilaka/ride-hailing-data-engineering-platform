import csv
import json
import os
import random
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

from producers.log_utils import log_event
from producers.simulation_clock import SimulationClock


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

VEHICLE_COUNT = 25


def csv_path_for(expense_date: date) -> Path:
    """One file is dropped per simulated day."""

    return (
        INCOMING_DIR
        / f"vehicle_expenses_{expense_date.isoformat()}.csv"
    )


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


def generate_expense_records(
    clock: SimulationClock | None = None,
    expense_date: date | None = None,
) -> list[dict]:

    if expense_date is None:

        if clock is None:
            clock = SimulationClock()

        expense_date = clock.previous_date()

    records = []

    for index in range(
        1,
        VEHICLE_COUNT + 1,
    ):

        vehicle_id = (
            f"VH-{index:03d}"
        )

        seed = (
            f"{expense_date.isoformat()}"
            f":{vehicle_id}"
        )

        rng = random.Random(seed)

        expense_id = (
            f"EXP-"
            f"{expense_date.isoformat()}-"
            f"{vehicle_id}"
        )


        distance = round(
            rng.uniform(
                80.0,
                260.0,
            ),
            2,
        )


        fuel_cost = round(
            distance
            * rng.uniform(
                0.11,
                0.18,
            ),
            2,
        )

        # Approximately 20% of vehicles receive a
        # maintenance event.

        has_maintenance = (
            rng.random() < 0.20
        )

        maintenance_cost = (
            round(
                rng.uniform(
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
            "expense_id": expense_id,
            "vehicle_id": vehicle_id,
            "fuel_cost": fuel_cost,
            "maintenance_cost": maintenance_cost,
            "distance_covered": distance,
            "service_flag": service_flag,
            "expense_date": (
                expense_date.isoformat()
            ),
            # Partners submit the file at the end of the reported day.
            "generated_at": (
                f"{expense_date.isoformat()}T23:59:59+00:00"
            ),
        }

        records.append(record)

    return records


def write_csv(
    records: list[dict],
) -> Path:

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

    csv_file = csv_path_for(
        date.fromisoformat(
            records[0]["expense_date"]
        )
    )

    with csv_file.open(
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

    return csv_file


def publish_to_kafka(
    producer: KafkaProducer,
    records: list[dict],
) -> None:

    for record in records:

        producer.send(
            TOPIC,
            key=record["vehicle_id"],
            value=record,
        )

    producer.flush()


def main():

    service = "expense-producer"

    clock = SimulationClock()

    records = generate_expense_records(
        clock=clock
    )

    log_event(
        service,
        "ingestion",
        "expense_batch_generated",
        kafka=BOOTSTRAP_SERVERS,
        topic=TOPIC,
        simulated_now=clock.now().isoformat(),
        expense_date=records[0]["expense_date"],
        records=len(records),
    )

    csv_file = write_csv(
        records
    )

    log_event(
        service,
        "ingestion",
        "expense_file_written",
        path=str(csv_file),
        records=len(records),
    )

    producer = create_producer()

    try:

        publish_to_kafka(
            producer,
            records,
        )

        log_event(
            service,
            "ingestion",
            "expenses_published",
            topic=TOPIC,
            expense_date=records[0]["expense_date"],
            records=len(records),
        )

    finally:

        producer.close()


if __name__ == "__main__":
    main()
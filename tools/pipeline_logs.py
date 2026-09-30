"""
Unified pipeline log viewer.

Merges the structured JSON logs of every pipeline component into one
time-ordered view, so a failure can be traced across stages:

    ingestion      telemetry-producer   logs/telemetry-producer.log
    ingestion ..   expense-pipeline     logs/expense-pipeline.log (Airflow)
    processing     spark-streaming      docker logs ride-spark
    storage        spark-streaming      docker logs ride-spark
    serving        ride-api             docker logs ride-api

Examples:
    python tools/pipeline_logs.py                      # last 5 minutes
    python tools/pipeline_logs.py --since 15 --level ERROR
    python tools/pipeline_logs.py --stage storage --tail 20
    python tools/pipeline_logs.py --service spark-streaming --event query_progress
"""

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

LOG_FILES = [
    PROJECT_ROOT / "logs" / "telemetry-producer.log",
    PROJECT_ROOT / "logs" / "expense-pipeline.log",
    PROJECT_ROOT / "logs" / "expense-producer.log",
]

CONTAINERS = ["ride-spark", "ride-api"]

# Fields already shown in their own columns.
BASE_FIELDS = {"ts", "level", "service", "stage", "event"}

LEVEL_ORDER = {"INFO": 0, "WARNING": 1, "ERROR": 2}


def parse_records(lines):
    for line in lines:
        line = line.strip()

        if not line.startswith("{"):
            continue

        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        if "ts" in record and "event" in record:
            yield record


def read_log_files():
    for path in LOG_FILES:
        if path.exists():
            with path.open(encoding="utf-8", errors="replace") as file:
                yield from parse_records(file)


def read_container_logs(since_minutes):
    for container in CONTAINERS:
        try:
            result = subprocess.run(
                ["docker", "logs", container, "--since", f"{since_minutes}m"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=60,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            print(f"[warning] could not read docker logs for {container}: {exc}")
            continue

        # Structured records are written to stdout only.
        yield from parse_records(result.stdout.splitlines())


def to_datetime(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def details(record, width):
    extra = {
        key: value
        for key, value in record.items()
        if key not in BASE_FIELDS and value is not None
    }

    text = " ".join(
        f"{key}={json.dumps(value, default=str) if isinstance(value, (dict, list)) else value}"
        for key, value in extra.items()
    )

    return text if len(text) <= width else text[: width - 3] + "..."


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--since", type=int, default=5, help="minutes to look back (default 5)")
    parser.add_argument("--level", choices=["INFO", "WARNING", "ERROR"], help="minimum level")
    parser.add_argument("--service", help="e.g. spark-streaming, ride-api, telemetry-producer")
    parser.add_argument("--stage", help="ingestion / processing / storage / orchestration / serving")
    parser.add_argument("--event", help="exact event name, e.g. postgres_write_failed")
    parser.add_argument("--tail", type=int, default=40, help="show only the last N records (default 40)")
    parser.add_argument("--width", type=int, default=110, help="max width of the details column")
    args = parser.parse_args()

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=args.since)

    records = []

    for record in list(read_log_files()) + list(read_container_logs(args.since)):
        try:
            if to_datetime(record["ts"]) < cutoff:
                continue
        except ValueError:
            continue

        if args.level and LEVEL_ORDER.get(record.get("level"), 0) < LEVEL_ORDER[args.level]:
            continue
        if args.service and record.get("service") != args.service:
            continue
        if args.stage and record.get("stage") != args.stage:
            continue
        if args.event and record.get("event") != args.event:
            continue

        records.append(record)

    records.sort(key=lambda record: record["ts"])
    records = records[-args.tail:]

    print(f"{'time (UTC)':12} {'level':7} {'service':19} {'stage':13} {'event':30} details")
    print("-" * (12 + 7 + 19 + 13 + 30 + 5 + 20))

    for record in records:
        print(
            f"{record['ts'][11:23]:12} "
            f"{record.get('level', ''):7} "
            f"{record.get('service', ''):19} "
            f"{record.get('stage', ''):13} "
            f"{record['event']:30} "
            f"{details(record, args.width)}"
        )

    print(f"\n{len(records)} record(s), last {args.since} min")


if __name__ == "__main__":
    main()

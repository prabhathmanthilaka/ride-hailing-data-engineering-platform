import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


LOG_DIR = Path(
    os.getenv(
        "LOG_DIR",
        str(Path(__file__).resolve().parent.parent / "logs"),
    )
)


def log_event(
    service: str,
    stage: str,
    event: str,
    level: str = "INFO",
    **fields,
) -> dict:
    """Write one structured log record and return it."""

    record = {
        "ts": datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z"),
        "level": level,
        "service": service,
        "stage": stage,
        "event": event,
        **fields,
    }

    line = json.dumps(record, default=str)

    print(line, file=sys.stdout, flush=True)

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)

        with (LOG_DIR / f"{service}.log").open(
            "a",
            encoding="utf-8",
        ) as file:
            file.write(line + "\n")

    except OSError:
        # File logging is best effort; stdout already has the record.
        pass

    return record

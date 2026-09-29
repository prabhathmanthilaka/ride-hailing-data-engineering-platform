import os
import time
from datetime import datetime, timedelta, timezone


def _parse_start_date(value: str) -> datetime:

    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


SIMULATION_START_DATE = _parse_start_date(
    os.getenv(
        "SIMULATION_START_DATE",
        "2026-09-28T00:00:00+00:00",
    )
)

SIMULATED_DAY_MINUTES = float(
    os.getenv("SIMULATED_DAY_MINUTES", "5")
)

if SIMULATED_DAY_MINUTES <= 0:
    raise ValueError("SIMULATED_DAY_MINUTES must be greater than 0")


# Number of simulated seconds represented by one real second
SIMULATED_SECONDS_PER_REAL_SECOND = (
    24 * 60 * 60
) / (SIMULATED_DAY_MINUTES * 60)


class SimulationClock:

    def __init__(self):
        self._real_start = time.monotonic()

    def now(self) -> datetime:
    
        elapsed_real_seconds = time.monotonic() - self._real_start

        elapsed_simulated_seconds = (
            elapsed_real_seconds
            * SIMULATED_SECONDS_PER_REAL_SECOND
        )


        return SIMULATION_START_DATE + timedelta(
            seconds=elapsed_simulated_seconds
        )

    def date(self):
        return self.now().date()

    def speed(self) -> float:
        return SIMULATED_SECONDS_PER_REAL_SECOND
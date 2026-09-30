import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

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

SIMULATED_SECONDS_PER_REAL_SECOND = (
    24 * 60 * 60
) / (SIMULATED_DAY_MINUTES * 60)


EPOCH_FILE = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "processed"
    / "simulation_epoch.txt"
)


def resolve_epoch() -> datetime:

    env_value = os.getenv("SIMULATION_EPOCH")

    if env_value:
        return _parse_start_date(env_value)

    if EPOCH_FILE.exists():
        content = EPOCH_FILE.read_text(encoding="utf-8").strip()

        if content:
            return _parse_start_date(content)

    epoch = datetime.now(timezone.utc)

    EPOCH_FILE.parent.mkdir(parents=True, exist_ok=True)

    try:
        
        with EPOCH_FILE.open("x", encoding="utf-8") as file:
            file.write(epoch.isoformat())

    except FileExistsError:
        return _parse_start_date(
            EPOCH_FILE.read_text(encoding="utf-8").strip()
        )

    return epoch


class SimulationClock:

    def __init__(self, epoch: datetime | None = None):
        self.epoch = epoch or resolve_epoch()

    def simulated_at(self, real_time: datetime) -> datetime:

        elapsed_real_seconds = max(
            0.0,
            (real_time - self.epoch).total_seconds(),
        )

        elapsed_simulated_seconds = (
            elapsed_real_seconds
            * SIMULATED_SECONDS_PER_REAL_SECOND
        )

        return SIMULATION_START_DATE + timedelta(
            seconds=elapsed_simulated_seconds
        )

    def now(self) -> datetime:
        return self.simulated_at(datetime.now(timezone.utc))

    def date(self):
        return self.now().date()

    def previous_date(self):
        """The most recently completed simulated day ("yesterday")."""
        return self.date() - timedelta(days=1)

    def speed(self) -> float:
        return SIMULATED_SECONDS_PER_REAL_SECOND
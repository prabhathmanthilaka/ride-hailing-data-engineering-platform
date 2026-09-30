"""
Shared pytest setup.

- Makes the producers, API and Spark modules importable.
- Isolates the tests from the running pipeline: a fixed simulation
  epoch (so the real data/processed/simulation_epoch.txt is never read
  or created) and a temporary log folder (so logs/*.log are untouched).
"""

import os
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

os.environ["SIMULATION_EPOCH"] = "2026-01-01T00:00:00+00:00"
os.environ["LOG_DIR"] = tempfile.mkdtemp(prefix="ride-test-logs-")

for path in (ROOT, ROOT / "producers", ROOT / "api", ROOT / "spark"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

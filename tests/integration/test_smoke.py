"""P9 smoke test: the one-command demo must match the simulator's manifest exactly."""

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).parents[2]


def test_demo_seed_42_matches_its_manifest():
    run = subprocess.run(
        [sys.executable, "-m", "ccp.demo", "--seed", "42"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=400,
    )
    assert "RESULT: MATCH" in run.stdout, run.stdout[-2000:]
    assert run.returncode == 0

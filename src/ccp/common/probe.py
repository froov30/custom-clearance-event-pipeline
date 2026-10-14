"""Exec liveness probe for the consumers, which have no HTTP port (TRD §14).

    python -m ccp.common.probe <service> [max_age_seconds]

Exits 0 if <HEARTBEAT_DIR>/<service>.heartbeat was touched in the last max_age seconds (default 60).
The default must stay above the 14.7 s worst-case gap measured during a MongoDB outage (D-053),
or a pod that is correctly waiting on MongoDB gets restarted.
"""

import sys
import time
from pathlib import Path

from ccp.common.config import Config

DEFAULT_MAX_AGE_S = 60.0


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    service = args[0]
    max_age = float(args[1]) if len(args) > 1 else DEFAULT_MAX_AGE_S
    beat = Path(Config.from_env().heartbeat_dir) / f"{service}.heartbeat"
    try:
        age = time.time() - beat.stat().st_mtime
    except FileNotFoundError:
        return 1
    return 0 if age <= max_age else 1


if __name__ == "__main__":
    sys.exit(main())

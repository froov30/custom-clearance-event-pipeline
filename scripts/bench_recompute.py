"""Per-event processor cost as one shipment's history grows (limit 3, assumption A9).

Run from the repo root with the Compose stack up:  python scripts/bench_recompute.py
Uses a throwaway database and drops it. Worst case on purpose: after the declaration every event
is illegal, so the status document also carries one anomaly per event.
"""

import json
import statistics
import time
import uuid
from datetime import UTC, datetime, timedelta

from ccp.common import mongo
from ccp.common.config import Config
from ccp.processor.store import event_doc, store

SIZES = [10, 100, 1000, 5000]
SAMPLES = 7
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def raw(sid: str, i: int) -> bytes:
    first = i == 0
    return json.dumps(
        {
            "event_id": str(uuid.uuid4()),
            "shipment_id": sid,
            "container_id": "CSQU3054383",
            "event_type": "DECLARATION_FILED" if first else "RELEASED",
            "occurred_at": (T0 + timedelta(seconds=i)).isoformat(),
            "source": "bench",
            "schema_version": 1,
            "payload": {"port_code": "NLRTM", "hs_code": "847130", "declarant": "X"}
            if first
            else {},
        }
    ).encode()


def main() -> None:
    cfg = Config.from_env()
    client = mongo.client(cfg)
    name = f"ccp_bench_{uuid.uuid4().hex[:8]}"
    db = client[name]
    mongo.ensure_indexes(db)
    now = datetime.now(UTC)
    try:
        print(f"{'events in shipment':>18} | {'median ms per event':>19}")
        for n in SIZES:
            sid = f"BENCH-{n}"
            preload = [event_doc(raw(sid, i), now, ("bench", 0, i)) for i in range(n - 1)]
            db.events.insert_many(preload)
            timings = []
            for k in range(SAMPLES):
                t = time.perf_counter()
                store(db, raw(sid, n - 1 + k), now, ("bench", 1, n * 100 + k))
                timings.append((time.perf_counter() - t) * 1000)
            print(f"{n:>18} | {statistics.median(timings):>19.1f}")
    finally:
        client.drop_database(name)
        client.close()


if __name__ == "__main__":
    main()

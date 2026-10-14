"""Per-shipment consistency check (A13): every stored status equals a fresh fold of its events.

    python -m ccp.processor.audit

Read-only. Exits 0 if every shipment matches, 1 otherwise, printing up to 20 differences.
A stale status left behind by a rebalance, a crash or a race would show up here even when the
demo's aggregate counts happen to balance out.
"""

import json
import sys
from collections import defaultdict
from typing import Any

from pymongo.database import Database

from ccp.common import mongo
from ccp.common.config import Config
from ccp.processor.store import FOLD_FIELDS, _status

COMPARED = ("container_id", "state", "state_since", "event_count", "anomalies", "anomaly_count")


def differences(stored: dict[str, Any] | None, events: list[dict[str, Any]]) -> list[str]:
    if stored is None:
        return ["no status document"]
    fresh = _status(events, now=None)  # updated_at is not compared
    return [
        f"{k}: stored {stored.get(k)!r}, fold {fresh[k]!r}"
        for k in COMPARED
        if stored.get(k) != fresh[k]
    ]


def audit(db: Database) -> tuple[int, dict[str, list[str]]]:
    """(shipments checked, {shipment_id: differences}), over shipments in either collection."""
    events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for e in db.events.find({}, {**FOLD_FIELDS, "shipment_id": 1}):
        events[e.pop("shipment_id")].append(e)
    statuses = {s["_id"]: s for s in db.shipment_status.find()}
    shipments = sorted(events.keys() | statuses.keys())
    bad = {}
    for sid in shipments:
        diff = differences(statuses.get(sid), events.get(sid, []))
        if diff:
            bad[sid] = diff
    return len(shipments), bad


def main() -> int:
    db = mongo.database(Config.from_env())
    try:
        checked, bad = audit(db)
    finally:
        db.client.close()
    report = {"shipments": checked, "mismatched": len(bad), "details": dict(list(bad.items())[:20])}
    print(json.dumps(report, default=str))
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())

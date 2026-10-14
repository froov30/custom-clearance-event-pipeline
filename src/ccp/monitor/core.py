"""Monitor core (TRD §12): hold-too-long alerts and the DLQ sink. MongoDB only."""

import json
from datetime import datetime, timedelta

from pymongo.database import Database
from pymongo.errors import DuplicateKeyError


def scan_holds(db: Database, now: datetime, hold_alert_seconds: int) -> int:
    """Inserts one alert per shipment on hold longer than the threshold; returns how many are new.

    The unique (shipment_id, state_since) index turns a repeat into a no-op, so rescans and
    restarts never alert twice for the same hold (D-028).
    """
    cutoff = now - timedelta(seconds=hold_alert_seconds)
    held = db.shipment_status.find(
        {"state": "ON_HOLD", "state_since": {"$lt": cutoff}}, {"state_since": 1}
    )
    new = 0
    for s in held:
        alert = {
            "shipment_id": s["_id"],
            "state_since": s["state_since"],
            "held_seconds": int((now - s["state_since"]).total_seconds()),
            "raised_at": now,
            "resolved_at": None,
        }
        try:
            db.alerts.insert_one(alert)
            new += 1
        except DuplicateKeyError:
            pass  # this hold already has its alert
    return new


def stale_alerts(open_alerts: list[dict], statuses: dict[str, dict]) -> list:
    """The `_id`s of open alerts whose hold has ended: the shipment left ON_HOLD, has no status,
    or a later event moved the hold's start, so the alert is for a hold that no longer exists."""
    stale = []
    for a in open_alerts:
        s = statuses.get(a["shipment_id"])
        if s is None or s["state"] != "ON_HOLD" or s["state_since"] != a["state_since"]:
            stale.append(a["_id"])
    return stale


def resolve_alerts(db: Database, now: datetime) -> int:
    """Closes open alerts whose hold has ended, and returns how many. A false alert raised while
    processing lags (D-064) closes itself on the next scan after the clearing event is processed.

    shortcut: reads every open alert on each scan, which is fine for thousands of holds; add an
    index on `resolved_at` once the open set reaches tens of thousands.
    """
    open_alerts = list(db.alerts.find({"resolved_at": None}, {"shipment_id": 1, "state_since": 1}))
    if not open_alerts:
        return 0
    ids = [a["shipment_id"] for a in open_alerts]
    found = db.shipment_status.find({"_id": {"$in": ids}}, {"state": 1, "state_since": 1})
    stale = stale_alerts(open_alerts, {s["_id"]: s for s in found})
    if stale:
        db.alerts.update_many({"_id": {"$in": stale}}, {"$set": {"resolved_at": now}})
    return len(stale)


def store_rejection(db: Database, value: bytes) -> None:
    """Upserts a DLQ record into `rejections`, keyed by the original message (D-026).

    Replaying the DLQ, or a validator that re-emitted the same rejection, never double-counts.
    Raises ValueError or KeyError for an unreadable record.
    """
    r = json.loads(value)
    key = f"{r['source_topic']}:{r['source_partition']}:{r['source_offset']}"
    db.rejections.update_one(
        {"_id": key},
        {
            "$setOnInsert": {
                "reason": r["reason"],
                "detail": r["detail"],
                "raw": r["raw"],
                "failed_at": datetime.fromisoformat(r["failed_at"]),
            }
        },
        upsert=True,
    )

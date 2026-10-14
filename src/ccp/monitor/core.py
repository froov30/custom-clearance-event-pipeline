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
        }
        try:
            db.alerts.insert_one(alert)
            new += 1
        except DuplicateKeyError:
            pass  # this hold already has its alert
    return new


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

"""Processor core (TRD §8): store the event, fold its shipment, guarded status upsert."""

import logging
from datetime import datetime
from typing import Any

from pymongo.database import Database
from pymongo.errors import DuplicateKeyError

from ccp.common.fold import fold
from ccp.common.schema import parse_event

log = logging.getLogger(__name__)
FOLD_FIELDS = {"_id": 0, "event_id": 1, "event_type": 1, "occurred_at": 1, "container_id": 1}
# Re-folds allowed when events keep arriving while we fold. Every insert triggers its own recompute,
# so the last one always runs to completion: giving up here cannot leave a status permanently stale.
MAX_RECOMPUTES = 5

# (topic, partition, offset) of the Kafka message that carried the event.
Source = tuple[str, int, int]


def event_doc(raw: bytes, ingested_at: datetime, source: Source) -> dict[str, Any]:
    """The `events` document: the validated envelope plus `ingested_at` and where it came from."""
    ev = parse_event(raw)
    topic, partition, offset = source
    return {
        **ev.model_dump(),
        "event_id": str(ev.event_id),
        "event_type": ev.event_type.value,
        "ingested_at": ingested_at,
        "source_message": {"topic": topic, "partition": partition, "offset": offset},
    }


def write_status(db: Database, shipment_id: str, status: dict[str, Any]) -> bool:
    """Guarded upsert: never replaces a status that was folded from more events (D-027).

    Returns False when the write was stale and dropped.
    """
    try:
        db.shipment_status.update_one(
            {"_id": shipment_id, "event_count": {"$lte": status["event_count"]}},
            {"$set": status},
            upsert=True,
        )
        return True
    except DuplicateKeyError:
        # The filter missed because a newer status exists, so the upsert tried to insert on _id.
        log.info("stale status write dropped", extra={"fields": {"shipment_id": shipment_id}})
        return False


def _status(events: list[dict[str, Any]], now: datetime) -> dict[str, Any]:
    result = fold(events)
    return {
        "container_id": result.container_id,
        "state": result.state.value,
        "state_since": result.state_since,
        "event_count": len(events),
        "anomalies": [{**a, "from_state": a["from_state"].value} for a in result.anomalies],
        "anomaly_count": len(result.anomalies),
        "updated_at": now,
    }


def store(db: Database, raw: bytes, now: datetime, source: Source) -> bool:
    """Handles one validated event; returns True if it was a true duplicate. Safe to repeat.

    A true duplicate is the same `event_id` arriving in a different Kafka message. The same
    message arriving again (a redelivery after a crash or rebalance) is not one (D-051).
    """
    doc = event_doc(raw, now, source)
    sid = doc["shipment_id"]
    try:
        db.events.insert_one(doc)
        duplicate = False
    except DuplicateKeyError:
        first = db.events.find_one({"event_id": doc["event_id"]}, {"source_message": 1})
        duplicate = (first or {}).get("source_message") != doc["source_message"]

    # Recompute even on a duplicate key: it heals a crash between the insert and the status write.
    # shortcut: re-reads the whole shipment per event (6 ms at 10 events, 59 ms at 5,000, D-055);
    # switch to an incremental fold when the new event sorts last if histories grow past ~1,000.
    for _ in range(MAX_RECOMPUTES):
        events = list(db.events.find({"shipment_id": sid}, FOLD_FIELDS))
        write_status(db, sid, _status(events, now))
        # A read that overlapped a concurrent insert can miss that event, and the guard cannot
        # see it (the counts tie). If the count has moved on, fold again (D-052).
        if db.events.count_documents({"shipment_id": sid}) == len(events):
            break
    else:
        log.warning("shipment still changing", extra={"fields": {"shipment_id": sid}})

    if duplicate:
        topic, partition, offset = source
        db.duplicates.update_one(
            {"_id": f"{topic}:{partition}:{offset}"},
            {"$setOnInsert": {"event_id": doc["event_id"], "shipment_id": sid, "seen_at": now}},
            upsert=True,
        )
    return duplicate

"""Read-only query API over the pipeline's MongoDB (TRD §13). Swagger UI at /docs."""

from collections.abc import Callable
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pymongo.collection import Collection
from pymongo.database import Database

from ccp.common.fold import State, sort_key

DLQ_REASONS = [
    "SCHEMA_INVALID",
    "CONTAINER_ID_INVALID",
    "UNKNOWN_REFERENCE_DATA",
    "PROCESSING_ERROR",
]
# shortcut: offset pagination slows down on deep pages; switch to keyset on (state_since, _id)
# if anyone pages past a few thousand rows (TRD §13).
Limit = Annotated[int, Query(ge=1, le=200, description="Page size (max 200)")]
Offset = Annotated[int, Query(ge=0)]


def _db(request: Request) -> Database:
    return request.app.state.db


DB = Annotated[Database, Depends(_db)]


def _shipment(doc: dict[str, Any]) -> dict[str, Any]:
    return {"shipment_id": doc.pop("_id"), **doc}


def _page(
    coll: Collection,
    query: dict[str, Any],
    sort: list[tuple[str, int]],
    limit: int,
    offset: int,
    out: Callable[[dict[str, Any]], dict[str, Any]] = lambda d: d,
    projection: dict[str, int] | None = None,
) -> dict[str, Any]:
    docs = coll.find(query, projection).sort(sort).skip(offset).limit(limit)
    return {
        "items": [out(d) for d in docs],
        "limit": limit,
        "offset": offset,
        "total": coll.count_documents(query),
    }


def create_app(db: Database) -> FastAPI:
    app = FastAPI(
        title="Customs Clearance Event Pipeline API",
        description="Read-only views of shipment state, events, anomalies, alerts and rejections. "
        "All data is synthetic.",
        version="0.1.0",
    )
    app.state.db = db

    @app.get("/health")
    def health(db: DB) -> dict[str, str]:
        """Always 200. Reads the driver's cached view of MongoDB, so it never waits on it."""
        up = db.client.topology_description.has_readable_server()
        return {"status": "ok", "mongo": "ok" if up else "down"}

    @app.get("/shipments")
    def list_shipments(
        db: DB, state: State | None = None, limit: Limit = 50, offset: Offset = 0
    ) -> dict[str, Any]:
        """Current status per shipment, longest in its state first. Filter with `state`."""
        query = {"state": state.value} if state else {}
        sort = [("state_since", 1), ("_id", 1)]
        return _page(db.shipment_status, query, sort, limit, offset, _shipment)

    @app.get("/shipments/{shipment_id}")
    def get_shipment(shipment_id: str, db: DB) -> dict[str, Any]:
        """One shipment's current status, derived from its events."""
        doc = db.shipment_status.find_one({"_id": shipment_id})
        if doc is None:
            raise HTTPException(404, f"shipment {shipment_id} not found")
        return _shipment(doc)

    @app.get("/shipments/{shipment_id}/events")
    def shipment_events(shipment_id: str, db: DB) -> list[dict[str, Any]]:
        """Every stored event of a shipment, in the order the state fold applies them."""
        events = list(db.events.find({"shipment_id": shipment_id}, {"_id": 0}))
        if not events:
            raise HTTPException(404, f"no events for shipment {shipment_id}")
        return sorted(events, key=sort_key)

    @app.get("/anomalies")
    def anomalies(db: DB, limit: Limit = 50, offset: Offset = 0) -> dict[str, Any]:
        """Shipments whose event history contains illegal transitions."""
        query = {"anomaly_count": {"$gt": 0}}
        return _page(db.shipment_status, query, [("_id", 1)], limit, offset, _shipment)

    @app.get("/alerts")
    def alerts(db: DB, limit: Limit = 50, offset: Offset = 0) -> dict[str, Any]:
        """Hold-too-long alerts, newest first."""
        sort = [("raised_at", -1), ("_id", -1)]
        return _page(db.alerts, {}, sort, limit, offset, projection={"_id": 0})

    @app.get("/rejections/summary")
    def rejection_summary(db: DB) -> dict[str, int]:
        """Rejected messages per reason code; every known code is listed, even at zero."""
        pipeline = [{"$group": {"_id": "$reason", "n": {"$sum": 1}}}]
        counts = {r["_id"]: r["n"] for r in db.rejections.aggregate(pipeline)}
        return {**dict.fromkeys(DLQ_REASONS, 0), **counts}

    @app.get("/stats")
    def stats(db: DB) -> dict[str, Any]:
        """Pipeline totals, for the demo's reconciliation against `--expect`."""
        group_by_state = [{"$group": {"_id": "$state", "n": {"$sum": 1}}}]
        by_state = {r["_id"]: r["n"] for r in db.shipment_status.aggregate(group_by_state)}
        sum_anomalies = [{"$group": {"_id": None, "n": {"$sum": "$anomaly_count"}}}]
        anomaly_sum = list(db.shipment_status.aggregate(sum_anomalies))
        return {
            "events_stored": db.events.count_documents({}),
            "duplicates_dropped": db.duplicates.count_documents({}),
            "rejected_total": db.rejections.count_documents({}),
            "state_counts": {s.value: by_state.get(s.value, 0) for s in State},
            "anomalies_total": anomaly_sum[0]["n"] if anomaly_sum else 0,
            "alerts_total": db.alerts.count_documents({}),
        }

    return app

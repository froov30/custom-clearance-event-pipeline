"""P8 acceptance: every endpoint on a real MongoDB (D-037), 404s, page-size cap, state filter."""

import time
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from pymongo import MongoClient

from ccp.api.app import create_app
from ccp.common import mongo
from ccp.common.config import Config

pytestmark = pytest.mark.integration
T = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def db():
    cfg = Config.from_env()
    name = f"ccp_it_{uuid.uuid4().hex[:8]}"
    client = mongo.client(cfg)
    database = client[name]
    mongo.ensure_indexes(database)
    _seed(database)
    yield database
    client.drop_database(name)
    client.close()


@pytest.fixture
def api(db):
    return TestClient(create_app(db))


def _status(sid, state, since, anomalies=0):
    return {
        "_id": sid,
        "container_id": "CSQU3054383",
        "state": state,
        "state_since": since,
        "event_count": 3,
        "anomalies": [{"type": "ILLEGAL_TRANSITION"}] * anomalies,
        "anomaly_count": anomalies,
        "updated_at": T,
    }


def _event(sid, etype, minutes, eid):
    return {
        "event_id": eid,
        "shipment_id": sid,
        "container_id": "CSQU3054383",
        "event_type": etype,
        "occurred_at": T + timedelta(minutes=minutes),
        "source": "test",
        "schema_version": 1,
        "payload": {},
        "ingested_at": T,
    }


def _seed(db):
    db.shipment_status.insert_many(
        [
            _status("S-1", "ON_HOLD", T - timedelta(minutes=30)),
            _status("S-2", "CLEARED", T - timedelta(minutes=20), anomalies=2),
            _status("S-3", "RELEASED", T - timedelta(minutes=10)),
        ]
    )
    # Inserted out of order on purpose: the API must return fold order.
    db.events.insert_many(
        [
            _event("S-1", "INSPECTION_HOLD", 2, "e3"),
            _event("S-1", "DECLARATION_FILED", 0, "e1"),
            _event("S-1", "DOCUMENTS_VERIFIED", 1, "e2"),
        ]
    )
    later = T + timedelta(seconds=5)
    db.alerts.insert_many(
        [
            {"shipment_id": "S-1", "state_since": T, "held_seconds": 25, "raised_at": T},
            {"shipment_id": "S-9", "state_since": T, "held_seconds": 40, "raised_at": later},
        ]
    )
    db.rejections.insert_many(
        [
            {"_id": "raw:0:1", "reason": "SCHEMA_INVALID"},
            {"_id": "raw:0:2", "reason": "SCHEMA_INVALID"},
            {"_id": "raw:0:3", "reason": "CONTAINER_ID_INVALID"},
        ]
    )
    db.duplicates.insert_many([{"_id": "v:0:9"}, {"_id": "v:1:9"}])


def test_health_reports_mongo_ok(api):
    assert api.get("/health").json() == {"status": "ok", "mongo": "ok"}


def test_health_stays_200_and_fast_when_mongo_is_unreachable():
    client = MongoClient("mongodb://127.0.0.1:1/", serverSelectionTimeoutMS=200)
    started = time.monotonic()
    r = TestClient(create_app(client["x"])).get("/health")
    assert (r.status_code, r.json()) == (200, {"status": "ok", "mongo": "down"})
    assert time.monotonic() - started < 1.0  # a liveness probe must not wait on MongoDB
    client.close()


def test_list_shipments_paginates_with_totals(api):
    body = api.get("/shipments", params={"limit": 1, "offset": 1}).json()
    assert (body["limit"], body["offset"], body["total"]) == (1, 1, 3)
    assert [s["shipment_id"] for s in body["items"]] == ["S-2"]  # oldest state_since first
    assert "_id" not in body["items"][0]


def test_state_filter(api):
    body = api.get("/shipments", params={"state": "ON_HOLD"}).json()
    assert [s["shipment_id"] for s in body["items"]] == ["S-1"]
    assert body["total"] == 1


@pytest.mark.parametrize(
    "params",
    [{"limit": 201}, {"limit": 0}, {"offset": -1}, {"state": "NOT_A_STATE"}],
)
def test_bad_query_parameters_are_422(api, params):
    assert api.get("/shipments", params=params).status_code == 422


def test_page_size_cap_is_inclusive(api):
    assert api.get("/shipments", params={"limit": 200}).status_code == 200


def test_get_one_shipment_or_404(api):
    s = api.get("/shipments/S-2").json()
    assert (s["shipment_id"], s["state"], s["anomaly_count"]) == ("S-2", "CLEARED", 2)
    assert api.get("/shipments/NOPE").status_code == 404


def test_events_come_back_in_fold_order_or_404(api):
    events = api.get("/shipments/S-1/events").json()
    assert [e["event_id"] for e in events] == ["e1", "e2", "e3"]
    assert all("_id" not in e for e in events)
    assert api.get("/shipments/NOPE/events").status_code == 404


def test_anomalies_lists_only_shipments_with_anomalies(api):
    body = api.get("/anomalies").json()
    assert [s["shipment_id"] for s in body["items"]] == ["S-2"]
    assert body["total"] == 1


def test_alerts_newest_first(api):
    body = api.get("/alerts").json()
    assert [a["shipment_id"] for a in body["items"]] == ["S-9", "S-1"]
    assert all("_id" not in a for a in body["items"])


def test_rejection_summary_lists_every_reason_code(api):
    assert api.get("/rejections/summary").json() == {
        "SCHEMA_INVALID": 2,
        "CONTAINER_ID_INVALID": 1,
        "UNKNOWN_REFERENCE_DATA": 0,
        "PROCESSING_ERROR": 0,
    }


def test_stats(api):
    stats = api.get("/stats").json()
    assert stats == {
        "events_stored": 3,
        "duplicates_dropped": 2,
        "rejected_total": 3,
        "state_counts": {
            "UNKNOWN": 0,
            "FILED": 0,
            "DOCS_VERIFIED": 0,
            "ON_HOLD": 1,
            "CLEARED": 1,
            "RELEASED": 1,
            "REJECTED": 0,
        },
        "anomalies_total": 2,
        "alerts_total": 2,
    }


def test_swagger_and_openapi_are_served(api):
    assert api.get("/docs").status_code == 200
    paths = api.get("/openapi.json").json()["paths"]
    assert {"/health", "/shipments", "/shipments/{shipment_id}", "/stats"} <= set(paths)

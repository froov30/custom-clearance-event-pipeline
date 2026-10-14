"""P7 acceptance: one alert per hold across monitor restarts; DLQ replay never double-counts."""

import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ccp.common import mongo
from ccp.common.config import Config
from ccp.common.kafka import producer
from ccp.common.topics import DLQ
from ccp.monitor.core import scan_holds, store_rejection

pytestmark = pytest.mark.integration
ROOT = Path(__file__).parents[2]
NOW = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def cfg():
    return Config.from_env()


@pytest.fixture
def db(cfg):
    name = f"ccp_it_{uuid.uuid4().hex[:8]}"
    client = mongo.client(cfg)
    database = client[name]
    mongo.ensure_indexes(database)
    yield database
    client.drop_database(name)
    client.close()


def status(sid, state, since):
    return {"_id": sid, "state": state, "state_since": since, "event_count": 3}


def dlq_record(reason, source_topic, offset, raw="bad"):
    return json.dumps(
        {
            "reason": reason,
            "detail": "test",
            "source_topic": source_topic,
            "source_partition": 0,
            "source_offset": offset,
            "raw": raw,
            "failed_at": "2026-05-01T12:00:00+00:00",
        }
    ).encode()


# --- in process ----------------------------------------------------------------------------


def test_only_holds_older_than_the_threshold_alert_and_only_once(db):
    db.shipment_status.insert_many(
        [
            status("S-OLD", "ON_HOLD", NOW - timedelta(seconds=30)),
            status("S-NEW", "ON_HOLD", NOW - timedelta(seconds=5)),
            status("S-DONE", "CLEARED", NOW - timedelta(seconds=60)),
        ]
    )
    assert scan_holds(db, NOW, hold_alert_seconds=20) == 1
    assert scan_holds(db, NOW + timedelta(seconds=5), hold_alert_seconds=20) == 0  # same hold
    alert = db.alerts.find_one({"shipment_id": "S-OLD"}, {"_id": 0})
    assert alert == {
        "shipment_id": "S-OLD",
        "state_since": NOW - timedelta(seconds=30),
        "held_seconds": 30,
        "raised_at": NOW,
    }
    assert db.alerts.count_documents({}) == 1


def test_a_new_hold_on_the_same_shipment_alerts_again(db):
    db.shipment_status.insert_one(status("S-1", "ON_HOLD", NOW - timedelta(seconds=30)))
    assert scan_holds(db, NOW, 20) == 1
    db.shipment_status.update_one({"_id": "S-1"}, {"$set": {"state_since": NOW}})
    assert scan_holds(db, NOW + timedelta(seconds=25), 20) == 1
    assert db.alerts.count_documents({"shipment_id": "S-1"}) == 2


def test_rejections_are_keyed_by_the_original_message(db):
    first = dlq_record("SCHEMA_INVALID", "customs.events.raw", 7)
    store_rejection(db, first)
    store_rejection(db, first)  # the same DLQ record again (a replay)
    store_rejection(db, dlq_record("SCHEMA_INVALID", "customs.events.raw", 8))
    doc = db.rejections.find_one({"_id": "customs.events.raw:0:7"})
    assert (doc["reason"], doc["raw"], doc["failed_at"]) == (
        "SCHEMA_INVALID",
        "bad",
        datetime(2026, 5, 1, 12, 0, tzinfo=UTC),
    )
    assert db.rejections.count_documents({}) == 2


# --- the real monitor process ----------------------------------------------------------------


def _env(db, tmp_path, suffix):
    return {
        **os.environ,
        "MONGO_DB": db.name,
        "HEARTBEAT_DIR": str(tmp_path),
        "HOLD_ALERT_SECONDS": "20",
        "MONITOR_SCAN_SECONDS": "1",
        "CCP_GROUP_SUFFIX": suffix,
    }


def _run_monitor(env, log_path, seconds_or_until):
    """Runs the monitor until the predicate holds (or for N seconds), then stops it cleanly."""
    with log_path.open("ab") as out:
        proc = subprocess.Popen(
            [sys.executable, "-m", "ccp.monitor"],
            cwd=ROOT,
            env=env,
            stdout=out,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )
        try:
            if callable(seconds_or_until):
                end = time.monotonic() + 60
                while not seconds_or_until() and time.monotonic() < end:
                    time.sleep(0.5)
            else:
                time.sleep(seconds_or_until)
        finally:
            proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
    return proc.returncode


def test_one_alert_per_hold_across_two_monitor_restarts(db, tmp_path):
    now = datetime.now(UTC)
    db.shipment_status.insert_one(status("S-HELD", "ON_HOLD", now - timedelta(seconds=60)))
    env = _env(db, tmp_path, f"-it-{uuid.uuid4().hex[:8]}")
    log = tmp_path / "monitor.log"

    assert _run_monitor(env, log, lambda: db.alerts.count_documents({}) == 1) == 0
    assert db.alerts.count_documents({}) == 1
    for _ in range(2):  # two restarts, each running several scans
        assert _run_monitor(env, log, 4) == 0
    assert db.alerts.count_documents({"shipment_id": "S-HELD"}) == 1


def test_replaying_the_dlq_does_not_double_count_rejections(cfg, db, tmp_path):
    run = f"it-{uuid.uuid4().hex[:8]}"  # unique source topic name, so only this run's records match
    p = producer(cfg.kafka_bootstrap_servers)
    for offset, reason in enumerate(["SCHEMA_INVALID", "CONTAINER_ID_INVALID", "SCHEMA_INVALID"]):
        p.produce(DLQ, key=run.encode(), value=dlq_record(reason, run, offset))
    assert p.flush(15) == 0
    own = {"_id": {"$regex": f"^{run}:"}}
    log = tmp_path / "monitor.log"

    env = _env(db, tmp_path, f"-it-{uuid.uuid4().hex[:8]}")
    assert _run_monitor(env, log, lambda: db.rejections.count_documents(own) == 3) == 0
    # A brand-new group reads the whole DLQ again from the start: the full replay case.
    env = _env(db, tmp_path, f"-it-{uuid.uuid4().hex[:8]}")
    assert _run_monitor(env, log, 5) == 0
    assert db.rejections.count_documents(own) == 3
    reasons = sorted(d["reason"] for d in db.rejections.find(own))
    assert reasons == ["CONTAINER_ID_INVALID", "SCHEMA_INVALID", "SCHEMA_INVALID"]

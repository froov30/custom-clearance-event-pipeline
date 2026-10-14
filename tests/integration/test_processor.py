"""HARD CHECKPOINT 1 evidence (P6) plus the limit fixes (D-051 to D-054).

Covers: replay, shuffle, crash healing, kill and restart, true vs replayed duplicates,
overlapping workers, a stale read, and MongoDB being down or hung at start-up and mid-run.
"""

import json
import os
import random
import shutil
import signal
import subprocess
import sys
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ccp.common import mongo, refdata
from ccp.common.config import Config
from ccp.common.fold import fold
from ccp.common.kafka import producer
from ccp.common.topics import VALIDATED
from ccp.processor.store import event_doc, store, write_status
from ccp.simulator.generate import SimConfig, generate
from ccp.validator.route import route

pytestmark = pytest.mark.integration
ROOT = Path(__file__).parents[2]
REF = refdata.load(ROOT / "data" / "ref")
START = datetime(2026, 1, 1, tzinfo=UTC)
T1 = datetime(2026, 2, 1, tzinfo=UTC)
T2 = datetime(2026, 2, 2, tzinfo=UTC)
MONGO_CONTAINER = "ccp-mongo-1"
FAULTS = dict(
    open_hold_rate=0.2,
    duplicate_rate=0.15,
    late_rate=0.15,
    malformed_rate=0.05,
    illegal_rate=0.15,
    bad_container_rate=0.05,
    unknown_ref_rate=0.05,
)
DECLARATION = {"port_code": "NLRTM", "hs_code": "847130", "declarant": "X"}


def source(i: int):
    """A distinct Kafka position per delivered message."""
    return (VALIDATED, i % 6, i)


def raw_event(sid: str, etype: str, minute: int, payload: dict | None = None) -> bytes:
    return json.dumps(
        {
            "event_id": str(uuid.uuid4()),
            "shipment_id": sid,
            "container_id": "CSQU3054383",
            "event_type": etype,
            "occurred_at": f"2026-03-01T08:{minute:02d}:00Z",
            "source": "test",
            "schema_version": 1,
            "payload": payload or {},
        }
    ).encode()


def validated_stream(seed: int, shipments: int, prefix: str = ""):
    """What the validator emits for a simulated run, in send order, duplicates included."""
    plan = generate(seed, SimConfig(shipments=shipments, **FAULTS), START, REF)
    stream = []
    for m in plan.messages:
        r = route(m.value, REF)
        if r.reason is None:
            ev = json.loads(r.value)
            ev["shipment_id"] = prefix + ev["shipment_id"]
            if prefix:
                # Topics outlive a test run, and a fixed seed repeats its event_ids, so an old run's
                # events would make this run's look like duplicates. Same id for both copies of a
                # duplicate, different id per run.
                ev["event_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, prefix + ev["event_id"]))
            stream.append((ev["shipment_id"], json.dumps(ev).encode()))
    return plan, stream


def expected(plan, prefix: str = "") -> dict[str, tuple]:
    """(state, anomaly_count, event_count) per shipment, folded from the plan's valid events."""
    by_shipment: dict[str, list] = {}
    for e in plan.events:
        by_shipment.setdefault(e["shipment_id"], []).append(e)
    out = {}
    for sid, events in by_shipment.items():
        r = fold(events)
        out[prefix + sid] = (r.state.value, len(r.anomalies), len(events))
    return out


def statuses(db, prefix: str = "") -> dict[str, tuple]:
    docs = db.shipment_status.find({"_id": {"$regex": f"^{prefix}"}})
    return {d["_id"]: (d["state"], d["anomaly_count"], d["event_count"]) for d in docs}


def snapshot(db) -> list[dict]:
    """Every status document except `updated_at`, which a replay is allowed to change."""
    docs = db.shipment_status.find().sort("_id")
    return [{k: v for k, v in d.items() if k != "updated_at"} for d in docs]


def duplicates_dropped(db, prefix: str = "") -> int:
    return db.duplicates.count_documents({"shipment_id": {"$regex": f"^{prefix}"}})


def deliver_all(db, stream, now, base: int = 0) -> None:
    for i, (_, value) in enumerate(stream):
        store(db, value, now, source(base + i))


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


# --- Checkpoint 1: replay, shuffle, crash healing -------------------------------------------


def test_redelivering_the_stream_changes_nothing(db):
    plan, stream = validated_stream(seed=101, shipments=30)
    deliver_all(db, stream, T1)
    first = snapshot(db)
    assert statuses(db) == expected(plan)
    injected = len(stream) - len(plan.events)
    assert injected > 0
    assert duplicates_dropped(db) == injected

    deliver_all(db, stream, T2)  # the same Kafka messages again, as after a crash or rebalance
    assert snapshot(db) == first  # byte-identical apart from updated_at
    assert db.events.count_documents({}) == len(plan.events)
    assert all(d["updated_at"] == T2 for d in db.shipment_status.find())  # the replay did write
    assert duplicates_dropped(db) == injected  # redeliveries are not duplicates (D-051)


def test_resending_the_stream_as_new_messages_counts_every_message_as_a_duplicate(db):
    plan, stream = validated_stream(seed=101, shipments=30)
    deliver_all(db, stream, T1)
    first = snapshot(db)
    injected = duplicates_dropped(db)

    deliver_all(db, stream, T2, base=len(stream))  # same events, new Kafka positions
    assert snapshot(db) == first
    assert db.events.count_documents({}) == len(plan.events)
    assert duplicates_dropped(db) == injected + len(stream)


def test_shuffled_arrival_orders_give_identical_final_states(db):
    plan, stream = validated_stream(seed=202, shipments=30)
    snapshots = []
    for order_seed in range(3):
        for name in ("events", "shipment_status", "duplicates"):
            db[name].delete_many({})
        shuffled = stream[:]
        random.Random(order_seed).shuffle(shuffled)
        deliver_all(db, shuffled, T1)
        snapshots.append(snapshot(db))
        assert duplicates_dropped(db) == len(stream) - len(plan.events)
    assert snapshots[0] == snapshots[1] == snapshots[2]
    assert statuses(db) == expected(plan)


def test_crash_between_insert_and_status_write_heals_on_redelivery(db):
    plan, stream = validated_stream(seed=303, shipments=8)
    ids = [json.loads(v)["event_id"] for _, v in stream]
    once = Counter(ids)
    crash = max(i for i, eid in enumerate(ids) if once[eid] == 1)
    for i, (_, value) in enumerate(stream):
        if i != crash:
            store(db, value, T1, source(i))

    # The crash: the event is inserted, then the process dies before the status write.
    sid, value = stream[crash]
    db.events.insert_one(event_doc(value, T1, source(crash)))
    before = db.shipment_status.find_one({"_id": sid})
    assert before is None or before["event_count"] < db.events.count_documents({"shipment_id": sid})

    assert store(db, value, T2, source(crash)) is False  # redelivered: heals, and is no duplicate
    assert statuses(db) == expected(plan)
    assert duplicates_dropped(db) == len(stream) - len(plan.events)


# --- Limit 4: duplicates are counted exactly ---------------------------------------------------


def test_a_redelivered_message_is_not_a_duplicate_but_a_second_message_is(db):
    raw = raw_event("S-1", "DECLARATION_FILED", 0, DECLARATION)
    assert store(db, raw, T1, source(1)) is False
    assert store(db, raw, T2, source(1)) is False  # same Kafka message again
    assert duplicates_dropped(db) == 0
    assert store(db, raw, T2, source(2)) is True  # same event_id in another message
    assert store(db, raw, T2, source(2)) is True  # ... redelivered: still counted once
    assert duplicates_dropped(db) == 1


# --- Limit 2: overlapping workers ------------------------------------------------------------


def test_older_computation_never_overwrites_a_newer_status(db):
    def status(n, state):
        return {"state": state, "event_count": n}

    assert write_status(db, "S-1", status(4, "RELEASED")) is True
    assert write_status(db, "S-1", status(3, "CLEARED")) is False  # from an overlapping worker
    assert db.shipment_status.find_one({"_id": "S-1"}) == {
        "_id": "S-1",
        "state": "RELEASED",
        "event_count": 4,
    }
    assert write_status(db, "S-1", status(4, "RELEASED")) is True  # equal count: a replay
    assert write_status(db, "S-1", status(5, "RELEASED")) is True


class StaleOnce:
    """A database whose first events.find() misses the newest event, like a read that overlapped
    a concurrent insert. The guard cannot catch this: the stale and the real count can tie."""

    class _Events:
        def __init__(self, real):
            self._real, self._stale = real, True

        def find(self, *args, **kwargs):
            docs = list(self._real.find(*args, **kwargs))
            if self._stale and docs:
                self._stale = False
                return docs[:-1]
            return docs

        def __getattr__(self, name):
            return getattr(self._real, name)

    def __init__(self, real):
        self._real = real
        self.events = self._Events(real.events)

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_a_read_that_missed_a_concurrent_insert_is_repaired(db):
    steps = [
        ("DECLARATION_FILED", DECLARATION),
        ("DOCUMENTS_VERIFIED", {}),
        ("INSPECTION_HOLD", {"port_code": "NLRTM", "reason": "RANDOM_CHECK"}),
        ("CLEARED", {}),
    ]
    for i, (etype, payload) in enumerate(steps):
        target = StaleOnce(db) if i == 3 else db
        store(target, raw_event("S-RACE", etype, i, payload), T1, source(i))
    s = db.shipment_status.find_one({"_id": "S-RACE"})
    assert (s["state"], s["event_count"]) == ("CLEARED", 4)


def test_workers_overlapping_on_the_same_shipments_converge(db):
    plan, stream = validated_stream(seed=505, shipments=40)
    for round_ in range(3):
        for name in ("events", "shipment_status", "duplicates"):
            db[name].delete_many({})
        shuffled = stream[:]
        random.Random(round_).shuffle(shuffled)
        # 8 threads on one MongoDB: every shipment is handled by several workers at once,
        # as two processors would during a rebalance.
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda iv: store(db, iv[1][1], T1, source(iv[0])), enumerate(shuffled)))
        assert statuses(db) == expected(plan), f"round {round_}"


# --- Real processor subprocess: kill, outage, start-up -----------------------------------------


def _env(db, tmp_path):
    return {
        **os.environ,
        "MONGO_DB": db.name,
        "HEARTBEAT_DIR": str(tmp_path),
        "CCP_GROUP_SUFFIX": f"-it-{uuid.uuid4().hex[:8]}",  # own group: the real one is untouched
    }


def _start(env, log_path):
    out = log_path.open("ab")
    proc = subprocess.Popen(
        [sys.executable, "-m", "ccp.processor"],
        cwd=ROOT,
        env=env,
        stdout=out,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    return proc, out


def _stop(proc, out) -> int:
    """Asks the processor to stop, then returns its real exit code."""
    if proc.poll() is None:
        proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    out.close()
    return proc.returncode


def _wait_for(predicate, timeout: float, interval: float = 0.5) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def _produce(cfg, *pairs):
    p = producer(cfg.kafka_bootstrap_servers)
    for sid, value in pairs:
        p.produce(VALIDATED, key=sid.encode(), value=value)
    assert p.flush(15) == 0


def _own_declaration(prefix: str, i: int):
    sid = f"{prefix}{i}"
    return sid, raw_event(sid, "DECLARATION_FILED", i, DECLARATION)


def _heartbeat_age(tmp_path) -> float:
    beat = tmp_path / "processor.heartbeat"
    return time.time() - beat.stat().st_mtime if beat.exists() else float("inf")


def _docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=120)


def _need_mongo_container():
    if shutil.which("docker") is None or _docker("inspect", MONGO_CONTAINER).returncode != 0:
        pytest.skip(f"needs the Compose container {MONGO_CONTAINER}")


def _mongo_answers(db) -> bool:
    try:
        db.client.admin.command("ping")
        return True
    except Exception:
        return False


def test_killed_processor_heals_after_restart(cfg, db, tmp_path):
    prefix = f"IT{uuid.uuid4().hex[:6]}-"
    plan, stream = validated_stream(seed=404, shipments=60, prefix=prefix)
    _produce(cfg, *stream)
    own = {"shipment_id": {"$regex": f"^{prefix}"}}
    env, log_path = _env(db, tmp_path), tmp_path / "processor.log"

    proc, out = _start(env, log_path)
    assert _wait_for(lambda: db.events.count_documents(own) > 0, 60, interval=0.02)
    proc.kill()  # hard kill: no signal handler, no clean close, in-flight work lost
    proc.wait()
    out.close()
    stored_at_kill = db.events.count_documents(own)
    assert 0 < stored_at_kill < len(plan.events), f"kill must land mid-run ({stored_at_kill})"

    proc, out = _start(env, log_path)
    try:
        assert _wait_for(lambda: statuses(db, prefix) == expected(plan, prefix), 120, interval=1)
        assert db.events.count_documents(own) == len(plan.events)
        # Crash replays are redeliveries, not duplicates: only the injected ones are counted.
        assert duplicates_dropped(db, prefix) == len(stream) - len(plan.events)
    finally:
        code = _stop(proc, out)
    assert code == 0


def test_mongo_hung_mid_run_keeps_the_heartbeat_fresh_and_processing_resumes(cfg, db, tmp_path):
    """Limit 1: a paused MongoDB must look like an outage (retry, heartbeat), not a silent hang."""
    _need_mongo_container()
    prefix = f"IT{uuid.uuid4().hex[:6]}-"
    own = {"shipment_id": {"$regex": f"^{prefix}"}}
    proc, out = _start(_env(db, tmp_path), tmp_path / "processor.log")
    try:
        _produce(cfg, _own_declaration(prefix, 0))
        assert _wait_for(lambda: db.events.count_documents(own) == 1, 90)

        assert _docker("pause", MONGO_CONTAINER).returncode == 0
        try:
            _produce(cfg, _own_declaration(prefix, 1), _own_declaration(prefix, 2))
            peak, end = 0.0, time.monotonic() + 40
            while time.monotonic() < end:
                time.sleep(0.5)
                peak = max(peak, _heartbeat_age(tmp_path))
            alive = proc.poll() is None
        finally:
            _docker("unpause", MONGO_CONTAINER)

        assert alive, "the processor must not exit during an outage"
        # Worst case is three stacked 5 s timeouts (server selection, connect, socket): measured
        # 14.7 s. Without them a hung call never returns and the age grows to the full 40 s.
        assert peak < 25, f"heartbeat went stale for {peak:.1f}s: the pod would be restarted"
        assert _wait_for(lambda: _mongo_answers(db), 60)
        assert _wait_for(lambda: db.events.count_documents(own) == 3, 90)
    finally:
        _stop(proc, out)


def test_processor_waits_for_mongo_at_startup(cfg, db, tmp_path):
    """Limit 5: MongoDB down when the processor starts must not crash the process."""
    _need_mongo_container()
    prefix = f"IT{uuid.uuid4().hex[:6]}-"
    own = {"shipment_id": {"$regex": f"^{prefix}"}}
    proc = out = None
    try:
        assert _docker("stop", MONGO_CONTAINER).returncode == 0
        proc, out = _start(_env(db, tmp_path), tmp_path / "processor.log")
        time.sleep(12)
        alive, age = proc.poll() is None, _heartbeat_age(tmp_path)
    finally:
        _docker("start", MONGO_CONTAINER)
    try:
        assert alive, "the processor exited because MongoDB was down at startup"
        assert age < 10, f"heartbeat went stale ({age:.1f}s) while waiting for MongoDB"
        assert _wait_for(lambda: _mongo_answers(db), 90)
        _produce(cfg, _own_declaration(prefix, 0))
        assert _wait_for(lambda: db.events.count_documents(own) == 1, 90)
    finally:
        code = _stop(proc, out) if proc is not None else None
    assert code == 0

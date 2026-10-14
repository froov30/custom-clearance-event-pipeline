import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from confluent_kafka import Consumer

from ccp.common.config import Config
from ccp.common.kafka import producer
from ccp.common.topics import DLQ, RAW, VALIDATED

pytestmark = pytest.mark.integration
ROOT = Path(__file__).parents[2]


def event(shipment_id, **over) -> bytes:
    ev = {
        "event_id": str(uuid.uuid4()),
        "shipment_id": shipment_id,
        "container_id": "CSQU3054383",
        "event_type": "DECLARATION_FILED",
        "occurred_at": "2026-10-11T08:00:00Z",
        "source": "integration_test",
        "schema_version": 1,
        "payload": {"port_code": "NLRTM", "hs_code": "847130", "declarant": "ACME Imports"},
    }
    ev.update(over)
    return json.dumps(ev).encode()


def collect(cfg: Config, topic: str, keys: set[str], timeout: float = 90.0) -> dict[str, bytes]:
    """Reads the topic from the start until every key has been seen, using a throwaway group."""
    c = Consumer(
        {
            "bootstrap.servers": cfg.kafka_bootstrap_servers,
            "group.id": f"ccp-it-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([topic])
    found: dict[str, bytes] = {}
    deadline = time.monotonic() + timeout
    try:
        while keys - found.keys() and time.monotonic() < deadline:
            m = c.poll(1.0)
            if m is None or m.error():
                continue
            key = (m.key() or b"").decode()
            if key in keys:
                found[key] = m.value()
    finally:
        c.close()
    return found


@pytest.fixture
def cfg():
    return Config.from_env()


@pytest.fixture
def validator(tmp_path):
    log_file = (tmp_path / "validator.log").open("wb")
    windows = os.name == "nt"
    proc = subprocess.Popen(
        [sys.executable, "-m", "ccp.validator"],
        cwd=ROOT,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if windows else 0,
    )
    yield proc, tmp_path / "validator.log"
    if proc.poll() is None:
        proc.kill()
    log_file.close()


def ask_to_stop(proc: subprocess.Popen) -> int:
    # Ctrl+Break is how a separate Windows process group is asked to stop; SIGTERM elsewhere.
    proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
    return proc.wait(timeout=30)


def test_routes_each_event_by_reason_and_stops_cleanly(cfg, validator):
    proc, log_path = validator
    run = uuid.uuid4().hex[:8]
    ok = f"IT-{run}-ok"
    unknown_ref = {"port_code": "XXZZZ", "hs_code": "847130", "declarant": "A"}
    bad = {
        f"IT-{run}-schema": (b"{not json", "SCHEMA_INVALID"),
        f"IT-{run}-container": (
            event(f"IT-{run}-container", container_id="CSQU3054384"),
            "CONTAINER_ID_INVALID",
        ),
        f"IT-{run}-ref": (event(f"IT-{run}-ref", payload=unknown_ref), "UNKNOWN_REFERENCE_DATA"),
    }

    p = producer(cfg.kafka_bootstrap_servers)
    p.produce(RAW, key=ok.encode(), value=event(ok))
    for key, (value, _) in bad.items():
        p.produce(RAW, key=key.encode(), value=value)
    assert p.flush(10) == 0

    validated = collect(cfg, VALIDATED, {ok})
    assert ok in validated, "the clean event must reach the validated topic"
    assert json.loads(validated[ok])["shipment_id"] == ok

    dlq = collect(cfg, DLQ, set(bad))
    for key, (_, reason) in bad.items():
        record = json.loads(dlq[key])
        assert (record["reason"], record["source_topic"]) == (reason, RAW)

    assert ask_to_stop(proc) == 0
    log = log_path.read_text(encoding="utf-8", errors="replace")
    assert '"msg": "consumer closed"' in log
    assert '"msg": "stopped"' in log

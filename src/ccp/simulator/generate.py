"""Seeded, pure event generator with fault injection and the expected-outcome manifest (TRD §11).

Callers: `ccp.simulator.__main__` (produce and --expect) and tests/test_simulator.py.
It reads no files and no clock: the reference data and start time are arguments.
"""

import json
import random
import string
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from ccp.common import iso6346
from ccp.common.fold import State, fold
from ccp.common.refdata import RefData
from ccp.common.schema import EventType

DF, DV, IH = EventType.DECLARATION_FILED, EventType.DOCUMENTS_VERIFIED, EventType.INSPECTION_HOLD
CL, RL, RJ = EventType.CLEARED, EventType.RELEASED, EventType.REJECTED

SCRIPTS = {
    "happy": [DF, DV, CL, RL],
    "hold_clear": [DF, DV, IH, CL],
    "hold_reject": [DF, DV, IH, RJ],
    "filed_reject": [DF, RJ],
}
OPEN_HOLD = [DF, DV, IH]  # left on hold: the alert case
# Illegal shapes: skip DOCUMENTS_VERIFIED, or release without CLEARED. Both yield anomalies.
ILLEGAL_SKIP_DV = [DF, CL, RL]
ILLEGAL_RELEASE = [DF, DV, RL]

UNKNOWN_PORT = "XXZZZ"  # well-formed (PORT_PATTERN) but not in data/ref/ports.csv
UNKNOWN_HS = "999999"  # well-formed (HS_PATTERN) but not in data/ref/hs_codes.csv
HOLD_REASONS = ["RANDOM_CHECK", "DOCUMENT_MISMATCH"]
JITTER_S = 0.5  # extra delay per step, uniform in [0, JITTER_S)
DUPLICATE_DELAY_S = 0.5
SOURCE = "simulator"
REJECT_REASON = {
    "malformed": "SCHEMA_INVALID",
    "bad_container": "CONTAINER_ID_INVALID",
    "unknown_reference": "UNKNOWN_REFERENCE_DATA",
}
DLQ_CODES = ["SCHEMA_INVALID", "CONTAINER_ID_INVALID", "UNKNOWN_REFERENCE_DATA", "PROCESSING_ERROR"]


@dataclass(frozen=True)
class SimConfig:
    shipments: int = 20
    step_delay: float = 1.0
    late_delay: float = 3.0
    open_hold_rate: float = 0.0
    duplicate_rate: float = 0.0
    late_rate: float = 0.0
    malformed_rate: float = 0.0
    illegal_rate: float = 0.0
    bad_container_rate: float = 0.0
    unknown_ref_rate: float = 0.0


@dataclass(frozen=True)
class Message:
    send_at: float  # seconds after the run starts
    key: str  # shipment_id
    value: bytes
    event_id: str
    occurred_at: datetime
    fault: str | None  # None, "late", "duplicate", or a key of REJECT_REASON


@dataclass(frozen=True)
class Plan:
    messages: list[Message]
    events: list[dict[str, Any]]  # unique events that pass validation, for the fold
    shipments: list[str]
    illegal_shipments: int


def check_timing(cfg: SimConfig, hold_alert_seconds: int) -> None:
    """Keeps a held-then-cleared shipment from alerting, and a late event from reading as a hold."""
    if cfg.late_delay >= hold_alert_seconds:
        raise ValueError(
            f"late_delay {cfg.late_delay} must be below HOLD_ALERT_SECONDS {hold_alert_seconds}"
        )
    if cfg.step_delay + JITTER_S >= hold_alert_seconds:
        raise ValueError(f"step_delay {cfg.step_delay} + jitter must be below HOLD_ALERT_SECONDS")


def _container(rng: random.Random) -> str:
    owner = "".join(rng.choice(string.ascii_uppercase) for _ in range(3))
    first_ten = f"{owner}U{rng.randrange(10**6):06d}"
    return first_ten + str(iso6346.check_digit(first_ten))


def _event(rng, sid, container, etype, occurred_at, ports, hs_codes) -> dict[str, Any]:
    if etype == DF:
        payload = {
            "port_code": rng.choice(ports),
            "hs_code": rng.choice(hs_codes),
            "declarant": f"DECL-{rng.randrange(10_000):04d}",
        }
    elif etype == IH:
        payload = {"port_code": rng.choice(ports), "reason": rng.choice(HOLD_REASONS)}
    elif etype == RJ:
        payload = {"reason": "INCOMPLETE_DOCUMENTS"}
    else:
        payload = {}
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "shipment_id": sid,
        "container_id": container,
        "event_type": etype.value,
        "occurred_at": occurred_at,
        "source": SOURCE,
        "schema_version": 1,
        "payload": payload,
    }


def _wire(ev: dict[str, Any]) -> bytes:
    out = {**ev, "occurred_at": ev["occurred_at"].isoformat()}
    return json.dumps(out, sort_keys=True).encode()


def _malformed(rng: random.Random, ev: dict[str, Any]) -> bytes:
    variant = rng.randrange(3)
    if variant == 0:
        return b"{not json"
    wire = json.loads(_wire(ev))
    if variant == 1:
        del wire["source"]
    else:
        wire["schema_version"] = "1"  # a string where strict mode needs an int
    return json.dumps(wire, sort_keys=True).encode()


def _corrupt_check_digit(container: str) -> str:
    return container[:-1] + str((int(container[-1]) + 1) % 10)


def _script(rng: random.Random, cfg: SimConfig) -> tuple[list[EventType], bool]:
    if rng.random() < cfg.open_hold_rate:
        steps = OPEN_HOLD
    else:
        steps = SCRIPTS[rng.choice(sorted(SCRIPTS))]
    illegal = rng.random() < cfg.illegal_rate
    skip_dv = rng.random() < 0.5
    if illegal:
        steps = ILLEGAL_SKIP_DV if skip_dv else ILLEGAL_RELEASE
    return list(steps), illegal


def generate(seed: int, cfg: SimConfig, start: datetime, ref: RefData) -> Plan:
    rng = random.Random(seed)
    # sorted(): frozenset order changes between processes, which would break reproducibility.
    ports, hs_codes = sorted(ref.ports), sorted(ref.hs_codes)
    candidates: list[dict[str, Any]] = []
    shipments: list[str] = []
    illegal_shipments = 0

    for i in range(cfg.shipments):
        sid = f"SHP-{i + 1:04d}"
        shipments.append(sid)
        container = _container(rng)
        offset = rng.uniform(0, 5)
        steps, illegal = _script(rng, cfg)
        illegal_shipments += illegal
        for k, etype in enumerate(steps):
            is_last = k == len(steps) - 1
            ev = _event(
                rng, sid, container, etype, start + timedelta(seconds=offset), ports, hs_codes
            )
            # Draw every coin before acting, so the random sequence never depends on outcomes.
            coin_malformed = rng.random() < cfg.malformed_rate
            coin_bad_container = rng.random() < cfg.bad_container_rate
            coin_unknown = rng.random() < cfg.unknown_ref_rate and etype == DF
            coin_late = rng.random() < cfg.late_rate
            coin_duplicate = rng.random() < cfg.duplicate_rate
            fault: str | None = None
            if coin_malformed:
                fault, value = "malformed", _malformed(rng, ev)
            elif coin_bad_container:
                ev["container_id"] = _corrupt_check_digit(container)
                fault, value = "bad_container", _wire(ev)
            elif coin_unknown:
                ev["payload"] = {**ev["payload"], "port_code": UNKNOWN_PORT, "hs_code": UNKNOWN_HS}
                fault, value = "unknown_reference", _wire(ev)
            else:
                value = _wire(ev)
            # A message is clean, late or duplicated, never more than one. Rejects are neither.
            late = fault is None and coin_late and not is_last
            duplicate = fault is None and not late and coin_duplicate
            candidates.append(
                {
                    "send_at": offset,
                    "key": sid,
                    "value": value,
                    "ev": ev,
                    "fault": fault,
                    "late": late,
                    "duplicate": duplicate,
                    "last": is_last,
                }
            )
            offset += cfg.step_delay + rng.uniform(0, JITTER_S)

    # A late event is sent after the shipment's last event, but keeps its own occurred_at.
    last_send = {c["key"]: c["send_at"] for c in candidates if c["last"]}
    messages: list[Message] = []
    events: list[dict[str, Any]] = []
    for c in candidates:
        ev = c["ev"]
        send_at, fault = c["send_at"], c["fault"]
        if c["late"]:
            send_at, fault = last_send[c["key"]] + cfg.late_delay, "late"
        if fault in (None, "late"):
            events.append(
                {
                    k: ev[k]
                    for k in (
                        "event_id",
                        "shipment_id",
                        "container_id",
                        "event_type",
                        "occurred_at",
                    )
                }
            )
        messages.append(
            Message(send_at, c["key"], c["value"], ev["event_id"], ev["occurred_at"], fault)
        )
        if c["duplicate"]:
            messages.append(
                Message(
                    send_at + DUPLICATE_DELAY_S,
                    c["key"],
                    c["value"],
                    ev["event_id"],
                    ev["occurred_at"],
                    "duplicate",
                )
            )
    messages.sort(key=lambda m: m.send_at)  # stable, so equal times keep generation order
    return Plan(messages, events, shipments, illegal_shipments)


def expect(plan: Plan) -> dict[str, Any]:
    """The expected outcome, from the same fold the processor will run. No Kafka."""
    by_shipment: dict[str, list[dict[str, Any]]] = {}
    for e in plan.events:
        by_shipment.setdefault(e["shipment_id"], []).append(e)
    state_counts: Counter[str] = Counter({s.value: 0 for s in State})
    anomalies_total = 0
    # Only shipments with a stored event get a status document; one whose every message was
    # rejected never reaches the processor, so it is not counted (not even as UNKNOWN).
    for events in by_shipment.values():
        result = fold(events)
        state_counts[result.state.value] += 1
        anomalies_total += len(result.anomalies)
    rejected: Counter[str] = Counter({code: 0 for code in DLQ_CODES})
    for m in plan.messages:
        if m.fault in REJECT_REASON:
            rejected[REJECT_REASON[m.fault]] += 1
    return {
        "produced_total": len(plan.messages),
        "expected_stored": len(plan.events),
        "expected_duplicates": sum(m.fault == "duplicate" for m in plan.messages),
        "rejected_by_reason": {code: rejected[code] for code in DLQ_CODES},
        "state_counts": {s.value: state_counts[s.value] for s in State},
        "anomalies_total": anomalies_total,
        "expected_alerts": state_counts[State.ON_HOLD.value],
    }

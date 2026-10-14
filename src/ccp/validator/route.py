"""Pure routing for the validator (TRD §4, §10). No I/O: the service sends what this returns."""

import json
from dataclasses import dataclass
from datetime import datetime

from pydantic import ValidationError

from ccp.common import iso6346
from ccp.common.refdata import RefData
from ccp.common.schema import parse_event
from ccp.common.topics import DLQ, VALIDATED


@dataclass(frozen=True)
class Routed:
    topic: str
    value: bytes | None  # canonical JSON for `validated`; None for the DLQ
    reason: str | None  # DLQ reason code; None when valid
    detail: str


def route(raw: bytes | None, ref: RefData) -> Routed:
    """Runs the TRD §4 checks in order and stops at the first failure."""
    if not raw:
        return Routed(DLQ, None, "SCHEMA_INVALID", "empty message")
    try:
        ev = parse_event(raw)
    except ValidationError as e:
        first = e.errors()[0]
        where = ".".join(str(p) for p in first["loc"]) or "message"
        return Routed(DLQ, None, "SCHEMA_INVALID", f"{where}: {first['msg']}")
    if not iso6346.is_valid(ev.container_id):
        return Routed(DLQ, None, "CONTAINER_ID_INVALID", f"container_id={ev.container_id}")
    unknown = ref.unknown_in(ev.payload)
    if unknown:
        return Routed(DLQ, None, "UNKNOWN_REFERENCE_DATA", ", ".join(unknown))
    return Routed(VALIDATED, ev.model_dump_json().encode(), None, "")


def dlq_record(
    reason: str,
    detail: str,
    topic: str,
    partition: int,
    offset: int,
    raw: bytes | None,
    failed_at: datetime,
) -> bytes:
    """The DLQ message shape from TRD §10."""
    return json.dumps(
        {
            "reason": reason,
            "detail": detail,
            "source_topic": topic,
            "source_partition": partition,
            "source_offset": offset,
            "raw": (raw or b"").decode("utf-8", errors="replace"),
            "failed_at": failed_at.isoformat(),
        }
    ).encode()

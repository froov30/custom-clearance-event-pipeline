"""Event-time state fold (TRD §5).

Pure, with no I/O, so the processor and the simulator's --expect share it.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from ccp.common.schema import EventType


class State(StrEnum):
    UNKNOWN = "UNKNOWN"
    FILED = "FILED"
    DOCS_VERIFIED = "DOCS_VERIFIED"
    ON_HOLD = "ON_HOLD"
    CLEARED = "CLEARED"
    RELEASED = "RELEASED"
    REJECTED = "REJECTED"


# Same-instant tie-break: lifecycle order (D-015).
RANK = {t: i for i, t in enumerate(EventType, start=1)}

# event -> (allowed from-states, target state)
TRANSITIONS: dict[str, tuple[frozenset[str], State]] = {
    EventType.DECLARATION_FILED: (frozenset({State.UNKNOWN}), State.FILED),
    EventType.DOCUMENTS_VERIFIED: (frozenset({State.FILED}), State.DOCS_VERIFIED),
    EventType.INSPECTION_HOLD: (frozenset({State.DOCS_VERIFIED}), State.ON_HOLD),
    EventType.CLEARED: (frozenset({State.DOCS_VERIFIED, State.ON_HOLD}), State.CLEARED),
    EventType.RELEASED: (frozenset({State.CLEARED}), State.RELEASED),
    EventType.REJECTED: (frozenset({State.FILED, State.ON_HOLD}), State.REJECTED),
}


@dataclass(frozen=True)
class FoldResult:
    state: State = State.UNKNOWN
    state_since: datetime | None = None
    anomalies: list[dict[str, Any]] = field(default_factory=list)
    container_id: str | None = None


def sort_key(e: Mapping[str, Any]) -> tuple:
    return (e["occurred_at"], RANK[e["event_type"]], str(e["event_id"]))


def fold(events: Iterable[Mapping[str, Any]]) -> FoldResult:
    """Folds one shipment's events.

    Each event needs event_id, event_type, occurred_at (aware UTC) and container_id.
    """
    ordered = sorted(events, key=sort_key)
    state, since, anomalies = State.UNKNOWN, None, []
    for e in ordered:
        allowed, target = TRANSITIONS[e["event_type"]]
        if state in allowed:
            state, since = target, e["occurred_at"]
        else:
            anomalies.append(
                {
                    "type": "ILLEGAL_TRANSITION",
                    "event_id": e["event_id"],
                    "event_type": e["event_type"],
                    "from_state": state,
                    "occurred_at": e["occurred_at"],
                }
            )
    container_id = ordered[0]["container_id"] if ordered else None
    return FoldResult(state, since, anomalies, container_id)

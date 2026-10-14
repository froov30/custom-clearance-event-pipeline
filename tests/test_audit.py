from datetime import UTC, datetime, timedelta

from ccp.processor import audit
from ccp.processor.store import _status

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def ev(event_type, minute):
    return {
        "event_id": f"{minute:04d}",
        "event_type": event_type,
        "occurred_at": T0 + timedelta(minutes=minute),
        "container_id": "CSQU3054383",
    }


EVENTS = [ev("DECLARATION_FILED", 0), ev("DOCUMENTS_VERIFIED", 1), ev("CLEARED", 2)]


def test_a_status_folded_from_all_events_matches():
    assert audit.differences(_status(EVENTS, T0), EVENTS) == []


def test_a_stale_status_is_reported():
    stale = _status(EVENTS[:2], T0)  # folded before the last event arrived
    assert audit.differences(stale, EVENTS) == [
        "state: stored 'DOCS_VERIFIED', fold 'CLEARED'",
        f"state_since: stored {EVENTS[1]['occurred_at']!r}, fold {EVENTS[2]['occurred_at']!r}",
        "event_count: stored 2, fold 3",
    ]


def test_updated_at_is_not_compared():
    assert audit.differences(_status(EVENTS, T0 + timedelta(days=1)), EVENTS) == []


def test_events_without_a_status_are_reported():
    assert audit.differences(None, EVENTS) == ["no status document"]

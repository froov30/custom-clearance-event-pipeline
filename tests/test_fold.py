import itertools
from datetime import UTC, datetime, timedelta

import pytest

from ccp.common.fold import State, fold

T0 = datetime(2026, 1, 1, tzinfo=UTC)

DF, DV, IH, CL, RL, RJ = (
    "DECLARATION_FILED",
    "DOCUMENTS_VERIFIED",
    "INSPECTION_HOLD",
    "CLEARED",
    "RELEASED",
    "REJECTED",
)
EVENT_TYPES = [DF, DV, IH, CL, RL, RJ]

# TRD §5, written out independently of the implementation: (from_state, event) -> to_state.
LEGAL = {
    ("UNKNOWN", DF): "FILED",
    ("FILED", DV): "DOCS_VERIFIED",
    ("FILED", RJ): "REJECTED",
    ("DOCS_VERIFIED", IH): "ON_HOLD",
    ("DOCS_VERIFIED", CL): "CLEARED",
    ("ON_HOLD", CL): "CLEARED",
    ("ON_HOLD", RJ): "REJECTED",
    ("CLEARED", RL): "RELEASED",
}

# A legal history that reaches each state.
PATH_TO = {
    "UNKNOWN": [],
    "FILED": [DF],
    "DOCS_VERIFIED": [DF, DV],
    "ON_HOLD": [DF, DV, IH],
    "CLEARED": [DF, DV, CL],
    "RELEASED": [DF, DV, CL, RL],
    "REJECTED": [DF, RJ],
}


def ev(event_type, minute, event_id=None, container_id="CSQU3054383"):
    return {
        "event_id": event_id or f"{minute:04d}-{event_type}",
        "event_type": event_type,
        "occurred_at": T0 + timedelta(minutes=minute),
        "container_id": container_id,
    }


def history(types):
    return [ev(t, i) for i, t in enumerate(types)]


def test_state_names_match_trd():
    assert [s.value for s in State] == list(PATH_TO)


def test_empty_history_is_unknown():
    r = fold([])
    assert (r.state, r.state_since, r.anomalies, r.container_id) == ("UNKNOWN", None, [], None)


@pytest.mark.parametrize("from_state", list(PATH_TO))
@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_every_state_event_pair(from_state, event_type):
    events = history(PATH_TO[from_state]) + [ev(event_type, 100)]
    r = fold(events)
    to_state = LEGAL.get((from_state, event_type))
    if to_state:
        assert r.state == to_state
        assert r.state_since == T0 + timedelta(minutes=100)
        assert r.anomalies == []
    else:
        assert r.state == from_state
        assert r.anomalies == [
            {
                "type": "ILLEGAL_TRANSITION",
                "event_id": f"0100-{event_type}",
                "event_type": event_type,
                "from_state": from_state,
                "occurred_at": T0 + timedelta(minutes=100),
            }
        ]


@pytest.mark.parametrize("terminal", ["RELEASED", "REJECTED"])
def test_terminal_states_accept_nothing(terminal):
    events = history(PATH_TO[terminal]) + [ev(t, 100 + i) for i, t in enumerate(EVENT_TYPES)]
    r = fold(events)
    assert r.state == terminal
    assert len(r.anomalies) == len(EVENT_TYPES)


def test_state_since_is_time_of_last_legal_step():
    r = fold(history([DF, DV, IH]) + [ev(RL, 50)])  # RELEASED from ON_HOLD is illegal
    assert r.state == "ON_HOLD"
    assert r.state_since == T0 + timedelta(minutes=2)


def test_late_event_heals_anomaly():
    early = [ev(DF, 0), ev(CL, 10)]
    before = fold(early)
    assert before.state == "FILED"
    assert [a["event_type"] for a in before.anomalies] == [CL]

    after = fold(early + [ev(DV, 5)])  # arrives last, happened in between
    assert after.state == "CLEARED"
    assert after.state_since == T0 + timedelta(minutes=10)
    assert after.anomalies == []


def test_same_timestamp_ordered_by_lifecycle_rank():
    # Listed in reverse; rank puts DF before DV before CL.
    r = fold([ev(CL, 0), ev(DV, 0), ev(DF, 0)])
    assert r.state == "CLEARED"
    assert r.anomalies == []


def test_same_timestamp_and_type_ordered_by_event_id():
    r = fold([ev(DF, 0, event_id="b"), ev(DF, 0, event_id="a")])
    assert r.state == "FILED"
    assert [a["event_id"] for a in r.anomalies] == ["b"]


def test_container_id_comes_from_first_event_in_fold_order():
    r = fold([ev(DV, 5, container_id="LATER"), ev(DF, 0, container_id="FIRST")])
    assert r.container_id == "FIRST"


def test_input_is_not_mutated():
    events = [ev(CL, 2), ev(DF, 0), ev(DV, 1)]
    snapshot = [dict(e) for e in events]
    fold(events)
    assert events == snapshot


@pytest.mark.parametrize(
    "lifecycle",
    [
        [DF, DV, IH, CL, RL],  # hold path, released
        [DF, DV, IH, RJ],  # hold path, rejected
        [DF, DV, CL, RL, RJ],  # one illegal step after a terminal state
    ],
)
def test_every_arrival_order_gives_same_result(lifecycle):
    events = history(lifecycle)
    expected = fold(events)
    for perm in itertools.permutations(events):
        assert fold(list(perm)) == expected

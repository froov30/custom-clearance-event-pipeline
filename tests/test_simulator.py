from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from ccp.common import iso6346, refdata
from ccp.common.schema import parse_event
from ccp.simulator.generate import (
    UNKNOWN_HS,
    UNKNOWN_PORT,
    SimConfig,
    check_timing,
    expect,
    generate,
)

REF = refdata.load(Path(__file__).parents[1] / "data" / "ref")
START = datetime(2026, 1, 1, tzinfo=UTC)
REJECT_REASON = {
    "malformed": "SCHEMA_INVALID",
    "bad_container": "CONTAINER_ID_INVALID",
    "unknown_reference": "UNKNOWN_REFERENCE_DATA",
}
RATE_FOR = {
    "malformed": "malformed_rate",
    "bad_container": "bad_container_rate",
    "unknown_reference": "unknown_ref_rate",
}
ALL_RATES = dict(
    open_hold_rate=0.2,
    duplicate_rate=0.2,
    late_rate=0.2,
    malformed_rate=0.1,
    illegal_rate=0.2,
    bad_container_rate=0.1,
    unknown_ref_rate=0.1,
)


def cfg(**kw):
    return SimConfig(shipments=40, **kw)


def reason_of(value: bytes) -> str | None:
    """Checks a message with the real validation rules (TRD §4), not the generator's labels."""
    try:
        ev = parse_event(value)
    except ValidationError:
        return "SCHEMA_INVALID"
    if not iso6346.is_valid(ev.container_id):
        return "CONTAINER_ID_INVALID"
    if REF.unknown_in(ev.payload):
        return "UNKNOWN_REFERENCE_DATA"
    return None


def test_unknown_codes_really_are_unknown():
    assert UNKNOWN_PORT not in REF.ports
    assert UNKNOWN_HS not in REF.hs_codes


def test_same_seed_gives_identical_messages_and_manifest():
    a = generate(42, cfg(**ALL_RATES), START, REF)
    b = generate(42, cfg(**ALL_RATES), START, REF)
    assert [m.value for m in a.messages] == [m.value for m in b.messages]
    assert expect(a) == expect(b)


def test_different_seed_gives_different_messages():
    a = generate(1, cfg(**ALL_RATES), START, REF)
    b = generate(2, cfg(**ALL_RATES), START, REF)
    assert [m.value for m in a.messages] != [m.value for m in b.messages]


def test_clean_run_every_message_is_valid_and_one_per_event():
    plan = generate(7, cfg(), START, REF)
    assert plan.messages
    assert all(reason_of(m.value) is None for m in plan.messages)
    assert len(plan.messages) == len(plan.events)


@pytest.mark.parametrize("fault", list(REJECT_REASON))
def test_each_reject_fault_fails_the_real_check_with_its_reason(fault):
    plan = generate(3, cfg(**{RATE_FOR[fault]: 1.0}), START, REF)
    bad = [m for m in plan.messages if m.fault == fault]
    assert bad, "rate 1.0 must inject the fault"
    for m in bad:
        assert reason_of(m.value) == REJECT_REASON[fault]


def test_labels_agree_with_real_checks_on_a_mixed_run():
    plan = generate(11, cfg(**ALL_RATES), START, REF)
    for m in plan.messages:
        assert reason_of(m.value) == REJECT_REASON.get(m.fault)


def test_duplicates_are_byte_identical_resends_of_accepted_events():
    plan = generate(9, cfg(duplicate_rate=1.0), START, REF)
    dups = [m for m in plan.messages if m.fault == "duplicate"]
    assert len(dups) == len(plan.events)
    for d in dups:
        twins = [m for m in plan.messages if m.event_id == d.event_id]
        assert len(twins) == 2
        assert twins[0].value == twins[1].value


def test_late_events_are_sent_after_later_events_but_keep_their_time():
    plan = generate(5, cfg(late_rate=1.0), START, REF)
    late = [m for m in plan.messages if m.fault == "late"]
    assert late
    for m in late:
        overtaken = [
            x
            for x in plan.messages
            if x.key == m.key and x.occurred_at > m.occurred_at and x.send_at < m.send_at
        ]
        assert overtaken, "a late event must arrive after a later event of its shipment"


def test_illegal_sequences_stay_valid_but_create_anomalies():
    plan = generate(4, cfg(illegal_rate=1.0), START, REF)
    assert plan.illegal_shipments == len(plan.shipments)
    assert all(reason_of(m.value) is None for m in plan.messages)
    assert expect(plan)["anomalies_total"] > 0


def test_manifest_reconciles_and_alerts_match_final_on_hold_states():
    plan = generate(21, cfg(**ALL_RATES), START, REF)
    m = expect(plan)
    assert m["produced_total"] == (
        m["expected_stored"] + m["expected_duplicates"] + sum(m["rejected_by_reason"].values())
    )
    assert set(m["rejected_by_reason"]) == {
        "SCHEMA_INVALID",
        "CONTAINER_ID_INVALID",
        "UNKNOWN_REFERENCE_DATA",
        "PROCESSING_ERROR",
    }
    assert m["rejected_by_reason"]["PROCESSING_ERROR"] == 0
    assert m["expected_alerts"] == m["state_counts"]["ON_HOLD"]
    assert m["expected_alerts"] > 0
    assert sum(m["state_counts"].values()) == len(plan.shipments)


def test_timing_constraints_are_enforced():
    check_timing(SimConfig(), hold_alert_seconds=20)
    with pytest.raises(ValueError):
        check_timing(SimConfig(late_delay=20.0), hold_alert_seconds=20)
    with pytest.raises(ValueError):
        check_timing(SimConfig(step_delay=20.0), hold_alert_seconds=20)

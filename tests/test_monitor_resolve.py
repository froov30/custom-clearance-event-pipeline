from datetime import UTC, datetime

from ccp.monitor.core import stale_alerts

T = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)


def alert(aid, sid, since):
    return {"_id": aid, "shipment_id": sid, "state_since": since}


def status(state, since):
    return {"state": state, "state_since": since}


def test_a_hold_that_is_still_open_is_not_stale():
    assert stale_alerts([alert(1, "S", T)], {"S": status("ON_HOLD", T)}) == []


def test_an_alert_is_stale_once_its_shipment_leaves_on_hold():
    assert stale_alerts([alert(1, "S", T)], {"S": status("CLEARED", T)}) == [1]


def test_an_alert_is_stale_when_its_shipment_has_no_status():
    assert stale_alerts([alert(1, "S", T)], {}) == [1]


def test_an_alert_is_stale_when_a_later_event_moved_the_hold_start():
    later = datetime(2026, 5, 1, 12, 1, tzinfo=UTC)
    assert stale_alerts([alert(1, "S", T)], {"S": status("ON_HOLD", later)}) == [1]


def test_only_the_stale_ones_are_returned():
    open_alerts = [alert(1, "A", T), alert(2, "B", T)]
    statuses = {"A": status("ON_HOLD", T), "B": status("REJECTED", T)}
    assert stale_alerts(open_alerts, statuses) == [2]

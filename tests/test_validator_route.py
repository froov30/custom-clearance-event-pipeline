import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ccp.common import refdata
from ccp.common.schema import parse_event
from ccp.common.topics import DLQ, VALIDATED
from ccp.validator.route import dlq_record, route

REF = refdata.load(Path(__file__).parents[1] / "data" / "ref")


def event(**over):
    ev = {
        "event_id": "6f1c3a52-6a5e-4c55-9d3e-2f0d0b7c9a11",
        "shipment_id": "SHP-0001",
        "container_id": "CSQU3054383",
        "event_type": "DECLARATION_FILED",
        "occurred_at": "2026-10-11T08:00:00Z",
        "source": "declaration_system",
        "schema_version": 1,
        "payload": {"port_code": "NLRTM", "hs_code": "847130", "declarant": "ACME Imports"},
    }
    ev.update(over)
    return ev


def raw(ev) -> bytes:
    return json.dumps(ev).encode()


def without(key):
    return {k: v for k, v in event().items() if k != key}


def test_clean_event_goes_to_validated_as_canonical_json():
    r = route(raw(event(extra_field="dropped")), REF)
    assert (r.topic, r.reason) == (VALIDATED, None)
    assert parse_event(r.value) == parse_event(raw(event()))
    assert b"extra_field" not in r.value


def test_canonical_output_routes_to_itself():
    once = route(raw(event()), REF).value
    assert route(once, REF).value == once


@pytest.mark.parametrize(
    "value, reason",
    [
        (b"{not json", "SCHEMA_INVALID"),
        (None, "SCHEMA_INVALID"),
        (raw(without("source")), "SCHEMA_INVALID"),
        (raw(event(schema_version="1")), "SCHEMA_INVALID"),
        (raw(event(container_id="CSQU3054384")), "CONTAINER_ID_INVALID"),
        (raw(event(container_id="csqu3054383")), "CONTAINER_ID_INVALID"),
        (
            raw(event(payload={"port_code": "XXZZZ", "hs_code": "847130", "declarant": "A"})),
            "UNKNOWN_REFERENCE_DATA",
        ),
        (
            raw(event(payload={"port_code": "NLRTM", "hs_code": "999999", "declarant": "A"})),
            "UNKNOWN_REFERENCE_DATA",
        ),
    ],
)
def test_bad_events_go_to_dlq_with_their_reason(value, reason):
    r = route(value, REF)
    assert (r.topic, r.reason, r.value) == (DLQ, reason, None)
    assert r.detail


def test_first_failing_check_wins():
    bad_both = event(
        container_id="CSQU3054384",
        payload={"port_code": "XXZZZ", "hs_code": "847130", "declarant": "A"},
    )
    assert route(raw(bad_both), REF).reason == "CONTAINER_ID_INVALID"


def test_unknown_reference_detail_names_the_code():
    bad = event(payload={"port_code": "XXZZZ", "hs_code": "847130", "declarant": "A"})
    assert "port_code=XXZZZ" in route(raw(bad), REF).detail


def test_dlq_record_carries_source_coordinates_and_raw_text():
    rec = json.loads(
        dlq_record(
            "SCHEMA_INVALID",
            "bad",
            "customs.events.raw",
            3,
            41,
            b"\xff{oops",
            datetime(2026, 1, 1, tzinfo=UTC),
        )
    )
    assert rec == {
        "reason": "SCHEMA_INVALID",
        "detail": "bad",
        "source_topic": "customs.events.raw",
        "source_partition": 3,
        "source_offset": 41,
        "raw": "�{oops",
        "failed_at": "2026-01-01T00:00:00+00:00",
    }

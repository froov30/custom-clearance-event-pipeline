import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from ccp.common.schema import EventType, parse_event

BASE = {
    "event_id": "6f1c2a9e-0b7d-4c1e-9a52-3d8e4f6a7b10",
    "shipment_id": "SHP-000042",
    "container_id": "CSQU3054383",
    "event_type": "DECLARATION_FILED",
    "occurred_at": "2026-10-11T08:00:00Z",
    "source": "declaration_system",
    "schema_version": 1,
    "payload": {"port_code": "NLRTM", "hs_code": "847130", "declarant": "ACME Imports"},
}


def raw(**changes) -> bytes:
    d = {**BASE, **changes}
    return json.dumps({k: v for k, v in d.items() if v is not ...}).encode()


def test_valid_event_parses():
    e = parse_event(raw())
    assert e.event_type is EventType.DECLARATION_FILED
    assert e.payload["hs_code"] == "847130"


def test_offset_is_normalised_to_utc():
    e = parse_event(raw(occurred_at="2026-10-11T13:30:00+05:30"))
    assert e.occurred_at == datetime(2026, 10, 11, 8, 0, tzinfo=UTC)
    assert e.occurred_at.utcoffset().total_seconds() == 0


def test_canonical_json_round_trips():
    e = parse_event(raw())
    assert parse_event(e.model_dump_json().encode()) == e


def test_unknown_fields_are_ignored():
    e = parse_event(raw(extra_field="x", payload={**BASE["payload"], "note": "y"}))
    assert "note" not in e.payload


def test_payloadless_types_accept_empty_payload():
    assert parse_event(raw(event_type="CLEARED", payload={})).event_type is EventType.CLEARED


@pytest.mark.parametrize(
    "bad",
    [
        b"not json at all",
        b"",
        raw(container_id=...),  # missing field
        raw(event_type="TELEPORTED"),
        raw(schema_version=2),  # unsupported version
        raw(schema_version="1"),  # wrong type
        raw(occurred_at="2026-10-11T08:00:00"),  # naive timestamp
        raw(event_id="not-a-uuid"),
        raw(shipment_id=""),
        raw(source=""),
        raw(payload={"port_code": "NLRTM", "declarant": "ACME"}),  # missing hs_code
        raw(payload={**BASE["payload"], "hs_code": "84713"}),  # HS code must be 6 digits
        raw(payload={**BASE["payload"], "port_code": "rotterdam"}),  # not a UN/LOCODE shape
        raw(event_type="INSPECTION_HOLD", payload={"port_code": "NLRTM"}),  # missing reason
        raw(event_type="REJECTED", payload={}),  # missing reason
    ],
)
def test_schema_rejections(bad):
    with pytest.raises(ValidationError):
        parse_event(bad)

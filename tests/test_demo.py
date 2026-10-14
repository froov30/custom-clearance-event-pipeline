from ccp import demo

STATS = {
    "events_stored": 62,
    "duplicates_dropped": 1,
    "rejected_total": 6,
    "state_counts": {"ON_HOLD": 2},
    "anomalies_total": 1,
    "alerts_total": 2,
}
SUMMARY = {"SCHEMA_INVALID": 4, "CONTAINER_ID_INVALID": 2}


def test_actual_reads_the_api_into_the_manifest_shape(monkeypatch):
    pages = {"/stats": STATS, "/rejections/summary": SUMMARY}
    monkeypatch.setattr(demo, "get", lambda _url, path: pages[path])
    assert demo.actual("http://x") == {
        "produced_total": 69,  # M2: stored + duplicates + rejected
        "expected_stored": 62,
        "expected_duplicates": 1,
        "rejected_by_reason": SUMMARY,
        "state_counts": {"ON_HOLD": 2},
        "anomalies_total": 1,
        "expected_alerts": 2,
    }


def test_mismatches_name_every_differing_field():
    want = {"a": 1, "b": {"x": 2}, "c": 3}
    got = {"a": 1, "b": {"x": 3}}
    assert demo.mismatches(want, got) == [
        "b: expected {'x': 2}, got {'x': 3}",
        "c: expected 3, got None",
    ]
    assert demo.mismatches(want, dict(want)) == []

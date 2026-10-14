from ccp import demo

STATS = {
    "events_stored": 62,
    "duplicates_dropped": 1,
    "rejected_total": 6,
    "state_counts": {"ON_HOLD": 2},
    "anomalies_total": 1,
    "alerts_total": 5,  # raised, including ones that closed when processing caught up (D-064)
    "open_alerts": 2,
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
        "expected_alerts": 2,  # open alerts, which equal the shipments still ON_HOLD
    }


def test_verify_keeps_polling_through_an_api_error(monkeypatch):
    # The API can fail while MongoDB restarts (D-068); that must not end the check.
    answers = iter([OSError("connection reset"), {"a": 1}, {"a": 1}])

    def fake_actual(_url):
        value = next(answers)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(demo, "actual", fake_actual)
    monkeypatch.setattr(demo.time, "sleep", lambda _s: None)
    got, diff, _want, _n = demo.verify("http://x", {"a": 1}, 30, 0, lambda w: (w, 0))
    assert (got, diff) == ({"a": 1}, [])


def test_with_reemits_adds_the_unaccounted_messages_to_duplicates_and_to_produced():
    # The demo's "produced" is stored + duplicates + rejected, so each re-emit raises it too.
    want = {"produced_total": 21506, "expected_stored": 19213, "expected_duplicates": 907}
    adjusted, n = demo.with_reemits(want, validated=20122)
    assert n == 2
    assert adjusted == {
        "produced_total": 21508,
        "expected_stored": 19213,
        "expected_duplicates": 909,
    }
    assert want["expected_duplicates"] == 907  # the manifest itself is not changed


def test_reemits_are_the_validated_messages_the_manifest_does_not_account_for():
    # 20,122 messages reached `validated`; the manifest stores 19,213 and drops 907 duplicates.
    assert demo.reemitted(validated=20122, expected_stored=19213, expected_duplicates=907) == 2


def test_no_reemits_when_validated_matches_the_manifest():
    assert demo.reemitted(validated=20120, expected_stored=19213, expected_duplicates=907) == 0


def test_unknown_flags_are_kept_for_the_simulator():
    args = demo.parse_args(["--seed", "7", "--verify-only", "--shipments", "50"])
    assert (args.seed, args.verify_only, args.sim_args) == (7, True, ["--shipments", "50"])


def test_expected_passes_simulator_flags_through():
    # Every shipment ends in exactly one state, so the state counts add up to --shipments.
    assert sum(demo.expected(42, ["--shipments", "5"])["state_counts"].values()) == 5


def test_mismatches_name_every_differing_field():
    want = {"a": 1, "b": {"x": 2}, "c": 3}
    got = {"a": 1, "b": {"x": 3}}
    assert demo.mismatches(want, got) == [
        "b: expected {'x': 2}, got {'x': 3}",
        "c: expected 3, got None",
    ]
    assert demo.mismatches(want, dict(want)) == []

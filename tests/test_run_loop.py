from itertools import islice

import pytest

from ccp.common.kafka import MAX_ATTEMPTS, Stop, Transient, backoff, process


def never(msg, exc):
    raise AssertionError("on_poison must not be called")


def test_backoff_doubles_from_half_a_second_and_caps_at_30():
    assert list(islice(backoff(), 8)) == [0.5, 1, 2, 4, 8, 16, 30, 30]


def test_success_needs_one_call_and_no_sleep(tmp_path):
    calls, slept = [], []
    assert process("m", calls.append, never, tmp_path / "hb", Stop(), sleep=slept.append)
    assert (calls, slept) == (["m"], [])


def test_transient_errors_back_off_and_keep_the_heartbeat_fresh(tmp_path):
    outcomes = iter([Transient("down")] * 4 + [None])

    def handle(_msg):
        err = next(outcomes)
        if err:
            raise err

    slept = []
    beat = tmp_path / "hb"
    assert process("m", handle, never, beat, Stop(), sleep=slept.append)
    assert sum(slept) == pytest.approx(0.5 + 1 + 2 + 4)
    assert max(slept) <= 0.5  # slept in slices, so a stop request is noticed quickly
    assert beat.exists()


def test_unexpected_error_goes_to_the_dlq_after_max_attempts(tmp_path):
    calls, poisoned = [], []

    def handle(msg):
        calls.append(msg)
        raise ValueError("boom")

    ok = process("m", handle, lambda m, e: poisoned.append(e), tmp_path / "hb", Stop(), sleep=None)
    assert ok
    assert len(calls) == MAX_ATTEMPTS == 5
    assert isinstance(poisoned[0], ValueError)


def test_a_transient_error_while_sending_to_the_dlq_is_retried(tmp_path):
    sends = iter([Transient("dlq down"), None])

    def on_poison(_m, _e):
        err = next(sends)
        if err:
            raise err

    def handle(_msg):
        raise ValueError("boom")

    slept = []
    assert process("m", handle, on_poison, tmp_path / "hb", Stop(), sleep=slept.append)
    assert sum(slept) == pytest.approx(0.5)


def test_stop_during_backoff_returns_false_so_nothing_is_committed(tmp_path):
    stop = Stop()

    def handle(_msg):
        stop.requested = True
        raise Transient("down")

    assert process("m", handle, never, tmp_path / "hb", stop, sleep=lambda s: None) is False

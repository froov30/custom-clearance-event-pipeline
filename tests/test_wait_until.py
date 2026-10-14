import pytest
from pymongo.errors import ConnectionFailure

from ccp.common.kafka import Stop, wait_until

TRANSIENT = (ConnectionFailure,)


def test_retries_transient_errors_with_backoff_then_returns_true(tmp_path):
    outcomes = iter([ConnectionFailure("down")] * 3 + [None])

    def fn():
        err = next(outcomes)
        if err:
            raise err

    slept = []
    beat = tmp_path / "hb"
    assert wait_until(fn, TRANSIENT, beat, Stop(), sleep=slept.append)
    assert sum(slept) == pytest.approx(0.5 + 1 + 2)
    assert beat.exists()  # the heartbeat was touched while waiting


def test_other_errors_propagate_instead_of_being_retried(tmp_path):
    def fn():
        raise ValueError("a bug, not an outage")

    with pytest.raises(ValueError):
        wait_until(fn, TRANSIENT, tmp_path / "hb", Stop(), sleep=lambda s: None)


def test_returns_false_when_stopped_while_waiting(tmp_path):
    stop = Stop()

    def fn():
        stop.requested = True
        raise ConnectionFailure("down")

    assert wait_until(fn, TRANSIENT, tmp_path / "hb", stop, sleep=lambda s: None) is False


def test_does_not_call_fn_once_already_stopped(tmp_path):
    stop = Stop()
    stop.requested = True
    assert (
        wait_until(lambda: pytest.fail("must not run"), TRANSIENT, tmp_path / "hb", stop) is False
    )

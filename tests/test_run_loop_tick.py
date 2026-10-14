import logging

from confluent_kafka import TopicPartition

from ccp.common.kafka import Stop, Transient, _log_partitions, run


def test_rebalance_callback_logs_the_partitions(caplog):
    with caplog.at_level(logging.INFO, logger="ccp.common.kafka"):
        _log_partitions("assigned")(None, [TopicPartition("t", 1), TopicPartition("t", 0)])
    record = caplog.records[-1]
    assert record.getMessage() == "partitions assigned"
    assert record.fields == {"partitions": ["t[0]", "t[1]"]}


class FakeConsumer:
    """Just enough of a consumer for the loop: never delivers a message."""

    def __init__(self):
        self.closed = False

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        self.topics = topics
        self.callbacks = (on_assign, on_revoke)

    def poll(self, timeout):
        return None

    def close(self):
        self.closed = True


def never(*_):
    raise AssertionError("no message was delivered")


def test_tick_runs_every_iteration_and_its_errors_never_stop_the_loop(tmp_path):
    stop, calls = Stop(), []

    def tick():
        calls.append(1)
        if len(calls) == 1:
            raise Transient("MongoDB down")  # logged, retried next iteration
        if len(calls) == 2:
            raise ValueError("a bug in the scan")  # logged with traceback, loop survives
        if len(calls) == 4:
            stop.requested = True

    cons = FakeConsumer()
    run(cons, ["t"], never, never, tmp_path / "hb", stop, tick=tick)
    assert len(calls) == 4
    assert cons.closed
    assert (tmp_path / "hb").exists()

"""Kafka client factories and the shared consume loop (TRD §8, §10)."""

import logging
import signal
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 5  # D-031: an unexpected error is retried this many times, then sent to the DLQ
_SLICE_S = (
    0.5  # backoff sleeps in slices this long, so the heartbeat stays fresh and a stop is seen
)


def producer(bootstrap: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})


def consumer(bootstrap: str, group_id: str) -> Consumer:
    # Offsets are committed by hand, only after the write that handles the message succeeds.
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )


class Transient(Exception):
    """Infrastructure is down: retry with backoff, never send to the DLQ (TRD §10)."""


def deliver(prod: Producer, topic: str, key: bytes | None, value: bytes) -> None:
    """Produces one message and waits for its delivery report. Any failure is Transient."""
    errors: list = []
    try:
        prod.produce(topic, key=key, value=value, on_delivery=lambda err, _m: errors.append(err))
        # shortcut: one flush per message is slow; batch the produce and commit per batch
        # if throughput ever matters (TRD §8).
        unsent = prod.flush(10)
    except (BufferError, KafkaException) as e:
        raise Transient(f"produce to {topic} failed: {e}") from e
    if unsent or errors != [None]:
        raise Transient(f"delivery to {topic} failed: {errors or 'timed out'}")


class Stop:
    """Set by a signal; checked between messages and during backoff."""

    def __init__(self) -> None:
        self.requested = False


def install_signal_handlers(stop: Stop) -> None:
    def _handler(signum, _frame) -> None:
        log.info("stop requested", extra={"fields": {"signal": signum}})
        stop.requested = True

    # SIGINT is Ctrl+C. SIGBREAK (Ctrl+Break) is how a Windows process group is asked to stop.
    for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            signal.signal(sig, _handler)


def heartbeat_path(heartbeat_dir: str, service: str) -> Path:
    path = Path(heartbeat_dir) / f"{service}.heartbeat"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def backoff() -> Iterator[float]:
    delay = 0.5
    while True:
        yield delay
        delay = min(delay * 2, 30)


def _wait(seconds: float, beat: Path, stop: Stop, sleep: Callable[[float], Any]) -> bool:
    """Sleeps in slices, touching the heartbeat; returns False as soon as a stop is requested."""
    remaining = seconds
    while remaining > 0:
        if stop.requested:
            return False
        beat.touch()
        step = min(_SLICE_S, remaining)
        sleep(step)
        remaining -= step
    return not stop.requested


def wait_until(
    fn: Callable[[], Any],
    transient: tuple[type[Exception], ...],
    beat: Path,
    stop: Stop,
    sleep: Callable[[float], Any] = time.sleep,
) -> bool:
    """Calls fn() until it succeeds. Errors in `transient` back off, keeping the heartbeat fresh.

    Other errors propagate. Returns False if a stop was requested first.
    """
    delays = backoff()
    while not stop.requested:
        try:
            fn()
            return True
        except transient as e:
            log.warning("not ready, backing off", extra={"fields": {"error": str(e)}})
            if not _wait(next(delays), beat, stop, sleep):
                return False
    return False


def process(
    msg: Any,
    handle: Callable[[Any], None],
    on_poison: Callable[[Any, Exception], None],
    beat: Path,
    stop: Stop,
    sleep: Callable[[float], Any] = time.sleep,
) -> bool:
    """Handles one message. True: commit its offset. False: a stop cut a retry short."""
    delays = backoff()
    failures, last_error = 0, None
    while True:
        try:
            if failures >= MAX_ATTEMPTS:
                on_poison(msg, last_error)
            else:
                handle(msg)
            return True
        except Transient as e:
            log.warning("transient error, backing off", extra={"fields": {"error": str(e)}})
            if not _wait(next(delays), beat, stop, sleep):
                return False
        except Exception as e:
            if failures >= MAX_ATTEMPTS:
                raise  # sending to the DLQ failed for a non-transient reason: nothing safe is left
            failures, last_error = failures + 1, e
            log.exception("unexpected error", extra={"fields": {"attempt": failures}})


def _log_partitions(event: str) -> Callable[[Any, list], None]:
    """Rebalance callback that logs which partitions this consumer gained or lost (P11 evidence)."""

    def callback(_consumer, partitions) -> None:
        owned = sorted(f"{p.topic}[{p.partition}]" for p in partitions)
        log.info(f"partitions {event}", extra={"fields": {"partitions": owned}})

    return callback


def _safe_tick(tick: Callable[[], None]) -> None:
    """Periodic work never stops the consumer loop; a failure is logged and retried later."""
    try:
        tick()
    except Transient as e:
        log.warning("periodic task deferred", extra={"fields": {"error": str(e)}})
    except Exception:
        log.exception("periodic task failed")


def run(
    cons: Consumer,
    topics: list[str],
    handle: Callable[[Any], None],
    on_poison: Callable[[Any, Exception], None],
    beat: Path,
    stop: Stop,
    tick: Callable[[], None] | None = None,
) -> None:
    """Poll, handle, then commit synchronously only after success. Always closes the consumer.

    `tick`, if given, runs once per loop iteration (about every second) for periodic work.
    """
    cons.subscribe(
        topics, on_assign=_log_partitions("assigned"), on_revoke=_log_partitions("revoked")
    )
    try:
        while not stop.requested:
            beat.touch()
            msg = cons.poll(1.0)
            if tick is not None:
                _safe_tick(tick)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer error", extra={"fields": {"error": str(msg.error())}})
                continue
            if not process(msg, handle, on_poison, beat, stop):
                break  # stopped mid-retry: no commit, so the message is redelivered after a restart
            try:
                cons.commit(message=msg, asynchronous=False)
            except KafkaException as e:
                # Already handled; it may be handled again after a restart (at-least-once).
                log.warning("commit failed", extra={"fields": {"error": str(e)}})
    finally:
        cons.close()
        log.info("consumer closed")

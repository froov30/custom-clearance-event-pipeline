"""Processor: customs.events.validated -> MongoDB (TRD §6, §8). Run: python -m ccp.processor"""

import logging
from collections import Counter
from datetime import UTC, datetime

from pydantic import ValidationError

from ccp.common import mongo
from ccp.common.config import Config
from ccp.common.kafka import (
    Stop,
    Transient,
    consumer,
    deliver,
    heartbeat_path,
    install_signal_handlers,
    producer,
    run,
    wait_until,
)
from ccp.common.log import setup_logging
from ccp.common.topics import DLQ, VALIDATED
from ccp.processor.store import store
from ccp.validator.route import dlq_record

GROUP = "ccp-processor"
log = logging.getLogger(__name__)


def main() -> None:
    cfg = Config.from_env()
    setup_logging("processor", cfg.log_level)
    db = mongo.database(cfg)
    prod = producer(cfg.kafka_bootstrap_servers)
    counts: Counter[str] = Counter()

    def to_dlq(msg, reason: str, detail: str) -> None:
        record = dlq_record(
            reason,
            detail,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.value(),
            datetime.now(UTC),
        )
        deliver(prod, DLQ, msg.key(), record)
        counts[reason] += 1

    def handle(msg) -> None:
        try:
            source = (msg.topic(), msg.partition(), msg.offset())
            duplicate = store(db, msg.value(), datetime.now(UTC), source)
        except mongo.TRANSIENT as e:
            raise Transient(f"MongoDB unavailable: {e}") from e
        except ValidationError as e:
            # Should not happen: the validator only emits valid events (TRD §9).
            to_dlq(msg, "SCHEMA_INVALID", str(e.errors()[0]["msg"]))
            return
        counts["duplicate" if duplicate else "stored"] += 1

    def on_poison(msg, exc: Exception) -> None:
        to_dlq(msg, "PROCESSING_ERROR", repr(exc)[:500])

    stop = Stop()
    install_signal_handlers(stop)
    beat = heartbeat_path(cfg.heartbeat_dir, "processor")
    # Wait for MongoDB instead of exiting, so a start-up race never crash-loops the pod.
    if not wait_until(lambda: mongo.ensure_indexes(db), mongo.TRANSIENT, beat, stop):
        return
    group = GROUP + cfg.group_suffix
    log.info("started", extra={"fields": {"group": group, "db": cfg.mongo_db}})
    run(
        consumer(cfg.kafka_bootstrap_servers, group),
        [VALIDATED],
        handle,
        on_poison,
        beat,
        stop,
    )
    prod.flush(10)
    log.info("stopped", extra={"fields": {"counts": dict(counts)}})


if __name__ == "__main__":
    main()

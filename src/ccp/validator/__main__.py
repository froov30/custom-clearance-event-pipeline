"""Validator: customs.events.raw -> validated or dlq (TRD §4, §10). Run: python -m ccp.validator"""

import logging
from collections import Counter
from datetime import UTC, datetime

from ccp.common import refdata
from ccp.common.config import Config
from ccp.common.kafka import (
    Stop,
    consumer,
    deliver,
    heartbeat_path,
    install_signal_handlers,
    producer,
    run,
)
from ccp.common.log import setup_logging
from ccp.common.topics import DLQ, RAW, VALIDATED
from ccp.validator.route import dlq_record, route

GROUP = "ccp-validator"
log = logging.getLogger(__name__)


def main() -> None:
    cfg = Config.from_env()
    setup_logging("validator", cfg.log_level)
    ref = refdata.load(cfg.ref_data_dir)
    prod = producer(cfg.kafka_bootstrap_servers)
    routed: Counter[str] = Counter()

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
        routed[reason] += 1

    def handle(msg) -> None:
        r = route(msg.value(), ref)
        if r.reason is None:
            deliver(prod, VALIDATED, msg.key(), r.value)
            routed["validated"] += 1
        else:
            to_dlq(msg, r.reason, r.detail)

    def on_poison(msg, exc: Exception) -> None:
        to_dlq(msg, "PROCESSING_ERROR", repr(exc)[:500])

    stop = Stop()
    install_signal_handlers(stop)
    log.info("started", extra={"fields": {"group": GROUP, "topic": RAW}})
    run(
        consumer(cfg.kafka_bootstrap_servers, GROUP + cfg.group_suffix),
        [RAW],
        handle,
        on_poison,
        heartbeat_path(cfg.heartbeat_dir, "validator"),
        stop,
    )
    prod.flush(10)
    log.info("stopped", extra={"fields": {"routed": dict(routed)}})


if __name__ == "__main__":
    main()

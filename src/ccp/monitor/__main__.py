"""Monitor: hold alerts and DLQ -> rejections (TRD §12). Run: python -m ccp.monitor"""

import logging
import time
from collections import Counter
from datetime import UTC, datetime

from ccp.common import mongo
from ccp.common.config import Config
from ccp.common.kafka import (
    Stop,
    Transient,
    consumer,
    heartbeat_path,
    install_signal_handlers,
    run,
    wait_until,
)
from ccp.common.log import setup_logging
from ccp.common.topics import DLQ
from ccp.monitor.core import resolve_alerts, scan_holds, store_rejection

GROUP = "ccp-monitor-dlq"
log = logging.getLogger(__name__)


def main() -> None:
    cfg = Config.from_env()
    setup_logging("monitor", cfg.log_level)
    db = mongo.database(cfg)
    stop = Stop()
    install_signal_handlers(stop)
    beat = heartbeat_path(cfg.heartbeat_dir, "monitor")
    if not wait_until(lambda: mongo.ensure_indexes(db), mongo.TRANSIENT, beat, stop):
        return
    counts: Counter[str] = Counter()

    def handle(msg) -> None:
        try:
            store_rejection(db, msg.value())
        except mongo.TRANSIENT as e:
            raise Transient(f"MongoDB unavailable: {e}") from e
        counts["rejections"] += 1

    def on_poison(msg, exc: Exception) -> None:
        # The DLQ is the end of the line: record the unreadable message in the log and move on,
        # so it can never block the partition.
        fields = {"partition": msg.partition(), "offset": msg.offset(), "error": repr(exc)[:300]}
        log.error("unreadable DLQ record skipped", extra={"fields": fields})
        counts["skipped"] += 1

    last_scan = 0.0

    def tick() -> None:
        nonlocal last_scan
        if time.monotonic() - last_scan < cfg.monitor_scan_seconds:
            return
        last_scan = time.monotonic()
        now = datetime.now(UTC)
        try:
            new = scan_holds(db, now, cfg.hold_alert_seconds)
            closed = resolve_alerts(db, now)
        except mongo.TRANSIENT as e:
            raise Transient(f"MongoDB unavailable: {e}") from e
        if new or closed:
            counts["alerts"] += new
            counts["resolved"] += closed
            log.info("alerts scanned", extra={"fields": {"raised": new, "resolved": closed}})

    group = GROUP + cfg.group_suffix
    log.info("started", extra={"fields": {"group": group, "db": cfg.mongo_db}})
    run(consumer(cfg.kafka_bootstrap_servers, group), [DLQ], handle, on_poison, beat, stop, tick)
    log.info("stopped", extra={"fields": {"counts": dict(counts)}})


if __name__ == "__main__":
    main()

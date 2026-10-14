"""CLI: python -m ccp.simulator [--seed N] [--expect] (TRD §11).

Without --expect it produces to customs.events.raw in real time.
never connects to Kafka, so two runs with the same seed print the same output.
"""

import argparse
import json
import logging
import sys
import time
from collections import Counter
from datetime import UTC, datetime

from ccp.common import refdata
from ccp.common.config import Config
from ccp.common.kafka import producer
from ccp.common.log import setup_logging
from ccp.common.topics import RAW
from ccp.simulator.generate import Plan, SimConfig, check_timing, expect, generate

log = logging.getLogger(__name__)
# --expect ignores the clock, so a fixed start keeps its output identical from run to run.
EXPECT_START = datetime(2026, 1, 1, tzinfo=UTC)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="ccp.simulator", description="Synthetic clearance events (synthetic data)."
    )
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--expect", action="store_true", help="print the expected-outcome manifest; no Kafka"
    )
    p.add_argument("--shipments", type=int, default=20)
    p.add_argument("--step-delay", type=float, default=1.0, help="seconds between lifecycle steps")
    p.add_argument(
        "--late-delay", type=float, default=3.0, help="seconds a late event is held back"
    )
    p.add_argument(
        "--open-hold-rate", type=float, default=0.1, help="share of shipments left on hold"
    )
    p.add_argument("--duplicate-rate", type=float, default=0.05)
    p.add_argument("--late-rate", type=float, default=0.05)
    p.add_argument("--malformed-rate", type=float, default=0.03)
    p.add_argument(
        "--illegal-rate",
        type=float,
        default=0.05,
        help="share of shipments with an illegal sequence",
    )
    p.add_argument("--bad-container-rate", type=float, default=0.03)
    p.add_argument("--unknown-ref-rate", type=float, default=0.03)
    return p.parse_args(argv)


def sim_config(args: argparse.Namespace) -> SimConfig:
    return SimConfig(
        shipments=args.shipments,
        step_delay=args.step_delay,
        late_delay=args.late_delay,
        open_hold_rate=args.open_hold_rate,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        malformed_rate=args.malformed_rate,
        illegal_rate=args.illegal_rate,
        bad_container_rate=args.bad_container_rate,
        unknown_ref_rate=args.unknown_ref_rate,
    )


def produce(plan: Plan, cfg: Config) -> int:
    p = producer(cfg.kafka_bootstrap_servers)
    failures: list[str] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            failures.append(str(err))

    t0 = time.monotonic()
    for m in plan.messages:
        wait = t0 + m.send_at - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        p.produce(RAW, key=m.key.encode(), value=m.value, on_delivery=on_delivery)
        p.poll(0)
    unsent = p.flush(30)
    if unsent or failures:
        log.error("produce failed", extra={"fields": {"unsent": unsent, "errors": failures[:5]}})
        return 1
    faults = Counter(m.fault or "clean" for m in plan.messages)
    log.info(
        "produced",
        extra={"fields": {"topic": RAW, "messages": len(plan.messages), "faults": dict(faults)}},
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = Config.from_env()
    sim = sim_config(args)
    check_timing(sim, cfg.hold_alert_seconds)
    ref = refdata.load(cfg.ref_data_dir)
    if args.expect:
        plan = generate(args.seed, sim, EXPECT_START, ref)
        print(json.dumps(expect(plan), indent=2, sort_keys=True))
        return 0
    setup_logging("simulator", cfg.log_level)
    plan = generate(args.seed, sim, datetime.now(UTC), ref)
    return produce(plan, cfg)


if __name__ == "__main__":
    sys.exit(main())

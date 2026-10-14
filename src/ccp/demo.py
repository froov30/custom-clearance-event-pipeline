"""One-command, self-verifying demo (TRD §15, P9). All data is synthetic.

    python -m ccp.demo --seed 42
        Dev mode. Resets the `ccp_demo` database, starts validator, processor, monitor and api as
        subprocesses on their own `-demo` consumer groups (moved to the end of each topic first, so
        older messages are ignored), runs the simulator, then verifies the API against `--expect`.

    python -m ccp.demo --seed 42 --verify-only [--api-url URL]
        The services and simulator run elsewhere (Compose app profile, kind); only verify.

Any other flag (e.g. --shipments 2000 --step-delay 2) goes to the simulator, for both producing and
--expect, so a larger run is verified against its own manifest.

Exits 0 only if every expected value matches. Exit code 1 means a mismatch, 2 a setup failure.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

from confluent_kafka import Consumer, KafkaException, TopicPartition

from ccp.common import mongo
from ccp.common.config import Config
from ccp.common.topics import DLQ, PARTITIONS, RAW, VALIDATED

DEMO_DB = "ccp_demo"
DEMO_SUFFIX = "-demo"
SERVICES = ["validator", "processor", "monitor", "api"]
GROUP_TOPICS = {"ccp-validator": RAW, "ccp-processor": VALIDATED, "ccp-monitor-dlq": DLQ}
LOG_DIR = Path(".run") / "demo"


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="ccp.demo", description="Verified end-to-end demo.")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--verify-only", action="store_true", help="do not start services or produce")
    p.add_argument("--api-url", default=None, help="default: http://API_HOST:API_PORT")
    p.add_argument("--timeout", type=float, default=180, help="seconds to wait for a match")
    p.add_argument(
        "--validated-baseline",
        type=int,
        default=0,
        help="messages already on `validated` before this run (or use a fresh stack)",
    )
    args, args.sim_args = p.parse_known_args(argv)
    return args


def expected(seed: int, sim_args: Sequence[str] = ()) -> dict[str, Any]:
    """The simulator's own manifest for this seed, from the same CLI that produces the events."""
    out = subprocess.run(
        [sys.executable, "-m", "ccp.simulator", "--seed", str(seed), *sim_args, "--expect"],
        capture_output=True,
        check=True,
    )
    return json.loads(out.stdout)


def get(api_url: str, path: str) -> Any:
    with urlopen(f"{api_url}{path}", timeout=10) as r:
        return json.load(r)


def actual(api_url: str) -> dict[str, Any]:
    """The pipeline's results, read through the API, in the manifest's shape."""
    s = get(api_url, "/stats")
    return {
        # M2: everything produced was stored, dropped as a duplicate, or rejected.
        "produced_total": s["events_stored"] + s["duplicates_dropped"] + s["rejected_total"],
        "expected_stored": s["events_stored"],
        "expected_duplicates": s["duplicates_dropped"],
        "rejected_by_reason": get(api_url, "/rejections/summary"),
        "state_counts": s["state_counts"],
        "anomalies_total": s["anomalies_total"],
        # Open alerts, not every alert raised: a false one from processing lag closes (D-064).
        "expected_alerts": s["open_alerts"],
    }


def validated_count(bootstrap: str) -> int:
    """Messages ever produced to `validated`: the sum of its partitions' end offsets."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "ccp-demo-count"})
    try:
        ends = (
            c.get_watermark_offsets(TopicPartition(VALIDATED, p), timeout=15)[1]
            for p in range(PARTITIONS[VALIDATED])
        )
        return sum(ends)
    finally:
        c.close()


def with_reemits(want: dict[str, Any], validated: int) -> tuple[dict[str, Any], int]:
    """The manifest with its expected duplicates raised by the validator re-emits; returns both."""
    n = reemitted(validated, want["expected_stored"], want["expected_duplicates"])
    # A re-emit is one more message handled: "produced" (stored + duplicates + rejected) rises too.
    raised = {k: want[k] + n for k in ("produced_total", "expected_duplicates") if k in want}
    return {**want, **raised}, n


def reemitted(validated: int, expected_stored: int, expected_duplicates: int) -> int:
    """Messages on `validated` that the manifest does not account for (D-067).

    A validator that crashes after producing re-sends the event as a new message. The processor
    counts it as a duplicate, which is correct, so the expected duplicate count must include it.
    A negative value means fewer messages than the manifest expects; the stored counts show it.
    """
    return validated - (expected_stored + expected_duplicates)


def mismatches(want: dict[str, Any], got: dict[str, Any]) -> list[str]:
    return [f"{k}: expected {want[k]}, got {got.get(k)}" for k in want if got.get(k) != want[k]]


Adjust = Callable[[dict[str, Any]], tuple[dict[str, Any], int]]


def check(api_url: str, want: dict[str, Any], adjust: Adjust):
    """One comparison, with the expectation re-derived from `validated` at this moment.

    `validated` keeps growing while the validator is still routing the backlog, so the count is
    read on every check. An API or Kafka error is a difference, not a crash (D-068).
    """
    try:
        adjusted, n = adjust(want)
        got = actual(api_url)
    except (OSError, KafkaException) as e:
        return {}, [f"unreachable: {e}"], want, 0
    return got, mismatches(adjusted, got), adjusted, n


def verify(api_url: str, want: dict[str, Any], timeout: float, grace: float, adjust: Adjust):
    """Polls until everything matches (or times out), then waits `grace` and checks again,
    so a late, wrong alert or a late validator re-send is still caught (TRD §11)."""
    deadline = time.monotonic() + timeout
    while True:
        got, diff, adjusted, n = check(api_url, want, adjust)
        if not diff or time.monotonic() > deadline:
            break
        time.sleep(2)
    if diff:
        return got, diff, adjusted, n
    print(f"matched; re-checking after {grace:.0f}s for late alerts ...", flush=True)
    time.sleep(grace)
    return check(api_url, want, adjust)


def start_groups_at_end(bootstrap: str) -> None:
    """Commits each demo group's offsets at the end of its topic, so older messages are skipped."""
    for group, topic in GROUP_TOPICS.items():
        settings = {"bootstrap.servers": bootstrap, "group.id": group + DEMO_SUFFIX}
        c = Consumer({**settings, "enable.auto.commit": False})
        try:
            ends = []
            for p in range(PARTITIONS[topic]):
                high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=15)[1]
                ends.append(TopicPartition(topic, p, high))
            c.commit(offsets=ends, asynchronous=False)
        finally:
            c.close()


def spawn(module: str, env: dict[str, str]) -> subprocess.Popen:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    out = (LOG_DIR / f"{module}.log").open("ab")
    return subprocess.Popen(
        [sys.executable, "-m", f"ccp.{module}"],
        env=env,
        stdout=out,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )


def stop(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def wait_for_api(api_url: str, timeout: float = 60) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if get(api_url, "/health").get("mongo") == "ok":
                return True
        except (URLError, OSError):
            pass
        time.sleep(1)
    return False


def report(want: dict[str, Any], got: dict[str, Any], diff: list[str], reemits: int = 0) -> None:
    rows = [
        ("produced (= stored + duplicates + rejected)", "produced_total"),
        ("events stored", "expected_stored"),
        ("duplicates dropped", "expected_duplicates"),
        ("anomalies", "anomalies_total"),
        ("open hold alerts", "expected_alerts"),
    ]
    print("\nCustoms Clearance Event Pipeline: demo summary (synthetic data)")
    print(f"{'':44} {'expected':>9} {'actual':>9}")
    for label, key in rows:
        print(f"{label:44} {want[key]:>9} {got.get(key, '-'):>9}")
    for title, key in (("DLQ", "rejected_by_reason"), ("state", "state_counts")):
        for name, n in want[key].items():
            print(f"{title + ' ' + name:44} {n:>9} {got.get(key, {}).get(name, '-'):>9}")
    if reemits:
        print(f"validator re-emits: {reemits} (included in produced and duplicates above)")
    print("\nRESULT: " + ("MATCH" if not diff else "MISMATCH"))
    for line in diff:
        print("  - " + line)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = Config.from_env()
    api_url = args.api_url or f"http://{cfg.api_host}:{cfg.api_port}"
    grace = cfg.hold_alert_seconds + 2 * cfg.monitor_scan_seconds
    want = expected(args.seed, args.sim_args)

    def adjust_for(baseline: int) -> Adjust:
        return lambda w: with_reemits(w, validated_count(cfg.kafka_bootstrap_servers) - baseline)

    if args.verify_only:
        got, diff, adjusted, n = verify(
            api_url, want, args.timeout, grace, adjust_for(args.validated_baseline)
        )
        report(adjusted, got, diff, n)
        return 0 if not diff else 1

    client = mongo.client(cfg)
    client.drop_database(DEMO_DB)  # the demo owns this database and nothing else
    client.close()
    baseline = validated_count(cfg.kafka_bootstrap_servers)
    start_groups_at_end(cfg.kafka_bootstrap_servers)
    env = {**os.environ, "MONGO_DB": DEMO_DB, "CCP_GROUP_SUFFIX": DEMO_SUFFIX}
    procs = {name: spawn(name, env) for name in SERVICES}
    try:
        if not wait_for_api(api_url):
            print(f"setup failed: the API at {api_url} never reported MongoDB ok (logs: {LOG_DIR})")
            return 2
        print(f"services up (logs: {LOG_DIR}); producing seed {args.seed} ...", flush=True)
        sim = subprocess.run(
            [sys.executable, "-m", "ccp.simulator", "--seed", str(args.seed), *args.sim_args]
        )
        if sim.returncode != 0:
            print("setup failed: the simulator did not deliver every message")
            return 2
        got, diff, adjusted, n = verify(api_url, want, args.timeout, grace, adjust_for(baseline))
        dead = [name for name, p in procs.items() if p.poll() is not None]
        diff += [f"service {name} exited early (code {procs[name].returncode})" for name in dead]
        report(adjusted, got, diff, n)
        return 0 if not diff else 1
    finally:
        for proc in procs.values():
            stop(proc)


if __name__ == "__main__":
    sys.exit(main())

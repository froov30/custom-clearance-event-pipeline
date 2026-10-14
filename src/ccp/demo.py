"""One-command, self-verifying demo (TRD §15, P9). All data is synthetic.

    python -m ccp.demo --seed 42
        Dev mode. Resets the `ccp_demo` database, starts validator, processor, monitor and api as
        subprocesses on their own `-demo` consumer groups (moved to the end of each topic first, so
        older messages are ignored), runs the simulator, then verifies the API against `--expect`.

    python -m ccp.demo --seed 42 --verify-only [--api-url URL]
        The services and simulator run elsewhere (Compose app profile, kind); only verify.

Exits 0 only if every expected value matches. Exit code 1 means a mismatch, 2 a setup failure.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

from confluent_kafka import Consumer, TopicPartition

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
    return p.parse_args(argv)


def expected(seed: int) -> dict[str, Any]:
    """The simulator's own manifest for this seed, from the same CLI that produces the events."""
    out = subprocess.run(
        [sys.executable, "-m", "ccp.simulator", "--seed", str(seed), "--expect"],
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
        "expected_alerts": s["alerts_total"],
    }


def mismatches(want: dict[str, Any], got: dict[str, Any]) -> list[str]:
    return [f"{k}: expected {want[k]}, got {got.get(k)}" for k in want if got.get(k) != want[k]]


def verify(api_url: str, want: dict[str, Any], timeout: float, grace: float):
    """Polls until everything matches (or times out), then waits `grace` and checks again,
    so a late, wrong alert is still caught (TRD §11)."""
    deadline = time.monotonic() + timeout
    while True:
        got = actual(api_url)
        diff = mismatches(want, got)
        if not diff or time.monotonic() > deadline:
            break
        time.sleep(2)
    if diff:
        return got, diff
    print(f"matched; re-checking after {grace:.0f}s for late alerts ...", flush=True)
    time.sleep(grace)
    got = actual(api_url)
    return got, mismatches(want, got)


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


def report(want: dict[str, Any], got: dict[str, Any], diff: list[str]) -> None:
    rows = [
        ("produced (= stored + duplicates + rejected)", "produced_total"),
        ("events stored", "expected_stored"),
        ("duplicates dropped", "expected_duplicates"),
        ("anomalies", "anomalies_total"),
        ("hold alerts", "expected_alerts"),
    ]
    print("\nCustoms Clearance Event Pipeline: demo summary (synthetic data)")
    print(f"{'':44} {'expected':>9} {'actual':>9}")
    for label, key in rows:
        print(f"{label:44} {want[key]:>9} {got.get(key, '-'):>9}")
    for title, key in (("DLQ", "rejected_by_reason"), ("state", "state_counts")):
        for name, n in want[key].items():
            print(f"{title + ' ' + name:44} {n:>9} {got.get(key, {}).get(name, '-'):>9}")
    print("\nRESULT: " + ("MATCH" if not diff else "MISMATCH"))
    for line in diff:
        print("  - " + line)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = Config.from_env()
    api_url = args.api_url or f"http://{cfg.api_host}:{cfg.api_port}"
    grace = cfg.hold_alert_seconds + 2 * cfg.monitor_scan_seconds
    want = expected(args.seed)

    if args.verify_only:
        got, diff = verify(api_url, want, args.timeout, grace)
        report(want, got, diff)
        return 0 if not diff else 1

    client = mongo.client(cfg)
    client.drop_database(DEMO_DB)  # the demo owns this database and nothing else
    client.close()
    start_groups_at_end(cfg.kafka_bootstrap_servers)
    env = {**os.environ, "MONGO_DB": DEMO_DB, "CCP_GROUP_SUFFIX": DEMO_SUFFIX}
    procs = {name: spawn(name, env) for name in SERVICES}
    try:
        if not wait_for_api(api_url):
            print(f"setup failed: the API at {api_url} never reported MongoDB ok (logs: {LOG_DIR})")
            return 2
        print(f"services up (logs: {LOG_DIR}); producing seed {args.seed} ...", flush=True)
        sim = subprocess.run([sys.executable, "-m", "ccp.simulator", "--seed", str(args.seed)])
        if sim.returncode != 0:
            print("setup failed: the simulator did not deliver every message")
            return 2
        got, diff = verify(api_url, want, args.timeout, grace)
        dead = [n for n, p in procs.items() if p.poll() is not None]
        diff += [f"service {n} exited early (code {procs[n].returncode})" for n in dead]
        report(want, got, diff)
        return 0 if not diff else 1
    finally:
        for proc in procs.values():
            stop(proc)


if __name__ == "__main__":
    sys.exit(main())

"""Idempotent topic bootstrap (TRD §7). Run: python -m ccp.common.topics"""

import logging
import time

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ClusterMetadata, NewTopic

from ccp.common.config import Config
from ccp.common.log import setup_logging

RAW = "customs.events.raw"
VALIDATED = "customs.events.validated"
DLQ = "customs.events.dlq"
PARTITIONS = {RAW: 6, VALIDATED: 6, DLQ: 1}

log = logging.getLogger(__name__)


def wait_for_broker(admin: AdminClient, timeout_s: float = 60.0) -> ClusterMetadata:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return admin.list_topics(timeout=5)
        except KafkaException:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def ensure_topics(admin: AdminClient) -> dict[str, str]:
    """Creates missing topics; fails loudly if an existing one has the wrong partition count."""
    existing = wait_for_broker(admin).topics
    result = {}
    for topic, want in PARTITIONS.items():
        if topic in existing:
            have = len(existing[topic].partitions)
            if have != want:
                raise RuntimeError(f"{topic} has {have} partitions, expected {want}")
            result[topic] = "exists"
    missing = [
        NewTopic(t, num_partitions=n, replication_factor=1)
        for t, n in PARTITIONS.items()
        if t not in existing
    ]
    for topic, future in admin.create_topics(missing).items() if missing else []:
        try:
            future.result()
            result[topic] = "created"
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:  # a concurrent run won the race
                raise
            result[topic] = "exists"
    return result


def main() -> None:
    cfg = Config.from_env()
    setup_logging("topics", cfg.log_level)
    admin = AdminClient({"bootstrap.servers": cfg.kafka_bootstrap_servers})
    for topic, outcome in ensure_topics(admin).items():
        fields = {"topic": topic, "partitions": PARTITIONS[topic]}
        log.info("topic %s", outcome, extra={"fields": fields})


if __name__ == "__main__":
    main()

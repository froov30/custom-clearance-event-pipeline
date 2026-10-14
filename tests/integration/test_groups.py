import time
import uuid

import pytest
from confluent_kafka.admin import AdminClient

from ccp.common.config import Config
from ccp.common.groups import describe
from ccp.common.kafka import consumer
from ccp.common.topics import PARTITIONS, RAW

pytestmark = pytest.mark.integration


def test_describe_shows_the_member_and_its_partitions():
    cfg = Config.from_env()
    group = f"ccp-test-groups-{uuid.uuid4().hex[:8]}"
    c = consumer(cfg.kafka_bootstrap_servers, group)
    c.subscribe([RAW])
    try:
        end = time.monotonic() + 30
        while not c.assignment() and time.monotonic() < end:
            c.poll(0.5)
        assert c.assignment(), "the consumer never got its partitions"
        out = describe(AdminClient({"bootstrap.servers": cfg.kafka_bootstrap_servers}), group)
        assert out["group"] == group
        assert out["state"] == "STABLE"
        assert len(out["members"]) == 1
        assert out["members"][0]["partitions"] == sorted(
            f"{RAW}[{p}]" for p in range(PARTITIONS[RAW])
        )
    finally:
        c.close()

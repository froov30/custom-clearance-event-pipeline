import uuid

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from ccp.common.config import Config
from ccp.common.kafka import consumer, producer
from ccp.common.topics import PARTITIONS

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def cfg():
    return Config.from_env()


@pytest.fixture(scope="module")
def admin(cfg):
    return AdminClient({"bootstrap.servers": cfg.kafka_bootstrap_servers})


def test_topics_exist_with_expected_partitions(admin):
    topics = admin.list_topics(timeout=10).topics
    assert {t: len(topics[t].partitions) for t in PARTITIONS if t in topics} == PARTITIONS


def test_host_client_round_trip_through_external_listener(cfg, admin):
    # A scratch topic, so the pipeline topics never carry test messages.
    topic = f"ccp.test.{uuid.uuid4().hex[:8]}"
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result()
    try:
        errors = []
        p = producer(cfg.kafka_bootstrap_servers)
        p.produce(topic, key=b"S-1", value=b"hello", on_delivery=lambda err, _: errors.append(err))
        assert p.flush(10) == 0
        assert errors == [None]

        c = consumer(cfg.kafka_bootstrap_servers, f"ccp-test-{uuid.uuid4().hex[:8]}")
        c.subscribe([topic])
        try:
            msg = None
            for _ in range(30):
                msg = c.poll(1.0)
                if msg is not None and msg.error() is None:
                    break
            assert msg is not None and msg.error() is None
            assert (msg.key(), msg.value()) == (b"S-1", b"hello")
        finally:
            c.close()
    finally:
        admin.delete_topics([topic])[topic].result()


def test_mongo_accepts_credentials(cfg):
    with MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=5000, tz_aware=True) as client:
        assert client.admin.command("ping")["ok"] == 1
        client.list_database_names()


def test_mongo_rejects_anonymous_reads(cfg):
    anon = f"mongodb://{cfg.mongo_host}:{cfg.mongo_port}/"
    with MongoClient(anon, serverSelectionTimeoutMS=5000) as client:
        with pytest.raises(OperationFailure):
            client.list_database_names()

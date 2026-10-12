import pytest

from ccp.common.config import Config, ConfigError


def test_defaults():
    c = Config.from_env({})
    assert c.kafka_bootstrap_servers == "localhost:9092"
    assert (c.mongo_host, c.mongo_port, c.mongo_db) == ("localhost", 27017, "ccp")
    assert (c.hold_alert_seconds, c.monitor_scan_seconds) == (20, 5)
    assert c.heartbeat_dir == ".run"


def test_overrides_and_int_parsing():
    c = Config.from_env({"MONGO_PORT": "27018", "HOLD_ALERT_SECONDS": "7", "LOG_LEVEL": "debug"})
    assert (c.mongo_port, c.hold_alert_seconds, c.log_level) == (27018, 7, "DEBUG")


def test_bad_int_is_a_clear_error():
    with pytest.raises(ConfigError, match="HOLD_ALERT_SECONDS"):
        Config.from_env({"HOLD_ALERT_SECONDS": "soon"})


def test_mongo_uri_requires_credentials():
    with pytest.raises(ConfigError):
        _ = Config.from_env({}).mongo_uri


def test_mongo_uri_escapes_credentials():
    c = Config.from_env({"MONGO_USER": "ccp", "MONGO_PASSWORD": "p@ss:w/rd"})
    assert c.mongo_uri == "mongodb://ccp:p%40ss%3Aw%2Frd@localhost:27017/?authSource=admin"

"""Typed config from environment variables only (TRD §14, D-023)."""

import os
from dataclasses import dataclass
from urllib.parse import quote_plus


class ConfigError(ValueError):
    pass


def _int(env: dict[str, str], key: str, default: int) -> int:
    raw = env.get(key, str(default))
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"{key} must be an integer, got {raw!r}") from None


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    mongo_host: str
    mongo_port: int
    mongo_user: str
    mongo_password: str
    mongo_db: str
    ref_data_dir: str
    hold_alert_seconds: int
    monitor_scan_seconds: int
    heartbeat_dir: str
    log_level: str

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "Config":
        env = dict(os.environ) if env is None else env
        return cls(
            kafka_bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            mongo_host=env.get("MONGO_HOST", "localhost"),
            mongo_port=_int(env, "MONGO_PORT", 27017),
            mongo_user=env.get("MONGO_USER", ""),
            mongo_password=env.get("MONGO_PASSWORD", ""),
            mongo_db=env.get("MONGO_DB", "ccp"),
            ref_data_dir=env.get("REF_DATA_DIR", "data/ref"),
            hold_alert_seconds=_int(env, "HOLD_ALERT_SECONDS", 20),
            monitor_scan_seconds=_int(env, "MONITOR_SCAN_SECONDS", 5),
            heartbeat_dir=env.get("HEARTBEAT_DIR", ".run"),
            log_level=env.get("LOG_LEVEL", "INFO").upper(),
        )

    @property
    def mongo_uri(self) -> str:
        # Credentials are required only by the services that talk to MongoDB, so check on use.
        if not self.mongo_user or not self.mongo_password:
            raise ConfigError("MONGO_USER and MONGO_PASSWORD must be set")
        user, pw = quote_plus(self.mongo_user), quote_plus(self.mongo_password)
        return f"mongodb://{user}:{pw}@{self.mongo_host}:{self.mongo_port}/?authSource=admin"

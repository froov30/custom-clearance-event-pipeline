"""MongoDB client and indexes (TRD §6)."""

from pymongo import ASCENDING, MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure

from ccp.common.config import Config

# MongoDB unreachable (covers AutoReconnect and ServerSelectionTimeoutError): retry, never DLQ.
TRANSIENT = (ConnectionFailure,)


def client(cfg: Config) -> MongoClient:
    # tz_aware: stored datetimes come back as aware UTC, which the fold needs to compare them.
    # The socket and connect timeouts turn a hung MongoDB (paused, or a dead network path) into
    # AutoReconnect within 5 s. Without them a call blocks forever, the heartbeat goes stale,
    # and the liveness probe restarts a healthy pod (D-053).
    return MongoClient(
        cfg.mongo_uri,
        tz_aware=True,
        serverSelectionTimeoutMS=5000,
        socketTimeoutMS=5000,
        connectTimeoutMS=5000,
    )


def database(cfg: Config) -> Database:
    return client(cfg)[cfg.mongo_db]


def ensure_indexes(db: Database) -> None:
    """Idempotent: create_index is a no-op when the index already exists."""
    db.events.create_index([("event_id", ASCENDING)], unique=True)
    db.events.create_index([("shipment_id", ASCENDING), ("occurred_at", ASCENDING)])
    db.shipment_status.create_index([("state", ASCENDING), ("state_since", ASCENDING)])
    db.shipment_status.create_index([("anomaly_count", ASCENDING)])
    db.alerts.create_index([("shipment_id", ASCENDING), ("state_since", ASCENDING)], unique=True)
    db.rejections.create_index([("reason", ASCENDING)])

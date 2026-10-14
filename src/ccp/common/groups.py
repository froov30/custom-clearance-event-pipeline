"""Which member of a consumer group owns which partitions (P11 rebalance evidence).

    python -m ccp.common.groups ccp-processor [more groups ...]

The native Kafka image ships no CLI tools, so this uses the AdminClient (ARCHITECTURE §8).
"""

import json
import sys
from typing import Any

from confluent_kafka.admin import AdminClient

from ccp.common.config import Config


def describe(admin: AdminClient, group: str) -> dict[str, Any]:
    desc = admin.describe_consumer_groups([group], request_timeout=15)[group].result()
    members = [
        {
            "client_id": m.client_id,
            "host": m.host,
            "partitions": sorted(
                f"{tp.topic}[{tp.partition}]" for tp in m.assignment.topic_partitions
            ),
        }
        for m in desc.members
    ]
    members.sort(key=lambda m: m["partitions"])
    return {"group": group, "state": desc.state.name, "members": members}


def main(argv: list[str] | None = None) -> int:
    groups = sys.argv[1:] if argv is None else argv
    if not groups:
        print("usage: python -m ccp.common.groups GROUP [GROUP ...]", file=sys.stderr)
        return 2
    admin = AdminClient({"bootstrap.servers": Config.from_env().kafka_bootstrap_servers})
    for group in groups:
        print(json.dumps(describe(admin, group), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

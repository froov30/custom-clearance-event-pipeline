"""Chaos helper for scripts/kind-chaos.ps1, run inside the api pod on synthetic data (D-067).

Get-Content scripts/chaos_probe.py | kubectl -n ccp exec -i deploy/api -- python - offsets
    -> JSON list: the end offset of each `validated` partition
... | kubectl -n ccp exec -i deploy/api -- python - reemit <count> '<json offsets>'
    -> re-sends <count> messages of this run from the given offsets, byte for byte, as a validator
       re-send would. The processor counts each as a duplicate; the demo must account for them.
"""

import json
import sys

from confluent_kafka import Consumer, Producer, TopicPartition

from ccp.common.config import Config
from ccp.common.topics import PARTITIONS, VALIDATED

bootstrap = Config.from_env().kafka_bootstrap_servers
mode = sys.argv[1]

if mode == "offsets":
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "ccp-chaos-probe"})
    ends = [
        c.get_watermark_offsets(TopicPartition(VALIDATED, p), timeout=15)[1]
        for p in range(PARTITIONS[VALIDATED])
    ]
    c.close()
    print(json.dumps(ends))
elif mode == "reemit":
    count, start = int(sys.argv[2]), json.loads(sys.argv[3])
    c = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": "ccp-chaos-probe", "enable.auto.commit": False}
    )
    c.assign([TopicPartition(VALIDATED, 0, start[0])])
    msgs = []
    while len(msgs) < count:
        m = c.poll(15)
        if m is None or m.error():
            break
        msgs.append(m)
    c.close()
    p = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
    for m in msgs:
        p.produce(VALIDATED, key=m.key(), value=m.value())
    unsent = p.flush(30)
    print(json.dumps({"re_sent": len(msgs) if unsent == 0 else 0, "unsent": unsent}))
else:
    sys.exit(f"unknown mode {mode!r}")

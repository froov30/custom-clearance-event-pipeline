# TRD: Customs Clearance Event Pipeline

> **Synthetic data** throughout. The system design lives here and in ARCHITECTURE.md.
> Requirement IDs (FR-n) refer to PRD.md, and decision IDs (D-nnn) refer to DECISIONS.md.

## 1. Scope

There are five services: `simulator` → `validator` → `processor` → MongoDB, with `monitor` (alerts plus the DLQ sink)
and `api` alongside. All pure logic (schema, ISO 6346 check, reference lookup, state fold, simulator
generation) lives in I/O-free modules so it can be unit-tested without Kafka or MongoDB.

## 2. Event contract and versioning

Messages are UTF-8 JSON. The Kafka key is `shipment_id`.

| Field | Type | Rule |
|---|---|---|
| `event_id` | string | A UUID, unique per logical event. It is the dedupe key. |
| `shipment_id` | string | 1–64 characters. It is the partition key. |
| `container_id` | string | Must be present. Its content is checked separately against ISO 6346 (§4). |
| `event_type` | enum | `DECLARATION_FILED`, `DOCUMENTS_VERIFIED`, `INSPECTION_HOLD`, `CLEARED`, `RELEASED`, `REJECTED` |
| `occurred_at` | datetime | ISO-8601 with an offset. A naive timestamp is invalid. It is normalised to UTC. |
| `source` | string | Non-empty, for example `declaration_system`, `port_system` or `broker_system` |
| `schema_version` | int | Must be in the supported set, currently `{1}` |
| `payload` | object | Validated per `event_type` (table below) |

| `event_type` | Required payload |
|---|---|
| `DECLARATION_FILED` | `port_code` (UN/LOCODE), `hs_code` (6 digits), `declarant` (string) |
| `INSPECTION_HOLD` | `port_code`, `reason` |
| `REJECTED` | `reason` |
| others | none |

**Versioning.** Adding an optional field does not change the version, and unknown fields are ignored
(Pydantic `extra="ignore"`). A breaking change bumps `schema_version`. The validator accepts only the
versions it knows and rejects everything else as `SCHEMA_INVALID`. Shortcut: there is no schema registry,
because schemas live in code (a registry is listed as an add-on).

Example:
```json
{"event_id":"6f1c…","shipment_id":"SHP-000042","container_id":"CSQU3054383",
 "event_type":"DECLARATION_FILED","occurred_at":"2026-10-11T08:00:00Z","source":"declaration_system",
 "schema_version":1,"payload":{"port_code":"NLRTM","hs_code":"847130","declarant":"ACME Imports"}}
```

## 3. Reference data

`data/ref/ports.csv` (about 30 UN/LOCODEs) and `data/ref/hs_codes.csv` (about 20 6-digit HS codes) are
hand-picked subsets. Each service loads them once at startup into a `frozenset` (path set by `REF_DATA_DIR`).
Changing them requires a restart. That is a deliberate shortcut, and the fix would be a reference-data topic or collection.

## 4. Validation (validator service)

The validator runs these checks in order and stops at the first failure:

1. Parse the JSON and validate it against the Pydantic model (envelope plus the payload for that type). Failure → **`SCHEMA_INVALID`**
2. Check the ISO 6346 container ID: it must match `^[A-Z]{3}[UJZ][0-9]{6}[0-9]$`, and the check digit must be correct. To compute the check digit, map letters to values (A=10, skipping multiples of 11), compute Σ value·2^i for i = 0..9, take the result mod 11, then mod 10. Failure → **`CONTAINER_ID_INVALID`**
3. Check that every `port_code` and `hs_code` in the payload is in the reference sets. Failure → **`UNKNOWN_REFERENCE_DATA`**

An event that passes is re-serialised as canonical JSON and sent to `customs.events.validated`.
The validator does **not** deduplicate (D-012).

## 5. State fold

**Sort key:** `(occurred_at, lifecycle_rank, event_id)`. The ranks are: DECLARATION_FILED 1, DOCUMENTS_VERIFIED 2,
INSPECTION_HOLD 3, CLEARED 4, RELEASED 5, REJECTED 6. This key is a total order, so the fold result is
independent of arrival order (D-015).

**Transition table** (event → target state, allowed only from the listed states):

| Event | From | To |
|---|---|---|
| DECLARATION_FILED | UNKNOWN | FILED |
| DOCUMENTS_VERIFIED | FILED | DOCS_VERIFIED |
| INSPECTION_HOLD | DOCS_VERIFIED | ON_HOLD |
| CLEARED | DOCS_VERIFIED, ON_HOLD | CLEARED |
| RELEASED | CLEARED | RELEASED |
| REJECTED | FILED, ON_HOLD | REJECTED |

RELEASED and REJECTED are terminal.

**Fold:** start at `state=UNKNOWN, state_since=null`. For each event in sort order:
- If the transition is legal, set `state` to the target state and `state_since` to that event's `occurred_at`.
- If it is illegal, leave the state unchanged and add an anomaly:
  `{type:"ILLEGAL_TRANSITION", event_id, event_type, from_state, occurred_at}`

The result is `(state, state_since, anomalies)`. It is a pure function of the set of events, so
recomputing it after a late event can remove anomalies that existed before (FR-7).

## 6. Data model and indexes (MongoDB, database `ccp`)

| Collection | Document | Indexes |
|---|---|---|
| `events` | The validated envelope plus `ingested_at` and `source_message` (`{topic, partition, offset}` of the Kafka message that first carried it) | unique `event_id`; `{shipment_id:1, occurred_at:1}` |
| `shipment_status` | `_id`=shipment_id, `container_id`, `state`, `state_since`, `event_count`, `anomalies[]`, `anomaly_count`, `updated_at` | `{state:1, state_since:1}`; `{anomaly_count:1}` |
| `alerts` | `shipment_id`, `state_since`, `held_seconds`, `raised_at` | unique `{shipment_id:1, state_since:1}` |
| `rejections` | `_id`=`"{topic}:{partition}:{offset}"`, `reason`, `detail`, `raw`, `failed_at` | `{reason:1}` |
| `duplicates` | `_id`=`"{topic}:{partition}:{offset}"` of a message whose `event_id` was already stored from **another** message; `event_id`, `shipment_id`, `seen_at`. `duplicates_dropped` is this collection's count (D-051). | (by `_id`) |

`events` is append-only: the code only ever inserts. `container_id` on the status document comes from the
first event in fold order (known limit, §17).

## 7. Topics, partitioning and ordering

| Topic | Partitions | Key | Producer | Consumer group |
|---|---|---|---|---|
| `customs.events.raw` | 6 | shipment_id | simulator | `ccp-validator` |
| `customs.events.validated` | 6 | shipment_id | validator | `ccp-processor` |
| `customs.events.dlq` | 1 | original key | validator, processor | `ccp-monitor-dlq` |

Replication factor is 1 (single broker). Kafka keeps order only **within a partition**, and keying by
`shipment_id` sends all of a shipment's events to the same partition. The pipeline does **not** depend on that
ordering for correctness, though: the fold re-sorts by event time. Partition order mainly ensures that
one consumer at a time owns a given shipment. With 6 partitions on `validated`, at most 6 processor
replicas can be active.

## 8. Delivery semantics and idempotency

- **Producers:** `enable.idempotence=true`, `acks=all`.
- **Consumers:** `enable.auto.commit=false`. A consumer commits a message's offset synchronously only **after** its
  side effects succeed. For the validator, that means after the delivery report for the message it
  produced (shortcut: one `flush()` per message, which is simple but slow; the upgrade is a batched produce plus a commit per batch).
- **Shutdown:** SIGTERM and SIGINT set a flag. The current message finishes, then `consumer.close()` runs (it commits nothing extra
  and leaves the group cleanly). SIGINT is handled because the dev host is Windows.
- **Processor per message:**
  1. Insert into `events`, recording the message's `(topic, partition, offset)` as `source_message`. On `DuplicateKeyError`,
     compare: the **same** message again is a redelivery (not a duplicate); a **different** message is a true duplicate (D-051). **Continue** either way.
  2. Load all of the shipment's events (using the `shipment_id+occurred_at` index), fold, and upsert `shipment_status`
     **guarded by `event_count`**: `update_one({_id, event_count: {$lte: n}}, {$set: …}, upsert=True)`.
     If a newer document exists, the filter misses, the upsert hits `DuplicateKeyError`, and the stale write is
     dropped (D-027). Then **re-count** the shipment's events: if the count is no longer `n`, a concurrent insert was
     missed by the read (the guard cannot see that, because the counts can tie), so fold again, up to 5 times (D-052).
  3. If it was a true duplicate, upsert `duplicates` by the message's coordinates (idempotent).
  4. Commit the offset.
- **Healing:** a crash between steps 1 and 2 leaves the event stored but the status stale. On redelivery, step 1
  raises `DuplicateKeyError`, and step 2 still recomputes, so the status heals (D-017). Because the redelivery comes from
  the same message, it is **not** counted, and step 3 is idempotent, so `duplicates_dropped` is exact across processor
  crashes and replays (verified: kill-and-restart test). It still includes duplicates that reach `validated` as separate
  messages, e.g. a validator that crashed after producing and re-emitted on restart.
- **MongoDB timeouts:** the client uses 5 s server-selection, connect and socket timeouts, so a hung MongoDB raises
  `AutoReconnect` (retried with backoff, heartbeat kept fresh) instead of blocking forever. Measured worst-case heartbeat gap
  during a MongoDB pause: 14.7 s (three stacked timeouts), so the liveness probe threshold must be at least 45 s (D-053).
  At start-up the processor waits for MongoDB the same way instead of exiting.
- **Net effect:** at-least-once delivery plus idempotent, deterministic processing gives an exactly-once
  *effect* on stored state. The project does not use Kafka transactions and does not claim exactly-once semantics (D-019).

## 9. Failure-handling matrix

| Fault | Detection | Outcome |
|---|---|---|
| Non-JSON, missing field, wrong type, unknown type or version | Pydantic in the validator | DLQ `SCHEMA_INVALID`, commit |
| Bad container format or check digit | ISO 6346 check | DLQ `CONTAINER_ID_INVALID`, commit |
| Port or HS code not in the reference data | Set lookup | DLQ `UNKNOWN_REFERENCE_DATA`, commit |
| Duplicate `event_id` | Unique index → `DuplicateKeyError` | Recompute; record in `duplicates` only if it came in a different message (D-051); commit. Not sent to the DLQ. |
| Late or out-of-order event | — (handled by design) | The fold sorts by event time, so the state becomes correct once the event arrives and any interim anomaly heals |
| Illegal sequence or event after a terminal state | Fold | `ILLEGAL_TRANSITION` anomaly, state unchanged by that step. Not sent to the DLQ. |
| MongoDB unavailable | `pymongo.errors.AutoReconnect` / `ServerSelectionTimeoutError` | Retry with backoff, no commit, no DLQ |
| Kafka unavailable | librdkafka reconnects; produce or delivery error | Retry with backoff, no commit |
| Validator crash after produce, before commit | — | Re-emitted on restart, and the processor deduplicates it |
| Processor crash between insert and upsert | — | Redelivered → duplicate → recompute heals |
| Monitor crash after inserting an alert | Unique index on `alerts` | The repeat insert is ignored, so there is still one alert |
| DLQ sink replay | `rejections._id` = source coordinates | Upsert, so there is no double count (D-026) |
| Message on `validated` that fails to parse (should not happen) | Pydantic in the processor | DLQ `SCHEMA_INVALID`, commit |
| Unexpected exception on one message | Any exception not classified as transient | Retry up to 5 times, then DLQ `PROCESSING_ERROR` and commit, so the service never crash-loops (D-031) |
| Hung poll loop | Heartbeat file goes stale | The Kubernetes liveness probe restarts the pod |

## 10. Retry vs DLQ policy

- **Transient** (the infrastructure is down): exponential backoff from 0.5 s, doubling up to 30 s, with **unlimited** retries.
  The service does not commit, so the message is redelivered after a restart. The heartbeat **keeps being touched** while it retries,
  because restarting the pod cannot fix MongoDB. A SIGTERM interrupts the backoff.
- **Permanent** (the data is bad): send to the DLQ once with a reason code, then commit. A poison message cannot block a partition.
- **Reason codes (complete list):** `SCHEMA_INVALID`, `CONTAINER_ID_INVALID`, `UNKNOWN_REFERENCE_DATA` for bad data, and
  `PROCESSING_ERROR` for an unexpected exception that survives 5 attempts (D-031). A clean demo run expects `PROCESSING_ERROR = 0`.
- DLQ message: `{reason, detail, source_topic, source_partition, source_offset, raw, failed_at}`. `raw` is the
  original value decoded as UTF-8 with `errors="replace"`.

## 11. Simulator and expected outcomes

- `generate(seed, config, start) -> list[ScheduledEvent]` is pure. It uses `random.Random(seed)`, and `event_id`
  values come from that RNG, so the same seed and start time always produce the same events.
- Each shipment follows a script: the happy path (FILED → VERIFIED → CLEARED → RELEASED), hold-then-clear,
  hold-then-reject, filed-then-reject, or **left on hold**. Step *k* has `occurred_at = start + offset_k`, where the offsets use
  `--step-delay` plus jitter. The CLI sleeps until each event's scheduled time, so timestamps are real UTC "now".
- Faults (each with its own rate flag):
  - **duplicate:** re-send a sent event with the same `event_id`
  - **late:** emit after the shipment's later events, keeping the original `occurred_at`
  - **malformed:** replace the event with non-JSON, a missing field or a bad type
  - **illegal sequence:** skip DOCUMENTS_VERIFIED, or release without CLEARED
  - **bad container:** corrupt the check digit
  - **unknown reference:** use a port or HS code not in the CSVs
- `--expect` prints a manifest **without connecting to Kafka**: `produced_total`, `expected_stored`,
  `expected_duplicates`, `rejected_by_reason`, `state_counts`, `anomalies_total`, `expected_alerts`. To build it, the simulator runs the
  **same pure fold** over the events that will pass validation. This reuse is acknowledged in PRD §8.
- **Timing constraints** (enforced by the CLI at startup): `late_delay < HOLD_ALERT_SECONDS` and the hold-to-clear
  gap `< HOLD_ALERT_SECONDS`, so `expected_alerts` = the number of shipments whose final state is ON_HOLD, and the demo compares
  it with the **open** alerts (`resolved_at` null), which is what the monitor leaves open once processing has caught up (D-069).
  Defaults: `--step-delay 1` s (jitter up to 0.5 s), `--late-delay 3` s, `HOLD_ALERT_SECONDS=20`. Under heavy backlog a held-then-cleared
  shipment can raise an alert for a while, and it closes once its clearing event is processed (§17, D-064, D-069). The demo waits
  `HOLD_ALERT_SECONDS + 2 × MONITOR_SCAN_SECONDS` after the last event before it re-checks alerts. `rejected_by_reason` always lists
  all four codes, with `PROCESSING_ERROR: 0`. Validator re-emits (D-067) are added to the expected duplicates and reported on their own line.

## 12. Monitor

There is one process with one loop: poll the DLQ consumer with a 1 s timeout, and every `MONITOR_SCAN_SECONDS` run the hold
scan `shipment_status.find({state:"ON_HOLD", state_since:{$lt: now - HOLD_ALERT_SECONDS}})`, inserting one
alert per result. The unique index makes repeated inserts no-ops (D-028). The same scan then closes open alerts whose hold has ended:
the shipment is no longer ON_HOLD, has no status, or a later event moved its `state_since` (D-069). A closed alert keeps its record and
gets `resolved_at`; it is never re-raised. The DLQ sink upserts each record into `rejections`, then commits.

## 13. API contract (FastAPI; Swagger at `/docs`)

| Method and path | Returns |
|---|---|
| `GET /health` | Always 200, `{"status":"ok","mongo":"ok"\|"down"}` (it does not fail liveness when MongoDB is down) |
| `GET /shipments?state=&limit=&offset=` | A page of status documents. `state` is optional (enum). |
| `GET /shipments/{id}` | The status document, or 404 |
| `GET /shipments/{id}/events` | The events in fold order, or 404 |
| `GET /anomalies?limit=&offset=` | Shipments with `anomaly_count > 0` |
| `GET /alerts?limit=&offset=` | Alerts, newest first |
| `GET /rejections/summary` | `{reason: count}` |
| `GET /stats` | `events_stored`, `duplicates_dropped`, `rejected_total`, `state_counts`, `anomalies_total`, `alerts_total` (raised, including closed), `open_alerts` (not yet closed) |

Pagination: `limit` defaults to 50 with a **maximum of 200** (values above that get a 422). `offset` is ≥ 0. List responses are
`{"items":[…],"limit":n,"offset":n,"total":n}`. Shortcut: offset pagination gets slow on deep pages,
and the upgrade is keyset pagination on `(state_since, _id)`.

## 14. Configuration (environment variables only; `.env.example` is committed, `.env` is never committed)

| Var | Default | Used by |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | all except api |
| `MONGO_HOST` / `MONGO_PORT` | `localhost` / `27017` | processor, monitor, api |
| `MONGO_USER` / `MONGO_PASSWORD` | — (required) | the same, plus the mongo container |
| `MONGO_DB` | `ccp` | processor, monitor, api |
| `REF_DATA_DIR` | `data/ref` | validator, simulator |
| `HOLD_ALERT_SECONDS` | `20` | monitor, simulator (`--expect` checks) |
| `MONITOR_SCAN_SECONDS` | `5` | monitor |
| `HEARTBEAT_DIR` | `.run` (host dev, inside the repo on D:); the image sets `/tmp` | validator, processor, monitor. Each service writes `<dir>/<service>.heartbeat`, so the host services never share a file. |
| `LOG_LEVEL` | `INFO` | all |
| `API_HOST` / `API_PORT` | `127.0.0.1` / `8000` | api. Containers set `API_HOST=0.0.0.0`. |
| `CCP_GROUP_SUFFIX` | empty | validator, processor, monitor. Appended to the consumer group id; integration tests set it so they never move the real groups' offsets (D-050). |

Topic names and group IDs are code constants, not configuration. Simulator settings are CLI flags.
Logs are JSON lines on stdout.

**Host storage (D-040):** on the dev host, every path the project writes to is on D:. `scripts/dev-env.ps1` redirects the
uv, pip, temp, Docker CLI and kube locations to `D:\ccp-data\`, and the services write only inside the repo (`.run\`, `evidence\`).

## 15. Testing strategy

- **Unit (no infrastructure):** ISO 6346 (valid, bad check digit, bad format); the schema (each rejection path); reference lookup;
  the fold (every event type from every state, terminal states, anomaly healing, and **every permutation of a legal
  event set gives the same result**); and the simulator's determinism.
- **Integration** (marked `integration`, runs only when `CCP_INTEGRATION=1`, against Compose): validator routing,
  processor idempotency (replay twice), shuffled replay, kill-and-restart, the monitor raising a single alert per hold,
  and the API against real MongoDB (D-037).
- **Smoke:** `python -m ccp.demo` against Compose (locally and in CI). By default it spawns the services as host subprocesses (dev mode),
  on its own `ccp_demo` database and `-demo` consumer groups, which it first moves to the end of each topic (D-058).
  With `--verify-only` it skips spawning and producing (the simulator runs separately via `docker compose run` or the k8s Job) and only compares `/stats` with `--expect`.
  It exits non-zero on any mismatch with the expected values.

## 16. Non-functional requirements

| NFR | Target (verified in) |
|---|---|
| Correctness | The demo matches the expected values exactly in a clean run (P9) |
| Recoverability | Kill and restart any consumer with no state divergence (P6, P7) |
| Reproducibility | Same seed gives the same events and expected values (P4) |
| Footprint | One `python:3.11.17-slim-trixie` multi-stage image; its size is recorded in P10 (no number is claimed before then). Memory **limits** per container are set in D-039; actual usage is measured in P3 and P11. |
| Host storage | Nothing the project produces is written to C: (D-040); checked in P0 |
| Operability | Health endpoint, heartbeat probes, structured logs, Kafka UI (P3, P8, P11) |
| Security | MongoDB auth on, credentials from env or Secret, no secrets in git. API auth is out of scope. |
| Throughput | Not a goal. Any figure must come from a recorded run (an add-on Locust test). |

## 17. Known limits

- Clock skew between sources is not corrected: `occurred_at` is trusted as given.
- The same logical event re-sent with a **new** `event_id` is not detected as a duplicate.
- Each write re-reads all of the shipment's events, so the cost is O(events per shipment). Measured median per event
  (`python scripts/bench_recompute.py`, worst case with one anomaly per event): 6.2 ms at 10 events, 8.7 ms at 100,
  26.4 ms at 1,000, 59.2 ms at 5,000. Customs lifecycles have under 10 events, so this is fine; the upgrade for long
  histories is an incremental fold when the new event sorts last (D-055).
- A single Kafka broker with RF=1 and a single MongoDB node give no high availability. Without MongoDB transactions, the two writes are not atomic, and the recompute-on-duplicate step heals this.
- `duplicates_dropped` counts every message whose event is already stored, so a validator crash that re-sends an event counts too
  (processor crashes and redeliveries do not, D-051). The demo accounts for those re-sends (`validated − stored`, D-067), so a
  validator kill is verified, not excluded.
- Alerts are keyed by `(shipment_id, state_since)`. A late event that changes `state_since` leaves the old alert open until the scan
  closes it (D-069), and the new hold can raise its own alert. Closing does not delete: the history of raised alerts is kept.
- The monitor compares event time against its own wall clock, so processing lag reads as hold time. A hold that was cleared but
  whose `CLEARED` event is still in the backlog raises an alert once lag exceeds `HOLD_ALERT_SECONDS` minus the hold's real length,
  and that alert closes only after the clearing event is processed (D-069). Measured on kind (D-064): with 21,506 messages and chaos the
  lag reached 139 s (average 85 s), and a 20 s threshold raised 2,314 alerts for 605 holds. The closing fixes the stored state, not the
  page: anyone watching `alerts_total` or a live alert feed still sees the false ones. The real fix is an event-time watermark (add-on).
  Keep the threshold above worst-case lag (a SIGKILLed consumer alone stalls its partitions for the 45 s session timeout).
- A container ID that changes within a shipment is not detected; the first one in fold order wins.
- Reference data is static and requires a restart to change. Offset pagination is slow on deep pages. The API has no authentication.
- Kafka UI exists in Compose only.

## 18. Unverified assumptions (each proven or disproven in the phase named)

| # | Assumption | Phase |
|---|---|---|
| A1 | Kafka, MongoDB and Kafka UI fit within the D-039 limits on this 7.8 GB machine (0.8 GB free was measured with Docker stopped, so other apps must be closed) | P3 (prove with `docker stats`) |
| A2 | kind, Kafka, MongoDB and 4 service Deployments fit in RAM, with Compose stopped. **Verified (P11):** the node used 1.667 of 3.744 GiB (44.5%) with 3 processors; one reading (DECISIONS "P11 results"). | P11 |
| A3 | `apache/kafka-native:4.3.1` runs single-node KRaft with two listeners using environment variables only. **The tag exists (verified 2026-10-11); its runtime behaviour is unverified.** Fallback: `apache/kafka:4.3.1`. | P3 |
| A4 | `kafbat/kafka-ui:v1.5.0` works against KRaft (**the tag is verified; its behaviour is not**) | P3 |
| A5 | `mongo:8.0.32` with root-user auth from env and a 0.25 GB cache works, and pymongo connects with `authSource=admin` | P3 |
| A6 | `confluent-kafka` 2.16.0 installs without a compiler. **Wheels exist for cp311 win_amd64 and manylinux_2_28 x86_64 (verified); the install itself is unverified.** | P0, P10 |
| A14 | Redirecting `DOCKER_CONFIG` to D: still reaches the Docker Desktop engine (the `desktop-linux` context), and uv with `UV_PYTHON_INSTALL_DIR` and `--no-bin` writes nothing to C: | P0 |
| A15 | The kind node image and its PVCs live inside the Docker disk image, so they land on D: after the D-040 move. **Verified (P11):** the Docker disk image is `D:\ccp-data\docker\docker_data.vhdx` (12.03 GB); the kind binary and the image tar went to `D:\ccp-data`. | P11 |
| A7 | The idempotent producer works against a single broker with RF=1 | P4 |
| A8 | The SIGINT/SIGTERM handler gives a clean close on the Windows host | P5 |
| A9 | Recompute-per-event is fast enough at demo scale. **Verified (P6):** 6.2 ms per event at 10 events per shipment (D-055). | P6, P9 |
| A10 | In a clean run, the fold-based expected values equal the pipeline's output | P9 |
| A11 | SIGTERM reaches the Python process when it is PID 1 (exec-form CMD) | P10 |
| A12 | A GitHub-hosted runner can run the Compose smoke test within a reasonable time. **Verified:** CI run 38542228617 passed on its first push in 4 min 28 s (integration job 4 min 0 s, 39 tests; image job with demo MATCH 2 min 9 s). | P10 |
| A13 | The eager rebalance with finish-in-flight, plus the `event_count` guard, means no stale status survives a scale-out. **Verified (P11, D-063):** under a 21,506-message load with scaling 1 → 3 → 2 → 3, a graceful and a SIGKILLed processor, a liveness restart and a broker restart, the per-shipment audit found 0 stale statuses in 5,997 shipments and the demo matched 16/16. | P11 |

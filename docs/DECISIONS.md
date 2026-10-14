# Decisions log (live)

Each entry is added at the moment its decision is made. Format: **Context · Options · Choice · Why · Trade-off.**
"Locked" means the decision was set in the project brief. "Stage 1" means it was made while writing the design docs. All dates are 2026-10-11 unless noted.
The simplicity gate applies: every dependency, service or abstraction carries one line on why the stdlib or an existing piece is not enough.

---

### D-001 Language and tooling: Python 3.11, ruff, pytest (locked)
- **Context:** One language across five services and tests. **Options:** Python, Go, Java.
- **Choice:** Python 3.11 with type hints, ruff (lint and format) and pytest.
- **Why:** Fastest path to readable code that can be defended in an interview. Ruff replaces flake8, isort and black with one tool.
- **Trade-off:** Lower raw throughput than the JVM or Go, which does not matter at demo scale.

### D-002 Broker: Kafka, KRaft mode, single broker, official image (locked)
- **Context:** Needs an ordered, partitioned and replayable log with consumer groups. **Options:** Kafka, RabbitMQ, Redis Streams.
- **Choice:** Kafka in KRaft mode (no ZooKeeper), one broker, RF=1. The image is pinned in D-033 (`apache/kafka-native:4.3.1`, chosen for memory in D-039).
- **Why:** Partitions plus consumer groups are exactly the scaling story, and KRaft removes a whole extra service.
- **Trade-off:** No broker fault tolerance (TRD §17).

### D-003 Kafka client: confluent-kafka (locked)
- **Simplicity gate:** The stdlib has no Kafka client. confluent-kafka wraps librdkafka (idempotent producer, manual commits, rebalance callbacks) and ships wheels.
- **Trade-off:** A native library inside the image; wheel availability is checked as A6.

### D-004 Store: MongoDB single node, auth on, pymongo, no ODM (locked)
- **Context:** Needs an append-only event log, a current-state view and unique indexes. **Options:** MongoDB, PostgreSQL.
- **Choice:** MongoDB through plain pymongo.
- **Why:** Events are JSON documents. Unique indexes give dedupe and idempotent alerts. An ODM would add a layer over five collections and no value.
- **Trade-off:** Without a replica set there are no multi-document transactions, so the two writes are not atomic. D-017 heals that.

### D-005 Schemas: Pydantic v2 (locked)
- **Simplicity gate:** Hand-written dict checks would duplicate what Pydantic already provides (types, timezone-aware datetimes, enums, error messages), and FastAPI needs Pydantic anyway.

### D-006 API: FastAPI (locked)
- **Why:** Typed query parameters with validation (the max page size returns 422 for free) and Swagger with no extra work. **Trade-off:** It brings in uvicorn and starlette.

### D-007 CLI: argparse (locked)
- **Why:** The stdlib is enough for about 10 flags, so no click or typer.

### D-008 One Docker image, one command per service (locked)
- **Choice:** A multi-stage `python:3.11-slim` image (pinned as `python:3.11.17-slim-trixie`, D-033) whose command is `python -m ccp.<service>`.
- **Why:** One build, one artifact to scan and load into kind, and a small disk footprint.
- **Trade-off:** Every service carries every dependency (FastAPI in the validator, for example). That is a small cost.

### D-009 Kubernetes: kind with plain manifests, no Helm or operators (locked)
- **Why:** One namespace and one environment don't need templating. Running Kafka and MongoDB as hand-written single-replica StatefulSets shows what an operator would hide.
- **Trade-off:** Some duplication across manifests. Helm is listed as an add-on.

### D-010 CI: GitHub Actions (locked)
- **Why:** Free for public repos and widely recognised. It runs lint, unit tests and the Compose smoke test.

### D-011 Topics: raw / validated / dlq, keyed by shipment_id, 6 partitions (locked)
- **Why:** The key keeps each shipment on one partition, so one consumer owns it at a time. Six partitions allow scaling the processor up to 6 replicas.
- **Trade-off:** The partition count is fixed at creation, and growing it later would remap keys.

### D-012 Validator does not deduplicate (locked)
- **Why:** The validator has no durable state, and it can re-emit after a crash anyway. Deduplicating there would be both incomplete and redundant. The processor owns dedupe through a unique index.

### D-013 Event-sourced state: refold all of a shipment's events on every write (locked)
- **Options:** (a) an incremental state machine updated per event; (b) a full refold from the event log.
- **Choice:** (b).
- **Why:** The result is a pure function of the set of events, so it is order-independent, replay-safe and self-healing, and it can be tested exhaustively without infrastructure.
- **Trade-off:** O(events per shipment) reads per write. That is fine for about 10 events (TRD §17).

### D-014 Out-of-order events are folded, never rejected or buffered (locked)
- **Options:** reject late events; buffer behind a watermark; fold by event time.
- **Choice:** Fold by event time.
- **Why:** Rejecting them loses true facts. Buffering adds latency, state and a watermark policy that has to be tuned. Folding gives the correct answer as soon as the event arrives, and any interim anomaly heals.
- **Trade-off:** The state can be temporarily wrong until the late event arrives. That is unavoidable without perfect sources.

### D-015 Fold sort key (occurred_at, lifecycle_rank, event_id) (locked)
- **Why:** A total order. The lifecycle rank breaks timestamp ties in the business-sensible direction, and `event_id` makes the order fully deterministic.

### D-016 Illegal transitions become anomalies, not DLQ entries (locked)
- **Why:** An illegal sequence is a fact about the world, not unprocessable data. It is stored, shown and re-derived, so a late event can clear it.

### D-017 A duplicate still triggers a recompute (locked)
- **Why:** It heals a crash between the event insert and the status upsert. **Trade-off:** `duplicates_dropped` includes crash-replay duplicates and is documented that way.

### D-018 The DLQ is only for unprocessable events (locked)
- **Choice:** Reason codes `SCHEMA_INVALID`, `CONTAINER_ID_INVALID` and `UNKNOWN_REFERENCE_DATA`. Duplicates and ordering anomalies never go to the DLQ.

### D-019 At-least-once delivery plus idempotent processing; no Kafka transactions (locked)
- **Options:** Kafka transactions (read-process-write EOS); at-least-once plus idempotent effects.
- **Choice:** The latter. The idempotent producer and `acks=all` are used, and offsets are committed manually after the write.
- **Why:** Kafka transactions do not cover writes to MongoDB, so idempotency is needed at the sink anyway.
- **Trade-off:** Wording. Never claim "exactly-once semantics"; the effect on stored state is exactly-once.

### D-020 Error policy: transient errors retry, permanent errors go to the DLQ (locked)
- **Choice:** Infrastructure errors retry with backoff, without committing and without using the DLQ. Bad data goes to the DLQ with a reason, then the offset is committed. Never crash-loop.

### D-021 Consumer liveness through a heartbeat file and an exec probe (locked)
- **Why:** The consumers have no HTTP port, and adding one just for a probe is extra surface. The heartbeat is touched during backoff too, so a MongoDB outage does not restart pods (TRD §10).

### D-022 Synthetic data with curated reference subsets (locked)
- **Choice:** About 30 UN/LOCODEs and about 20 HS codes in CSV. Every doc says "synthetic data".

### D-023 Config through environment variables only (locked)
- **Simplicity gate:** `os.environ` plus a small typed loader is enough, so no pydantic-settings and no config files. `.env.example` is committed and `.env` is git-ignored.

### D-024 Dev mode: infrastructure in Compose, services in the host venv until P10 (locked)
- **Why:** Fast edit-and-run loops and no image rebuilds while the logic is changing.

### D-025 No DESIGN.md (locked)
- **Why:** The system design lives in TRD.md and ARCHITECTURE.md, and a third file would drift.

---

### D-026 Rejections keyed by source coordinates (Stage 1)
- **Context:** The DLQ sink is at-least-once, so a replay would double-count rejections and break the demo's reconciliation.
- **Options:** a random `_id` (double counts); a hash of the raw bytes (identical malformed messages from different sends would collapse); `topic:partition:offset`.
- **Choice:** `_id = "{source_topic}:{source_partition}:{source_offset}"` with an upsert.
- **Why:** Each source message has exactly one DLQ outcome, and the validator re-emitting after a crash produces the same `_id`.
- **Trade-off:** Resetting or recreating the topic reuses offsets, so `rejections` must be cleared alongside it (the demo does this).

### D-027 shipment_status upsert guarded by event_count (Stage 1)
- **Context:** During a rebalance, two processors could briefly recompute the same shipment, and the slower one might write an older view.
- **Options:** ignore the race; a per-shipment lock; a version guard.
- **Choice:** `update_one({_id, event_count: {$lte: n}}, …, upsert=True)`. A `DuplicateKeyError` means a newer view already exists, so the write is dropped.
- **Why:** One filter clause, no locks, and the log only grows, so `event_count` is monotonic.
- **Trade-off:** Equal counts can overwrite each other, which is harmless because the fold is deterministic.

### D-028 Monitor: one process, one loop (DLQ poll plus periodic hold scan) (Stage 1)
- **Options:** two services; two threads; one loop.
- **Choice:** One loop. It polls with a 1 s timeout and scans every `MONITOR_SCAN_SECONDS`.
- **Why:** No thread-safety concerns and one heartbeat. The brief already groups both jobs into `monitor`.
- **Trade-off:** A long scan delays DLQ consumption, which is negligible at demo scale.

### D-029 The simulator emits the expected outcomes by reusing the pure fold (Stage 1)
- **Context:** The demo must prove its summary "matches the injected faults".
- **Choice:** `--expect` computes `produced_total`, the expected stored, duplicate and rejected counts, the state counts, anomalies and alerts offline from the seed.
- **Why:** The expected values are deterministic and need no Kafka, so they can be compared on Compose and kind alike.
- **Trade-off:** It reuses the fold being tested. The fold is proven separately in P2, so the demo tests the plumbing around it (PRD §8).

### D-030 Demo and topic bootstrap written in Python, not bash (Stage 1)
- **Why:** The dev host is Windows (PowerShell 5.1), and the Python modules run identically on the host, in CI and in the image. The demo lives at `src/ccp/demo.py`, and topic bootstrap at `ccp.common.topics`.
- **Follow-on:** `--verify-only` makes the demo compare `/stats` with `--expect` and nothing else. On kind the host cannot reach Kafka, so the simulator runs in-cluster and one verification path serves Compose and kind alike.

### D-031 ACCEPTED: a `PROCESSING_ERROR` reason for unexpected exceptions (Stage 1, accepted at Gate 1)
- **Context:** The locked reason codes cover bad data only. A code bug that raises on one specific message would either crash-loop the service or block the partition forever, and the brief says "never crash-loop on a poison message".
- **Options:** let it crash (crash-loop); retry forever (stalled partition); retry 5 times, then send to the DLQ as `PROCESSING_ERROR`.
- **Choice:** the last option. It amends D-018: the DLQ codes are now `SCHEMA_INVALID`, `CONTAINER_ID_INVALID`, `UNKNOWN_REFERENCE_DATA` and `PROCESSING_ERROR`.
  Transient infrastructure errors are classified first (TRD §10) and never reach this path.
- **Trade-off:** It can hide a bug, but the DLQ makes the bug visible and counted. The demo expects `PROCESSING_ERROR = 0`, so any occurrence fails the demo.

### D-032 Offset pagination, limit capped at 200 (Stage 1)
- **Why:** The simplest approach that works at demo scale. **Shortcut:** deep pages get slow; upgrade to keyset pagination on `(state_since, _id)` if the data grows.

### D-033 Image tags and package versions pinned only after registry verification (Stage 1, verified at Gate 1)
- **Why:** Rule 7 forbids guessing. Everything below was checked against Docker Hub, PyPI and GitHub on 2026-10-11. Release candidates (`-rc`) and moving tags (`latest`, `main`) were rejected.

| Artifact | Pinned | Evidence |
|---|---|---|
| Kafka | `apache/kafka-native:4.3.1` (fallback `apache/kafka:4.3.1`, same env vars) | Latest non-rc release (2026-06-23). The 4.3.2 and 4.4.0 tags are rc only. Compressed size 55 MB native vs 239 MB JVM (D-039). |
| MongoDB | `mongo:8.0.32` | The 8.0 line is a major release; 8.2/8.3 are newer but 8.0 is the conservative pick. Published 2026-10-02, 318 MB. |
| Kafka UI | `kafbat/kafka-ui:v1.5.0` | The latest semver release (2026-04-20). `main` and commit-SHA tags were rejected. The provectus image is unmaintained. |
| Base image | `python:3.11.17-slim-trixie` | The latest 3.11 patch release, on Debian 13 slim. 46 MB compressed. |
| kind | `v0.33.0` with `kindest/node:v1.36.4` (digest pinned in P11) | Matches kubectl client v1.36.1 within the supported skew |
| Python packages | confluent-kafka 2.16.0, pymongo 4.18.3, pydantic 2.14.0, fastapi 0.143.0, uvicorn 0.54.0; dev: ruff 0.17.0, pytest 9.1.1, httpx 0.28.1 | PyPI. confluent-kafka ships `cp311-win_amd64` and `cp311-manylinux_2_28_x86_64` wheels, so no compiler is needed. All packages support 3.11. |

- **Simplicity gate (httpx):** FastAPI's `TestClient` requires it, and the stdlib `urllib` cannot drive an ASGI app in-process. It is a dev-only dependency.
- **Shortcut:** pins are exact today. Upgrade deliberately (re-verify, re-run CI), never through floating tags.

### D-034 SIGINT handled the same as SIGTERM (Stage 1)
- **Why:** On the Windows dev host, Ctrl+C sends SIGINT, while in containers and Kubernetes, SIGTERM arrives. Both paths should behave the same.

### D-035 Kafka topic auto-create disabled (Stage 1)
- **Why:** A typo in a topic name would otherwise create a new topic silently and lose data into it. Topics come only from `ccp.common.topics`.

### D-036 Compose `app` profile for the containerised services (Stage 1)
- **Why:** One compose file serves both dev mode (infra only, the default) and full mode (`--profile app`), with no second file.

### D-037 API tests run against real MongoDB, not a mock library (Stage 1)
- **Options:** mongomock (a new dev dependency); a hand-written fake; the real MongoDB in Compose.
- **Choice:** The real MongoDB, as `integration` tests.
- **Why:** No extra dependency, and the tests exercise the real index and aggregation behaviour. Compose is already running for the other integration tests.
- **Trade-off:** API tests need infrastructure, so they run in the CI smoke job, not the unit job.

### D-038 Resume bullets reworded to match what is built (Gate 1)
- **Context:** Bullet 1 claimed "document-submission and clearance-status services", which this design does not have: its services are split by pipeline stage. Bullets 2 and 3 were true but vague, and left out the strongest provable work.
- **Options:** rename or restructure the services to fit the bullet; reword the bullet to fit the design.
- **Choice:** Reword. Inventing domain services to fit a resume line would add code with no engineering reason. The proposed wording and the evidence for each bullet are in IMPLEMENTATION_PLAN.md ("Resume claims → evidence").
- **Trade-off:** The final wording is frozen only in P12, after the evidence exists. Bullet 2 stays conditional on Hard Checkpoint 3.

### D-039 Memory budget for a 7.8 GB machine (Gate 1)
- **Context:** Measured: 7.8 GB RAM total, **0.8 GB free with Docker stopped**, 8 logical CPUs. A JVM Kafka, MongoDB with default cache, the JVM Kafka UI, four Python services and a kind control plane would not fit.
- **Options:** keep the default images and hope; drop Kafka UI; switch to lighter images and set explicit limits.
- **Choice:**
  1. Use `apache/kafka-native` (Apache's GraalVM build; no JVM heap to size), with `apache/kafka` as a drop-in fallback.
  2. Start MongoDB with `--wiredTigerCacheSizeGB 0.25`.
  3. Put Kafka UI behind the Compose profile `ui`, so it starts only when you need to look at topics.
  4. Set `mem_limit` in Compose and `resources.limits` in Kubernetes: kafka 512 MiB, mongo 512 MiB, kafka-ui 512 MiB, each consumer 192 MiB, api 256 MiB.
  5. **Never run Compose and kind at the same time.** P11 starts with `$C down`.
  6. Do **not** rely on `.wslconfig`: it lives on C: (D-040). The per-container limits in item 4 are the containment, and you can add `.wslconfig` yourself if you want an extra VM-level cap.
- **Why:** These limits are **budgets, not measurements**. Actual usage is recorded with `docker stats --no-stream` in P3 and `kubectl top` (or `docker stats` on the kind node) in P11.
- **Trade-off:** The native Kafka image is aimed at local development and testing, which is exactly this use, and it ships without the `kafka-*.sh` CLI tools. Consumer-group inspection therefore goes through a small AdminClient module (`python -m ccp.common.groups`), consistent with D-030.
- **Fallback:** if kind still does not fit in P11, the evidence is recorded honestly, bullet 2 is cut, and Release 1 stands on its own.

### D-040 D: drive only: every write, cache and piece of app data lives on D: (Gate 1, the user's instruction)
- **Context:** The user's instruction: "All the development should be in D drive only. Do not write any files in C drive. Only fetch the libraries from C drive. Even the app data should be stored in D drive."
  Measured: C: is 99% full (3.7 GB free) and Docker's WSL data already takes 4.8 GB of it, while D: has 75 GB free.
- **Rule:** Executables and libraries already installed on C: (uv, the Docker CLI, kubectl) may be **read and run**. Nothing the project produces is written to C:.
- **Layout:** the repo is `D:\custom clearance event pipeline\`, and all other data goes under **`D:\ccp-data\`** (outside the repo, so it never enters git or the Docker build context):

| What | Default location (on C:) | Redirected to | How |
|---|---|---|---|
| venv | — | `<repo>\.venv` | `uv venv` in the repo |
| uv-managed Python 3.11 | `%APPDATA%\uv\python` | `D:\ccp-data\uv-python` | `UV_PYTHON_INSTALL_DIR`, plus `--no-bin` so no shims go to `~\.local\bin` (verified with `uv 0.11.31 --help`) |
| uv package cache | `%LOCALAPPDATA%\uv\cache` | `D:\ccp-data\uv-cache` | `UV_CACHE_DIR` (verified) |
| pip cache (fallback) | `%LOCALAPPDATA%\pip\cache` | `D:\ccp-data\pip-cache` | `PIP_CACHE_DIR` |
| Temp files (pytest `tmp_path`, builds) | `%TEMP%` | `D:\ccp-data\tmp` | `TEMP` and `TMP` set for the dev shell |
| ruff, pytest and `__pycache__` caches | — | inside the repo (git-ignored) | default behaviour |
| Heartbeat files (host dev mode) | `/tmp` would resolve to the drive root | `<repo>\.run\<service>.heartbeat` | `HEARTBEAT_DIR` (TRD §14) |
| Docker images, volumes (Kafka and MongoDB data), build cache, kind node | `%LOCALAPPDATA%\Docker\wsl` | `D:\ccp-data\docker` | **User action:** Docker Desktop → Settings → Resources → Advanced → Disk image location |
| Docker CLI config (contexts, buildx state) | `%USERPROFILE%\.docker` | `D:\ccp-data\docker-config` | `DOCKER_CONFIG`. The existing contexts are copied there **read-only from C:**, and P0 checks that `docker context ls` still finds the `desktop-linux` engine. |
| kubeconfig and kubectl cache | `%USERPROFILE%\.kube` | `D:\ccp-data\kube\config`, `D:\ccp-data\kube\cache` | `KUBECONFIG`, `KUBECACHEDIR` |
| kind binary | winget installs to C: | `D:\ccp-data\bin\kind.exe` (v0.33.0 from the GitHub release, P11, asked first) | added to `PATH` for the dev shell |
| Simulator manifests, demo output, evidence logs | — | `<repo>\evidence\` | committed as evidence |

- **Mechanism:** one dev-shell script, `scripts/dev-env.ps1` (P0), sets every variable above for the current PowerShell session only. No user or system environment variables are changed, and nothing persistent is written to C:.
  **Simplicity gate:** without one script, nine variables would have to be typed by hand in every new shell.
- **Residual writes to C: that the project cannot control** (named honestly, not hidden):
  - Docker Desktop's own app logs and settings (`%APPDATA%\Docker`, `%LOCALAPPDATA%\Docker\log`) and its small `docker-desktop` WSL system distro
  - Windows itself, and the Claude Code session transcript (`%USERPROFILE%\.claude`)
  - `%USERPROFILE%\.wslconfig`, which would cap WSL memory (D-039). It is **not created**, because it would be a file on C:. The per-container memory limits in D-039 do the containment instead. You can add `.wslconfig` yourself if you decide the exception is worth it.
- **Trade-off:** one script to source per shell, and a one-time Docker disk move. The payoff is that C: stops filling up and the project stays self-contained on D:.

### D-041 Host Python 3.11 through uv, not a system install (Gate 1)
- **Context:** Measured: the host has Python 3.12, 3.10 and 3.9, but **no 3.11**. The stack is locked to 3.11. `uv` is already installed.
- **Options:** develop on 3.12 and trust CI for 3.11; install 3.11 system-wide; use uv-managed 3.11 per project.
- **Choice:** with `scripts/dev-env.ps1` sourced (D-040), run `uv python install 3.11 --no-bin` (it installs into `D:\ccp-data\uv-python`), then `uv venv --python 3.11 .venv`, then `uv pip install -e .[dev]`. No admin rights are needed, nothing is written to C:, and the system Python is untouched.
- **Simplicity gate:** pip cannot fetch an interpreter. uv is a **host tool only**: `pyproject.toml` stays standard, and the image and CI use stock Python 3.11 with pip, so the project has no uv dependency.
- **Trade-off:** One more tool appears in the README's setup steps.

### D-042 CI mirrors local commands exactly (Gate 1)
- **Context:** CI only goes green after the user pushes (rule 1), so a CI-only failure would surface late.
- **Choice:** Every CI step is a command that is also run locally in the phase acceptance (`ruff check .`, `ruff format --check .`, `pytest -q`, then the Compose smoke test). The workflow adds no logic of its own, so local green means CI green, with the first push as confirmation.
- **Trade-off:** Differences between the runner and Windows (paths, line endings) can still appear. The mitigations are `.gitattributes` with `* text=auto eol=lf` and Linux-only shell steps in CI.

### D-043 Strict schema parsing; code format belongs to the schema, code membership to the reference data (P1)
- **Context:** Each malformed field needs exactly one reason code, and lax parsing would quietly coerce bad types (`"1"` → `1`).
- **Choice:** Pydantic `strict=True` with `model_validate_json`, so a wrong JSON type is `SCHEMA_INVALID`. An HS code or UN/LOCODE with the wrong **shape**
  (`84713`, `rotterdam`) is `SCHEMA_INVALID`. A well-formed code that is **not in the CSVs** (`XXZZZ`, `999999`) is `UNKNOWN_REFERENCE_DATA`.
  `container_id` is any string in the schema, so a bad ID gets the more specific `CONTAINER_ID_INVALID`.
- **Why:** Each fault maps to one reason, and the simulator's unknown-reference fault must use well-formed codes to land in the right bucket (P4).
- **Trade-off:** Strict mode rejects some inputs a lenient consumer would accept, which is deliberate at a trust boundary.

### D-044 One `.env` at the repo root, read by Compose and by the host shell (P3)
- **Context:** With `-f deploy/compose/docker-compose.yml`, Compose looks for `.env` next to the Compose file, not at the repo root. Host services and tests read env vars only (D-023).
- **Choice:** `$C` passes `--env-file .env` explicitly, and `scripts/dev-env.ps1` loads the same `.env` into the session (plain `KEY=VALUE` lines).
- **Why:** One git-ignored file and one set of credentials, with no new dependency such as python-dotenv. Mongo's root user is created from it on the first `up`.
- **Trade-off:** After changing the Mongo password, the volume must be recreated (`$C down -v`), because the init variables only apply to an empty volume.

### D-045 No Kafka volume in Compose; integration test uses a scratch topic (P3)
- **Context:** The Kafka images run as a non-root user, and a fresh named volume at a new path is root-owned. That is a likely start-up failure, and it can only be tested once Docker runs.
- **Choice:** Kafka keeps its log in the container. Topics are recreated by `python -m ccp.common.topics` after each `up`. The host round-trip test creates and deletes its own `ccp.test.<random>` topic.
- **Why:** Each demo run starts fresh anyway, and the pipeline topics never carry test messages, so the M2 reconciliation stays exact.
- **Trade-off:** `$C down` (or recreating the container) drops all topics and consumer offsets; MongoDB data survives in its volume. Add a volume if a run must survive that.

### D-046 Host Kafka address is 127.0.0.1, not localhost (P3, found by the round-trip test)
- **Context:** On this Windows machine `localhost` resolves to `::1` (IPv6) first. Docker publishes the port on IPv4 only. The bootstrap connection falls back to IPv4, but Kafka then sends the client back to the advertised address, `localhost:9092`, which fails on `::1`. The produce call timed out.
- **Choice:** The EXTERNAL listener advertises `127.0.0.1:9092`, and the host default and `.env` use `127.0.0.1:9092`.
- **Why:** The advertised address is what the client reconnects to, so it must be the address that works from the host.
- **Trade-off:** Anything that hard-codes `localhost:9092` must change too. The `KAFKA_BOOTSTRAP_SERVERS` value in `.env` is the only one.

### P3 results (2026-10-13, Docker Desktop 29.7.2, Compose v5.4.0)
- **Stack:** `docker compose --env-file .env -f deploy/compose/docker-compose.yml up -d --wait` → kafka and mongo healthy. `kafka-ui` healthy with `--profile ui`, HTTP 200 on `/`.
- **Topics:** `python -m ccp.common.topics` created 3 topics (6, 6, 1 partitions). A second run reported all 3 as `exists` (idempotent). Kafka UI lists the same partition counts.
- **Tests:** `CCP_INTEGRATION=1 pytest -q -m integration tests/integration/test_infra.py` → 4 passed (topics exist, host produce and consume through `127.0.0.1:9092`, Mongo accepts the password, Mongo refuses anonymous reads). Unit suite: 95 passed, 4 skipped.
- **`docker stats --no-stream`, one reading, all three up:**

  | Container | Memory | Limit | CPU at the reading |
  |---|---|---|---|
  | kafka | 234.5 MiB | 512 MiB | 6.5% |
  | mongo | 191.5 MiB | 512 MiB | 1.8% |
  | kafka-ui | 272 MiB | 512 MiB | 101% (Kafka UI was still scraping metrics after start) |

  This is a single snapshot, not a load test. The CPU figure for kafka-ui needs a second reading after it settles before it counts as steady state.
- **A3 verified:** `apache/kafka-native:4.3.1` runs single-node KRaft with the two listeners and `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`. The `nc` health check passed. The fallback image was not needed.
- **A4 verified:** `kafbat/kafka-ui:v1.5.0` connects to the KRaft cluster and lists the topics.
- **Lesson recorded as D-046:** the host address must be `127.0.0.1`, not `localhost`, on this machine.
- **Residual C: writes:** none from the project. C: free space was 10.34 GB at the end of this phase.

### D-047 The simulator's `generate` takes the reference data as an argument (P4)
- **Context:** TRD §11 gives `generate(seed, config, start)`. Loading `data/ref` inside it would make it read files.
- **Choice:** `generate(seed, cfg, start, ref)`. The CLI loads the reference data once, and the tests load it from `data/ref`.
- **Why:** The generator stays pure, as §11 requires. The codes are sorted before the RNG picks from them, because frozenset order changes between processes.
- **Trade-off:** One extra argument. Verified: `--expect` gives byte-identical output under three different `PYTHONHASHSEED` values.

### P4 results (2026-10-14, Docker Desktop resumed, Compose stack restarted, topics recreated)
- **Run 1:** `python -m ccp.simulator --seed 42` produced 69 messages (clean 60, malformed 4, bad_container 2, duplicate 1, late 2).
  Read back from all 6 partitions: 69 messages. Checked with the real rules: SCHEMA_INVALID 4, CONTAINER_ID_INVALID 2, UNKNOWN_REFERENCE_DATA 0, 62 unique valid events, 1 duplicate on the wire. Matches `--expect` exactly.
- **Run 2:** `--seed 7 --unknown-ref-rate 0.5` produced 69 messages (unknown_reference 12, clean 50, malformed 4, bad_container 1, late 2). Combined topic total is 138, equal to the two manifests added together. UNKNOWN_REFERENCE_DATA 12, SCHEMA_INVALID 8, CONTAINER_ID_INVALID 3, 114 unique valid events, 1 duplicate. All match.
- **Tests:** 13 simulator tests; mutation checks caught a removed late-event hold-back and a no-op check-digit corruption. Full suite: 108 passed, 4 skipped. Lint and format clean.
- **Determinism:** `--expect` byte-identical under three `PYTHONHASHSEED` values.
- **Kafka UI:** the topic and its 6 partitions are visible. Its REST endpoint for per-partition counts returned 405, so the totals were checked with the Kafka client, not in the UI.
- **Not yet checked:** the timing of the live send loop beyond these two runs (about 15 s each); the hold-to-alert timing belongs to P7.

### D-049 One shared consume loop; a typed `Transient` error; SIGBREAK on Windows (P5)
- **Context:** The validator, processor and monitor all need the same loop: poll, handle, commit only after success, heartbeat, backoff, 5 attempts then `PROCESSING_ERROR` (TRD §8, §10, D-031).
- **Choice:** `ccp.common.kafka.run()` and the pure, unit-tested `process()`. A handler raises `Transient` for "infrastructure is down"; anything else counts as an unexpected error. Backoff sleeps in 0.5 s slices, touching the heartbeat and checking the stop flag. A failed commit is logged, not retried, because the message is already handled and at-least-once allows a repeat. The pure `route()` lives in `ccp/validator/route.py` rather than `__init__`/`__main__`, so tests import it without the service.
- **Why:** One tested loop instead of three copies. SIGBREAK is added to SIGTERM and SIGINT (D-034) because Ctrl+Break is the only signal one Windows process can send to another's process group, which is how the integration test proves a clean stop.
- **Trade-off:** A service whose handler wrongly raises a plain exception for an outage would DLQ messages after 5 tries instead of waiting. Handlers must map outage errors to `Transient` (the validator maps Kafka produce and delivery errors).

### P5 results (2026-10-14)
- **Unit:** `tests/test_validator_route.py` (13) and `tests/test_run_loop.py` (6). Mutations caught: one extra attempt before the DLQ; backoff sleeping in one block. Full suite: 127 passed, 5 skipped. Lint and format clean.
- **Integration:** `CCP_INTEGRATION=1 pytest -m integration tests/integration` → 5 passed. A clean event reached `validated`; a malformed, a bad-container and an unknown-reference event reached the DLQ with the right `reason` and `source_topic`; Ctrl+Break gave exit code 0 with `consumer closed` and `stopped` logged (A8).
- **Backlog check:** the validator drained the 138 P4 messages plus the 4 test messages to lag 0, then stopped with exit code 0. `validated` = 116 (P4 manifests: 114 unique + 1 duplicate; plus 1 test event). DLQ: SCHEMA_INVALID 9 (8 + 1), UNKNOWN_REFERENCE_DATA 13 (12 + 1), CONTAINER_ID_INVALID 4 (3 + 1), PROCESSING_ERROR 0. All exact.
- **Not verified:** behaviour while Kafka is actually down mid-run (the backoff path is unit-tested only); a real Ctrl+C typed in a console (the same handler as Ctrl+Break, but not exercised).

### D-050 `CCP_GROUP_SUFFIX` isolates test consumers; processor tests use a throwaway database (P6)
- **Context:** The kill-and-restart test runs the real processor against the shared dev Kafka. With the fixed group `ccp-processor` it would move the real group's offsets, and with the `ccp` database it would mix test data into the demo state.
- **Choice:** One env var, `CCP_GROUP_SUFFIX` (default empty), appended to every group id. Processor tests set it and `MONGO_DB=ccp_it_<random>`, prefix their shipment ids, and drop the database afterwards. The replay, shuffle and crash-healing tests call `store()` directly against MongoDB, without Kafka. The kill-and-restart test uses the real service through Kafka.
- **Why:** The smallest change that keeps the dev stack's real groups and database clean, with no topic-name config.
- **Trade-off:** Test groups (`ccp-processor-it-*`) are left in Kafka. They are harmless and vanish on `$C down` (D-045). The in-process tests do not exercise Kafka redelivery; the kill test covers that path.

### P6 results (2026-10-14) — HARD CHECKPOINT 1 evidence
- **Integration:** `CCP_INTEGRATION=1 pytest -q -m integration tests/integration` → 9 passed in 63 s (4 infra, 1 validator, 4 processor):
  (a) replay twice: `shipment_status` identical apart from `updated_at`, `events` count unchanged, `duplicates_dropped` = injected + replayed;
  (b) 3 shuffled arrival orders give identical status documents, equal to the fold of the plan;
  crash between insert and status write: the redelivered duplicate recomputes and heals;
  (c) the processor subprocess was hard-killed after its first events were stored (mid-run, asserted), then restarted; final states equal the fold of the plan, and it then stopped with exit code 0.
- **Mutations caught:** returning early on a duplicate (no recompute) failed the replay and crash-healing tests; not counting duplicates failed the replay test.
- **Demo on a fresh stack:** validator + processor + `python -m ccp.simulator --seed 42`, zero lag after 15.4 s, both services exited 0. MongoDB and the DLQ match `--expect` exactly: stored 62, duplicates 1, DLQ SCHEMA_INVALID 4 and CONTAINER_ID_INVALID 2, PROCESSING_ERROR 0, states CLEARED 6 / DOCS_VERIFIED 1 / FILED 2 / ON_HOLD 2 / REJECTED 4 / RELEASED 4, anomalies 1. **Correction (found in P8):** this run compared every state except UNKNOWN, and claimed the manifest's one UNKNOWN shipment had no
status document. That was wrong: its events are valid but illegal from UNKNOWN, so it has a status document in state UNKNOWN. The live
`GET /stats` on the same database shows `UNKNOWN: 1`, which equals the manifest, so the run matched on all seven states.
- **Not verified:** MongoDB going down mid-run (the transient path is unit-tested in P5 only); two processors overlapping on one shipment during a rebalance (the `event_count` guard, D-027, is exercised only by its own logic, not by a real rebalance; P11 scales to 3 replicas); recompute cost on long histories (A9).

### Phase 6 limit fixes (2026-10-14, at the user's request after HARD CHECKPOINT 1)
Each limit was first reproduced on the unchanged code with `D:\ccp-data\tmp\repro_limits.py` (a throwaway script), then fixed,
then mutation-checked: the fix was reverted and its test had to fail.

| # | Limit | Kind | Evidence on the old code | Fix | Test that fails without the fix |
|---|---|---|---|---|---|
| 1 | MongoDB hung mid-run | Backend (driver configuration) | MongoDB paused 20 s: heartbeat 20.7 s stale (a call blocks forever) | 5 s socket, connect and server-selection timeouts (D-053) | `test_mongo_hung_mid_run_keeps_the_heartbeat_fresh_and_processing_resumes` |
| 2 | Overlapping recompute | Logic (race) | A read that missed a concurrent insert left `ON_HOLD`, 3 events (correct: `CLEARED`, 4); the guard could not see it because the counts tied | Re-count after the write and fold again if it moved (D-052); guard kept and tested | `test_a_read_that_missed_a_concurrent_insert_is_repaired`, `test_older_computation_never_overwrites_a_newer_status` |
| 3 | Recompute cost | Design limit (performance), not a defect | Unmeasured | Measured and bounded; `shortcut:` comment with the upgrade trigger (D-055) | None (performance); `scripts/bench_recompute.py` reproduces the numbers |
| 4 | Duplicate counter | Logic | One message redelivered once counted as 1 duplicate (correct: 0) | Duplicates keyed by Kafka coordinates; a redelivery of the same message is not a duplicate (D-051) | `test_redelivering_the_stream_changes_nothing`, `test_a_redelivered_message_is_not_a_duplicate_but_a_second_message_is` |
| 5 | MongoDB down at start-up | Code (missing retry) | Exit code 1 after 5 s (`ServerSelectionTimeoutError`) | `wait_until()` retries index creation with backoff and heartbeat (D-053) | `test_processor_waits_for_mongo_at_startup`; unit: `tests/test_wait_until.py` |
| 6 | Test consumer groups left behind | Code (test hygiene) | A `ccp-processor-*` group persisted after a run that committed offsets | Session fixture deletes test-prefixed groups (D-054) | Disabling it left `ccp-processor-it-*` behind; restored, 0 left |

**Also found while fixing:** the kill test passed once and failed on a second run, because a fixed seed repeats `event_id`s and the previous
run's events were still on the topic. Fixed by deriving a per-run `event_id` (D-054). The integration suite then passed **twice in a row: 16/16
each (159 s)**. Unit suite: 131 passed. Lint and format clean.

### D-051 Duplicates are identified by Kafka coordinates and derived, not counted (P6 fix; supersedes the `stats` counter in D-017's trade-off)
- **Context:** `$inc stats.duplicates_dropped` counted every `DuplicateKeyError`, including a redelivery of the **same** message after a crash
  or rebalance, and could over-count by one per crash. M2 (`produced = stored + duplicates + rejected`) was only exact without crashes.
- **Choice:** each stored event records `source_message = {topic, partition, offset}`. On `DuplicateKeyError`, the same coordinates mean a
  redelivery (not counted); different ones mean a true duplicate, upserted into `duplicates` with `_id = "topic:partition:offset"`.
  `duplicates_dropped` is that collection's count. The `stats` collection is gone.
- **Why:** an upsert by coordinates is idempotent, so no crash point can double-count, and nothing has to be atomic with the commit.
- **Trade-off:** one extra `find_one` on each duplicate, and one small document per duplicate. A validator that re-emits after a crash still
  creates real duplicates on `validated`, and they are counted (TRD §17).

### D-052 Re-count after writing the status; fold again if a concurrent insert was missed (P6 fix)
- **Context:** two workers on one shipment (a rebalance, or P11's 3 replicas) can each read the events while the other inserts. A read can miss
  an event that another worker inserted at the same time. If the two reads have the same size, the `event_count` guard lets the stale one
  through, and no later event repairs it. Reproduced deterministically with a read that drops the newest event.
- **Choice:** after the guarded write, `count_documents` for the shipment; if it differs from the folded count, fold again (at most 5 times;
  every insert runs its own recompute, so the last inserter always finishes).
- **Why:** it closes the hole with one indexed count per event, and the guard keeps older results from overwriting newer ones.
- **Trade-off:** one extra query per event. Proven by a deterministic stale-read test and an 8-thread overlap test (3 rounds); a real Kafka
  rebalance is still observed in P11 (A13).

### D-053 MongoDB timeouts and a start-up wait (P6 fix)
- **Context:** without a socket timeout, a hung MongoDB (paused container, dead network path) blocks a call forever: the heartbeat goes stale and
  Kubernetes would restart a pod that cannot fix MongoDB. Separately, the processor exited if MongoDB was down when it started.
- **Choice:** `socketTimeoutMS`, `connectTimeoutMS` and `serverSelectionTimeoutMS` all 5 s, so a hang becomes `AutoReconnect` and goes through
  the normal backoff. `ccp.common.kafka.wait_until()` retries start-up work (index creation) with the same backoff and heartbeat.
- **Why:** an outage must look like an outage: wait, keep the heartbeat fresh, resume, never crash-loop.
- **Trade-off:** a legitimately slow MongoDB call over 5 s now fails and is retried. Measured worst-case heartbeat gap during a 25 s pause:
  **14.7 s** (three stacked timeouts), so the P11 liveness probe threshold must be at least 45 s.

### D-054 Integration tests clean up their consumer groups and never reuse event ids across runs (P6 fix)
- **Context:** groups that committed offsets (`ccp-processor-it-*`) stayed in the shared dev Kafka. Topics also outlive a run (D-045), so a
  fixed-seed stream repeated its `event_id`s and collided with the previous run.
- **Choice:** a session fixture in `tests/integration/conftest.py` deletes groups with the test prefixes after the run (the real
  `ccp-validator` and `ccp-processor` are never touched) and fails loudly if a deletion fails. Prefixed test streams derive their
  `event_id` with `uuid5(prefix + id)`.
- **Why:** the suite must be repeatable against the same stack. **Remaining:** the validator integration test still uses the real
  `ccp-validator` group, because a fresh group would re-validate the whole `raw` history onto the shared topics.

### D-055 Recompute cost measured and accepted (P6, limit 3, A9)
- **Measured** (`python scripts/bench_recompute.py`, 2026-10-14; Windows 11, 7.8 GB RAM, Docker Desktop 29.7.2, `mongo:8.0.32` with a
  0.25 GB cache; worst case where every event after the first is illegal, so the status also stores one anomaly per event): median per
  event 6.2 ms at 10 events, 8.7 ms at 100, 26.4 ms at 1,000, 59.2 ms at 5,000. Roughly linear in history length.
- **Choice:** keep the full recompute. A customs lifecycle has under 10 events. A `shortcut:` comment in `store.py` names the upgrade
  (an incremental fold when the new event sorts last) and its trigger (histories past about 1,000 events).

### D-056 Monitor: periodic scan as a `tick` on the shared loop; unreadable DLQ records are logged and skipped (P7)
- **Context:** TRD §12 wants one process that polls the DLQ and scans holds every `MONITOR_SCAN_SECONDS`. The shared `run()` had no periodic hook.
  A DLQ record the sink cannot parse has nowhere further to go.
- **Choice:** `run(..., tick=...)` calls `tick()` once per loop iteration (about every second); the monitor's tick runs `scan_holds` when the interval
  has passed. A failing tick is logged (`Transient` as a warning, anything else with a traceback) and retried next interval, never stopping the loop.
  After 5 failed attempts an unreadable DLQ record is logged at ERROR with its partition and offset, then committed.
- **Why:** no second thread or scheduler; the heartbeat keeps covering both jobs. The DLQ must never block on a bad record.
- **Trade-off:** a skipped DLQ record exists only in the log and Kafka, not in `rejections`. It cannot come from our validator or processor, which
  always write well-formed records.

### P7 results (2026-10-14)
- `CCP_INTEGRATION=1 pytest -q -m integration tests/integration/test_monitor.py` → 5 passed (23 s): only holds older than the threshold alert;
  the same hold never alerts twice; a new hold alerts again; rejections are keyed by the original message; **the real monitor process restarted
  twice still leaves exactly one alert**; **a fresh consumer group replaying the whole DLQ leaves the rejection count unchanged (3)**.
- Unit: `tests/test_run_loop_tick.py` (tick errors never stop the loop). Full unit suite 132 passed; lint and format clean.
- Mutations caught: alert index without `unique` (2 tests fail); an unstable rejection key (2 tests fail). A first rejection mutant (an extra filter
  field) survived because `_id` uniqueness still held; that mutant did not model a real fault, so it was replaced by the unstable-key one.
- Test consumer groups after the run: 0 left (cleanup prefix list extended with `ccp-monitor-dlq-it-`).

### D-057 API: sync endpoints, the database injected, `/health` from the driver's cached topology (P8)
- **Context:** pymongo is blocking. Tests must run against real MongoDB (D-037). A liveness probe has about 1 s, while a ping to a down
  MongoDB waits for the 5 s server-selection timeout (D-053).
- **Choice:** plain `def` endpoints (FastAPI runs them in its threadpool, so the event loop never blocks). `create_app(db)` takes the
  database, so tests pass a throwaway one. `/health` reads `client.topology_description.has_readable_server()`: no network call, always 200.
  `/rejections/summary` lists all four reason codes, zeros included, so the demo can compare it field by field. `API_HOST`/`API_PORT`
  were added to the config (127.0.0.1:8000 on the host; containers set 0.0.0.0).
- **Why:** correct concurrency without an async driver; a health check that cannot be made to fail by MongoDB being slow.
- **Trade-off:** `mongo: ok/down` can lag reality by up to the driver's 10 s heartbeat. Offset pagination stays (shortcut comment in `app.py`).

### P8 results (2026-10-14)
- `CCP_INTEGRATION=1 pytest -q -m integration tests/integration/test_api.py` → 16 passed: every endpoint, 404s, `limit` 201/0 and `offset` -1
  → 422, `limit=200` → 200, an unknown `state` → 422, state filter, fold order of events, newest-first alerts, the summary with zero
  codes, `/stats`, Swagger and OpenAPI, and `/health` staying 200 in under 1 s with `mongo: down` against an unreachable server.
- Live: `python -m ccp.api` → `/health` `{"status":"ok","mongo":"ok"}`, `/docs` 200, OpenAPI lists all 8 paths, `/shipments?limit=201` 422,
  and the logs are JSON (uvicorn's loggers propagate to ours).
- Noted, not acted on: `fastapi.testclient` warns that starlette prefers `httpx2`. It is a deprecation warning with the pinned versions (D-033),
  and nothing fails.

### D-058 The demo owns `ccp_demo` and the `-demo` consumer groups, and starts them at the end of each topic (P9)
- **Context:** `python -m ccp.demo` must exit 0 only on an exact match, but topics persist between runs (D-045) and earlier tests leave
  messages on `raw` and `validated` that the real groups never consumed. Counting those would break the match.
- **Choice:** in dev mode the demo drops and recreates only the `ccp_demo` database, commits the offsets of `ccp-{validator,processor,
  monitor-dlq}-demo` at the current end of each topic, then starts the four services with `MONGO_DB=ccp_demo` and `CCP_GROUP_SUFFIX=-demo`.
  It polls `/stats` until everything matches (default 180 s), then waits `HOLD_ALERT_SECONDS + 2 × MONITOR_SCAN_SECONDS` and checks again,
  so a late, wrong alert still fails the run (TRD §11). `--verify-only` skips the reset and spawning, for Compose and kind.
- **Why:** the demo is repeatable on a long-lived dev stack without touching the real `ccp` database or the real groups.
- **Trade-off:** the three `-demo` groups stay in Kafka on purpose (they hold the demo's position). `--verify-only` assumes a fresh stack.

### P9 results (2026-10-14) — HARD CHECKPOINT 2 evidence
- `python -m ccp.demo --seed 42` → **RESULT: MATCH, exit 0, 72 s**. All 16 values equal the manifest: produced 69 (= stored 62 +
  duplicates 1 + rejected 6, M2), anomalies 1, hold alerts 2, DLQ CONTAINER_ID_INVALID 2 / SCHEMA_INVALID 4 / others 0, and all seven
  state counts, UNKNOWN included. **A10 verified.**
- The verifier can fail: `--verify-only --seed 7` against the seed-42 data → `RESULT: MISMATCH` (anomalies, alerts, duplicates differ),
  **exit 1**. `--verify-only --seed 42` → MATCH, exit 0.
- Full integration suite including the smoke test: **38 passed in 257 s**. Unit suite: 134 passed. Lint and format clean.
- README v1 written: the recorded demo summary, what is verified with links to the tests, known limits, and P10–P12 marked as planned.

### D-059 Image, Compose `app` profile, heartbeat probe and CI (P10)
- **Image:** multi-stage on `python:3.11.17-slim-trixie`; the builder makes wheels, the runtime installs them with `--no-index`, runs as
  uid 10001, sets `HEARTBEAT_DIR=/tmp`, `API_HOST=0.0.0.0`, `REF_DATA_DIR=/app/data/ref`, and uses an exec-form CMD.
- **Compose:** `topics-init` (one-shot) → validator, processor, monitor, api under `profiles: [app]`, sharing one `x-ccp` anchor (image,
  memory limit, env with `kafka:19092` and `mongo`). `MONGO_DB` is interpolated (`${MONGO_DB:-ccp}`), so a run can use a fresh database
  without deleting anything. `simulator` is under `profiles: [tools]` for `compose run`.
- **Probe:** `python -m ccp.common.probe <service> [max_age]` (default 60 s, above D-053's 14.7 s worst case). Compose uses it now; the kind
  exec probes reuse it in P11. Unit-tested (`tests/test_probe.py`, 4 tests).
- **CI:** `.github/workflows/ci.yml`, three jobs that run the local commands (D-042): lint + unit; Compose + integration suite (includes the
  smoke demo); image build + `app` profile + `compose run simulator` + `ccp.demo --verify-only`. Action tags pinned exactly after checking
  the GitHub releases API: `actions/checkout@v7.0.1`, `actions/setup-python@v7.0.0`.
- **Trade-off:** CI cannot be proven green until the user pushes (rule 1). The workflow YAML was not validated by GitHub yet.

### P10 results (2026-10-14)
- `docker build -t ccp:0.1.0 .` → exit 0 in 31 s; **image 268 MB** (`docker image ls`, Docker Desktop 29.7.2).
- Fresh stack (`$C down`, then `MONGO_DB=ccp_r1`): `$C --profile app up -d --wait` → every service healthy in 14 s, `topics-init` exited 0.
  `$C run --rm simulator --seed 42` → 69 messages. `python -m ccp.demo --seed 42 --verify-only` → **RESULT: MATCH, exit 0** on the fully
  containerised pipeline (all 16 values, as in P9).
- Memory (`docker stats --no-stream`, one reading after the run): kafka 239 MiB / 512, mongo 204 / 512, api 41 / 256, processor 42 / 192,
  validator 37 / 192, monitor 25 / 192. Well inside the D-039 budgets.
- **A11 verified:** `$C stop -t 30 validator processor monitor` took 2 s (a SIGKILL would take 30 s); each container exited 0 and logged
  `stop requested`, `consumer closed`, `stopped`, so SIGTERM reaches Python as PID 1.
- **Not verified:** CI (needs the user's push, A12); the workflow has not run anywhere yet.

### D-060 Kubernetes manifests for kind (P11 preparation; applied on 2026-10-14, see D-061 and the P11 results)
- **Choice:** plain manifests in `deploy/k8s/` (ARCHITECTURE §7): Kafka and MongoDB as single-replica StatefulSets (Kafka without a volume,
  as D-045; MongoDB with a 1 Gi PVC on kind's default StorageClass), a ConfigMap, a Secret created at deploy time from `.env`'s two
  `MONGO_*` keys (never committed), a topics Job, four Deployments with the D-039 memory limits, and a simulator Job with `backoffLimit: 0`.
  Consumers use `python -m ccp.common.probe` as an exec liveness probe (60 s, D-053/D-059); the API uses HTTP probes on `/health` (D-057).
  The kind node image is pinned by digest, as listed in the kind v0.33.0 release notes.
- **Rebalance evidence:** the shared loop now logs `partitions assigned` / `partitions revoked` with the partition list (`_log_partitions`,
  unit-tested; seen in the validator's log), and `python -m ccp.common.groups <group>` describes members and partitions through the
  AdminClient (integration-tested: 1 member, all 6 `raw` partitions, STABLE). On kind it runs inside a pod, because the broker is in-cluster only.
- **Was blocked on:** downloading `kind-windows-amd64` v0.33.0 (10.34 MB, kubernetes-sigs/kind GitHub release, published 2026-08-26, with a
  `.sha256sum` file). Downloads need the user's explicit approval, which an overnight "complete all phases" did not give; the user gave it
  on 2026-10-14. The manifests were unvalidated until then: no YAML parser is installed and `kubectl apply --dry-run=client` needs a
  cluster. Adding PyYAML only to lint them was not worth a dependency; `kubectl apply` validated them (and found D-061).

### D-061 `enableServiceLinks: false` on every pod (P11)
- **Context:** the first `kubectl apply` put all four services into CrashLoopBackOff: `ConfigError: API_PORT must be an integer, got
  'tcp://10.96.228.79:8000'`. Kubernetes injects Docker-link variables (`<SERVICE>_PORT`, `<SERVICE>_SERVICE_HOST`, ...) for every Service in the
  namespace, so the Service named `api` sets `API_PORT`, which is also our own setting. `kafka` and `mongo` have the same exposure
  (`MONGO_PORT` only survived because the ConfigMap sets it explicitly, which overrides the injected value).
- **Options:** rename our setting (touches config, `.env.example`, Compose, docs); rename the Service (fragile, any future Service named
  after one of our prefixes collides again); turn the injection off.
- **Choice:** `enableServiceLinks: false` in all eight pod specs. Services are found through DNS (`kafka`, `mongo`, `api`), never through link
  variables, and Compose never injected them, so only Kubernetes needed the fix. `Config` rejecting the bad value was correct and is unchanged.
- **Test:** `tests/test_k8s_manifests.py` is a static text check that every manifest with a pod spec sets it. Red first: 8 failed; after the
  edit: 9 passed. It guards the manifests only; the cluster run below is what proved the pods start.
- **Trade-off:** the test is a string check, not a schema validation of the YAML (no parser dependency, D-060).

### P11 results (2026-10-14; kind v0.33.0, Docker Desktop on D:)
- **kind download:** `kind-windows-amd64` v0.33.0 to `D:\ccp-data\bin\kind.exe`, 10,844,160 bytes. SHA-256
  `4b22adaa135368c5a465d56bbd8e520cbea87272a06ca00b6078e7b81515c9fc` equals the published `.sha256sum`. `kind version` → `v0.33.0 go1.26.7
  windows/amd64`. kubectl client v1.36.1 (the one Docker Desktop ships).
- **Cluster:** `kind create cluster --config deploy/k8s/kind-config.yaml` → exit 0, one control-plane node, `kindest/node:v1.36.4` by digest.
- **`kind load docker-image` failed** for all three images: `ctr: content digest sha256:5d72... not found` (the Docker image store keeps
  multi-platform manifests whose blobs the node's `ctr import` cannot find). Workarounds: Kafka and MongoDB were pulled by the kubelet from
  Docker Hub under the same pinned tags; `ccp:0.1.0` was loaded with `docker save` to a tar on D: and `kind load image-archive`. The tar was
  deleted afterwards. The README step was updated.
- **Stale image found:** the P10 `ccp:0.1.0` predated `ccp.common.groups` and the rebalance logging (`No module named ccp.common.groups`).
  Rebuilt, reloaded, `kubectl rollout restart`. Lesson: kind does not see a rebuilt image until it is loaded again and the pods restart.
- **Bring-up:** after D-061, all pods `Running` and `1/1`, `topics-init` Completed (raw 6, validated 6, dlq 1 partitions, from its log).
- **Demo on kind:** the `simulator` Job (seed 42) produced 69 messages (clean 60, malformed 4, bad_container 2, duplicate 1, late 2).
  `python -m ccp.demo --seed 42 --verify-only` through `kubectl port-forward svc/api 8000:8000` → **RESULT: MATCH, exit 0** (all 16 values;
  48 s including the 30 s late-alert recheck).
- **Rebalance (A-scale, hard checkpoint 3):** `python -m ccp.common.groups ccp-processor` inside the api pod, group STABLE with 3 members,
  two partitions each: `[0,1]`, `[2,3]`, `[4,5]`. Scaled 3 → 1: the survivor logged `partitions revoked` then `partitions assigned` with all six
  (`validated[0..5]`) and `groups` showed 1 member, 6 partitions. Scaled 1 → 3: the survivor logged `revoked [0..5]` then `assigned [2,3]`, and
  the new pods logged `assigned [0,1]` and `assigned [4,5]` (09:04:11 UTC, within the same second). `groups` again showed 3 members, 2 each.
  After all of it `--verify-only --seed 42` still gave **RESULT: MATCH**.
- **Memory (A2):** `docker stats --no-stream ccp-control-plane` → 1.667 GiB of 3.744 GiB (44.51%) with everything running and 3 processors.
  One reading, after the rebalances.
- **Not verified:** the pods were not killed to test liveness-probe restarts on kind (the probe is unit-tested and ran in Compose); no run longer
  than a few minutes; Kafka has no volume, so a broker pod restart loses the topics (D-045; the topics Job re-creates them).

### D-048 A message is clean, late, duplicate or rejected, never more than one (P4)
- **Context:** TRD §11 lists the faults but not how they combine. Stacking faults on one message would make the expected counts hard to read.
- **Choice:** A rejected message (malformed, bad container, unknown reference) is never also late or duplicated. A late or duplicate message is always a valid event. Illegal sequences are a shipment-level script change, so they produce anomalies in the fold, not a message label.
- **Why:** Each message has one label, so the manifest reconciles exactly: `produced = stored + duplicates + rejected` (M2).
- **Trade-off:** Rates are independent per event, so a fault can be suppressed by another. The rates are approximate shares, not exact counts.

---

## Environment record (measured 2026-10-11, before P0)

| Item | Value | Status |
|---|---|---|
| OS | Windows 11 Home 10.0.26300, 8 logical CPUs | — |
| RAM | 7.8 GB total, 0.8 GB free with Docker stopped | **Tight.** See D-039; close other heavy apps before running the stack |
| Disk | C: 3.7 GB free (99% used); D: 75 GB free | **C: is critical.** See D-040 |
| Docker | client 29.7.2, Compose v5.4.0; **daemon not running** | Start Docker Desktop before P3 |
| Docker data | 4.8 GB in `%LOCALAPPDATA%\Docker\wsl` on C: | **User action:** move the disk image to `D:\ccp-data\docker` (D-040) |
| kind | **not installed** | Needed only for P11: `kind.exe` v0.33.0 into `D:\ccp-data\bin` (D-040; not winget, which writes to C:) |
| kubectl | client v1.36.1 | OK |
| Python | 3.12.0 (default), 3.10, 3.9; **no 3.11** | uv-managed 3.11 (D-041) |
| uv | installed (`~\.local\bin\uv.exe`) | OK |
| .wslconfig | none (WSL2 memory uncapped) | Not created, because it would be on C: (D-040). Per-container limits are used instead (D-039). |

### P0 results (2026-10-11 17:41–17:45 IST)
- **C: free:** 5.602 GB before → 5.600 GB after. Under `uv`, `pip`, `Python`, `.local\bin`, `.docker` and `.kube` on C:, **0 files** were created or changed.
  The only new files on C: are Claude Code's own session counters and task logs in `%TEMP%` (a residual already listed in D-040).
- **Host Python:** `uv python install 3.11 --no-bin` installed **3.11.15** (uv's newest 3.11 build) into `D:\ccp-data\uv-python`. The image pins 3.11.17 (D-033).
  Both are 3.11 patch releases, so no API differences are expected, and the image and CI remain the reference.
- **A6 (host): verified.** `confluent-kafka 2.16.0` (librdkafka 2.16.0) installed from a wheel; pymongo 4.18.3, pydantic 2.14.0 and fastapi 0.143.0 all import.
- **A14: verified.** With `DOCKER_CONFIG=D:\ccp-data\docker-config`, `docker context ls` still lists `desktop-linux *`. Engine reachability is checked in P3, once Docker Desktop is running.
- **Fix during P0:** `dev-env.ps1` no longer sets a global `$ErrorActionPreference = 'Stop'`. When dot-sourced, it leaked into the shell, and PowerShell 5.1 then
  treated uv's stderr progress as a fatal error.

**Verdict:** Release 1 (Compose) is feasible once the D-040 user action (moving the Docker disk image to D:) is done and Docker Desktop is running. Release 2 (kind) is **at risk** on this machine and will be proven or cut in P11, with the measurements recorded.

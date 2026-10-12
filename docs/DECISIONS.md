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

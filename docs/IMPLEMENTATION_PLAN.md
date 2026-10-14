# Implementation plan

> **Synthetic data** project. **Release 1** = P0–P10 (the full pipeline in Compose, plus CI). **Release 2** = P11–P12 (kind, polish).
> Hard checkpoints come after **P6, P9 and P11**. After every phase: print `PHASE n DONE`, what changed, the acceptance output, and a suggested commit message, then wait.
> Commands are shown one per line so they work in both PowerShell 5.1 and bash. `$C` stands for
> `docker compose --env-file .env -f deploy/compose/docker-compose.yml`, run from the repo root (D-044).

## Before P0: user actions (one-time; the only steps that are yours, D-040)
1. Docker Desktop → Settings → Resources → Advanced → **Disk image location** → `D:\ccp-data\docker`, then apply (this moves the existing 4.8 GB off C:).
2. Start Docker Desktop and leave it running from P3 on. Close heavy apps before running the stack (RAM, D-039).

## P0: Scaffold and environment check
- **Goal:** an empty project that lints and tests clean, with the host set up so that nothing is written to C:.
  (The environment was already measured at Gate 1; see the DECISIONS.md environment record.)
- **Files:** `scripts/dev-env.ps1` (session-only env vars redirecting the uv, pip, temp, Docker CLI and kube locations to `D:\ccp-data`),
  `pyproject.toml` (exact pins from D-033, ruff `target-version = "py311"`, pytest config, `integration` marker), `.gitignore`
  (`.env`, `.venv`, `.run`, caches), `.gitattributes` (`* text=auto eol=lf`), `.dockerignore`, `.env.example`,
  `src/ccp/__init__.py`, `src/ccp/common/{__init__,config,log}.py`, `tests/test_config.py`, `Dockerfile` (skeleton), `README.md` (stub with the D:-only setup).
- **Acceptance:** `. .\scripts\dev-env.ps1` · `uv python install 3.11 --no-bin` · `uv venv --python 3.11 .venv` ·
  `uv pip install -e .[dev]` · `python --version` (expect 3.11.x, from a path under `D:\ccp-data\uv-python`) ·
  `docker context ls` (the engine is still found with the redirected `DOCKER_CONFIG`; A14) · `ruff check .` · `ruff format --check .` · `pytest -q`.
  **C: check:** C: free space recorded before and after the phase, with no project-attributable growth.
- **Commit:** `chore: scaffold project with D-drive dev env, config loader, JSON logging, lint and tests`
- **Risks:** A6 (the wheel exists, but the install is unproven); A14 (if the redirected `DOCKER_CONFIG` loses the Docker Desktop context, fall back
  to leaving the Docker CLI config where it is, record that as a residual write in D-040, and tell you).

## P1: Reference data, ISO 6346 and event schema
- **Goal:** the pure validation building blocks (TRD §2–4).
- **Files:** `data/ref/ports.csv`, `data/ref/hs_codes.csv`, `src/ccp/common/{iso6346,refdata,schema}.py`,
  `tests/test_{iso6346,refdata,schema}.py`.
- **Acceptance:** `pytest -q tests/test_iso6346.py tests/test_refdata.py tests/test_schema.py` covering valid IDs (including the published
  example `CSQU3054383`), a bad check digit, bad formats, each schema rejection path, a naive timestamp, and an unknown version.
- **Commit:** `feat(common): reference data, ISO 6346 check digit and Pydantic event schema`
- **Risks:** an off-by-one in the letter-value table. The mitigation is to test against published examples.

## P2: Pure state fold
- **Goal:** `fold(events) -> FoldResult` as specified in TRD §5.
- **Files:** `src/ccp/common/fold.py`, `tests/test_fold.py`.
- **Acceptance:** `pytest -q tests/test_fold.py` covering every (state × event) pair against the transition table, terminal states, anomaly
  healing by a late event, tie-break ordering, and an `itertools.permutations` test over a full legal lifecycle (including the hold path) that
  always yields the same result.
- **Commit:** `feat(common): event-time state fold with derived anomalies`
- **Risks:** none significant; this is pure code.

## P3: Compose infrastructure
- **Goal:** Kafka (KRaft), MongoDB (auth) and Kafka UI running healthy, with topics created and the host client working.
- **Files:** `deploy/compose/docker-compose.yml`, `src/ccp/common/topics.py`, `src/ccp/common/kafka.py` (producer and consumer
  factories), `tests/integration/test_infra.py`, ARCHITECTURE §5–6 (verified tags).
- **Acceptance:** `$C up -d --wait` · `python -m ccp.common.topics` (run twice, which proves it is idempotent) ·
  `pytest -q -m integration tests/integration/test_infra.py` (with `CCP_INTEGRATION=1`; a host round-trip produce and consume, plus an authenticated MongoDB ping) ·
  `$C --profile ui up -d` and Kafka UI shows the 3 topics · `docker stats --no-stream` recorded in DECISIONS.md (A1).
- **Commit:** `feat(deploy): compose infra with KRaft Kafka, MongoDB auth, Kafka UI and topic bootstrap`
- **Risks:** runtime behaviour of the pinned tags (A3, A4, A5; fall back to `apache/kafka:4.3.1` if the native image misbehaves), listener misconfiguration, RAM (A1).

## P4: Simulator
- **Goal:** a seeded, fault-injecting producer CLI plus the `--expect` manifest (TRD §11).
- **Files:** `src/ccp/simulator/{__init__,__main__,generate}.py`, `tests/test_simulator.py`.
- **Acceptance:** `pytest -q tests/test_simulator.py` (same seed and start give identical events; each fault type appears at the expected count) ·
  `python -m ccp.simulator --seed 42 --expect` run twice gives identical output · `python -m ccp.simulator --seed 42` produces to
  `customs.events.raw`, with each fault type visible in Kafka UI (described in the phase report).
- **Commit:** `feat(simulator): seeded event generator with fault injection and expected-outcome manifest`
- **Risks:** idempotent producer on a single broker (A7); timing constraints (TRD §11).

## P5: Validator
- **Goal:** route raw events to `validated` or `dlq` with reason codes. Add SIGTERM/SIGINT handling, heartbeat and backoff (TRD §4, §8, §10).
- **Files:** `src/ccp/validator/{__init__,__main__}.py` (with the pure `route(raw) -> (topic, payload)`),
  `src/ccp/common/kafka.py` (run loop: signals, heartbeat, backoff), `tests/test_validator_route.py`,
  `tests/integration/test_validator.py`.
- **Acceptance:** `pytest -q tests/test_validator_route.py` · `pytest -q -m integration tests/integration/test_validator.py`.
  Malformed, bad-container and unknown-reference events land in the DLQ with the correct `reason`, and clean events reach `validated`. Ctrl+C causes a clean close (A8).
- **Commit:** `feat(validator): schema, container and reference validation with reason-coded DLQ`
- **Risks:** the per-message `flush()` is slow (an accepted shortcut).

## P6: Processor (HARD CHECKPOINT 1)
- **Goal:** `validated` → MongoDB: insert, dedupe, fold, guarded upsert, manual commit, heartbeat (TRD §6, §8).
- **Files:** `src/ccp/processor/{__init__,__main__}.py`, `src/ccp/common/mongo.py` (client and indexes),
  `tests/integration/test_processor.py`.
- **Acceptance:** `pytest -q -m integration tests/integration/test_processor.py`, proving:
  (a) replaying the same stream twice leaves `shipment_status` byte-identical apart from `updated_at`, with the event count unchanged;
  (b) a shuffled arrival order gives identical final states; (c) killing the processor mid-run and restarting it leaves the final states equal to
  the `--expect` values. **Stop for approval** with the test output, a demo run and known limits.
- **Commit:** `feat(processor): idempotent event store and event-sourced shipment status`
- **Risks:** recompute cost (A9); reproducing a kill mid-run reliably (the test uses a subprocess and terminates it after N messages).

## P7: Monitor
- **Goal:** idempotent hold alerts and the DLQ → `rejections` sink (TRD §12).
- **Files:** `src/ccp/monitor/{__init__,__main__}.py`, `tests/integration/test_monitor.py`.
- **Acceptance:** `pytest -q -m integration tests/integration/test_monitor.py`: a shipment held past `HOLD_ALERT_SECONDS` produces
  exactly one alert across two monitor restarts, and replaying the DLQ does not double-count rejections.
- **Commit:** `feat(monitor): hold-too-long alerts and DLQ rejection sink`
- **Risks:** a timing-sensitive test. The mitigation is a small threshold plus polling with a timeout.

## P8: API
- **Goal:** the read-only FastAPI app (TRD §13).
- **Files:** `src/ccp/api/{__init__,__main__,app}.py`, `tests/integration/test_api.py`.
- **Acceptance:** `pytest -q -m integration tests/integration/test_api.py` (every endpoint, 404s, `limit=201` → 422,
  state filter) · `python -m ccp.api`, then open `http://localhost:8000/docs`.
- **Commit:** `feat(api): FastAPI query layer for status, history, holds, anomalies, alerts and stats`
- **Risks:** none significant.

## P9: Demo and smoke test (HARD CHECKPOINT 2)
- **Goal:** one command runs the pipeline and verifies it against `--expect`.
- **Files:** `src/ccp/demo.py`, `tests/integration/test_smoke.py`, `README.md` v1.
- **Acceptance:** `python -m ccp.demo --seed 42` starts the four services as subprocesses (dev mode), runs the simulator, polls
  `/stats` until it settles, prints the summary (produced, stored, duplicates, DLQ by reason, state counts, anomalies,
  alerts), checks `produced = stored + duplicates + rejected`, and exits 0 only if everything matches. **Stop for approval.**
- **Commit:** `feat(demo): one-command verified end-to-end demo and compose smoke test`
- **Risks:** A10; flaky waits. The mitigation is to poll until the expected totals are reached, with a hard timeout.

## P10: Image, Compose services and CI (Release 1 done)
- **Goal:** one slim multi-stage image for every service, the services in Compose (`profiles: [app]`), and GitHub Actions.
- **Files:** `Dockerfile`, `.dockerignore`, `deploy/compose/docker-compose.yml`, `.github/workflows/ci.yml`, README.
- **Acceptance:** `docker build -t ccp:0.1.0 .` (image size recorded) · `$C --profile app up -d --wait` ·
  `$C run --rm simulator --seed 42` then `python -m ccp.demo --seed 42 --verify-only` · CI green on **your** push (I do not push; rule 1).
- **Commit:** `build: single slim image, containerised services and CI pipeline`
- **Risks:** SIGTERM reaching PID 1 (A11); CI time and resources (A12).

## P11: Kubernetes on kind (HARD CHECKPOINT 3)
- **Goal:** the same stack on kind, using plain manifests, with the processor scaled to 3 (ARCHITECTURE §7–8).
- **Before starting:** `$C down`, so Compose and kind never run together (D-039). Download `kind.exe` v0.33.0 from the kubernetes-sigs/kind GitHub release
  into `D:\ccp-data\bin`. **I will ask before downloading** and state the file name, source and size.
- **Files:** `deploy/k8s/{kind-config,namespace,configmap,kafka,mongo,topics-job,validator,processor,monitor,api,simulator-job}.yaml`
  (memory limits as in D-039), `src/ccp/common/groups.py` (consumer-group describe through AdminClient), `deploy/k8s/README.md` (or a README section).
- **Acceptance:** all pods Ready · node memory recorded (A2) · `kubectl scale deploy/processor -n ccp --replicas=3` with rebalance logs and
  `python -m ccp.common.groups ccp-processor` output recorded · the simulator Job runs, then `python -m ccp.demo --seed 42 --verify-only` passes through a port-forward. **Stop for approval.**
- **Commit:** `feat(k8s): kind deployment with heartbeat probes and scalable processor`
- **Risks:** RAM (A2). If the node is OOM-killed or pods are evicted, the evidence is recorded, bullet 2 is cut, and Release 1 stands (D-039). Also PVCs on kind (A15) and A13.

## P12: Polish
- **Goal:** a README that a stranger can run, honest limits, and a resume-claim → evidence table.
- **Files:** `README.md` (Mermaid diagrams, run steps, Known limits, Add-ons), `docs/*` sync.
- **Acceptance:** every resume bullet maps to a verified artifact (test, log or command output) or is flagged for removal.
- **Commit:** `docs: final README, known limits and resume evidence map`

## Requirements traceability

| PRD req | TRD section | Phase(s) |
|---|---|---|
| FR-1 ingest on Kafka | §2, §7 | P3, P4 |
| FR-2 SCHEMA_INVALID | §2, §4 | P1, P5 |
| FR-3 CONTAINER_ID_INVALID | §4 | P1, P5 |
| FR-4 UNKNOWN_REFERENCE_DATA | §3, §4 | P1, P5 |
| FR-5 store once | §6, §8 | P6 |
| FR-6 event-time fold | §5, §8 | P2, P6 |
| FR-7 anomalies | §5 | P2, P6 |
| FR-8 duplicate count | §8 | P6 |
| FR-9 one alert per hold | §12 | P7 |
| FR-10 rejections | §6, §10, §12 | P7 |
| FR-11 status by id | §13 | P8 |
| FR-12 event history | §13 | P8 |
| FR-13 list by state, paginated | §13 | P8 |
| FR-14 anomalies, alerts, rejections, stats | §13 | P8 |
| FR-15 seeded simulator + manifest | §11 | P4 |
| FR-16 verified demo | §11, §15 | P9 |
| FR-17 Compose + kind, scalable processor | §7, ARCH §6–8 | P10, P11 |
| FR-18 CI | §15 | P10 |

## Resume claims → evidence (D-038; the wording is frozen only in P12)

Your original bullets, checked against the design:

| # | Original claim | Verdict |
|---|---|---|
| 1 | "decoupling document-submission and clearance-status services through Kafka" | **Inaccurate as worded.** No services have those names; they are split by pipeline stage. Reword it. |
| 2 | "Docker … local Kubernetes cluster using Deployment and Service manifests" | True **only if Hard Checkpoint 3 passes**. Services exist for kafka, mongo and api (the consumers need none). |
| 3 | "Persisted workflow events in MongoDB and validated asynchronous message-processing flows" | True after P6 and P9, but vague. The rewrite names what was actually proven. |

Proposed wording (every phrase maps to an artifact):

1. *Designed an event-driven microservices pipeline for a simulated customs-clearance workflow (synthetic data), decoupling ingestion,
   validation, state-derivation and alerting services through Kafka consumer groups, with a reason-coded dead-letter topic for invalid events.*
   **Evidence:** P5 integration test (DLQ reasons), P9 demo summary (DLQ counts by reason match the expected values), ARCHITECTURE §1.
2. *Containerized the services into a single slim Docker image and deployed them to a local Kubernetes (kind) cluster with Deployment,
   StatefulSet and Service manifests, heartbeat liveness probes, and a processor scaled to 3 replicas through consumer-group rebalancing.*
   **Evidence:** P10 image build and size, P11 `kubectl get pods`, the rebalance logs and the `ccp.common.groups` output. **Conditional on Hard Checkpoint 3.**
3. *Built an immutable event log and a current-state view in MongoDB, deriving each shipment's state by replaying its events in event-time
   order so that duplicate, late and out-of-order events self-heal; verified by replay, shuffled-order and crash-restart tests.*
   **Evidence:** P2 permutation test, the P6 integration tests (a, b, c), and the P9 reconciliation `produced = stored + duplicates + rejected`.

Not claimed anywhere: "exactly-once semantics", throughput numbers (none are measured), real-world data, or cloud deployment.

## Add-on features (after the main project works)

Not built until requested. Each one gets a DECISIONS.md entry before it starts.

- A Streamlit dashboard of held shipments, anomalies and DLQ stats (not front-end evidence for the resume)
- Prometheus + Grafana: consumer lag, throughput, DLQ rate
- Avro or JSON Schema with a schema registry and compatibility rules
- An event replay CLI (rebuild `shipment_status` from the `events` log)
- A Locust load test with recorded numbers
- Cloud deployment (managed Kubernetes or a container platform), optionally against Cosmos DB's MongoDB API, only if Cosmos is wanted on the resume
- A Helm chart for the k8s manifests
- API auth and rate limiting
- Event-time watermarking and clock-skew handling
- Alert resolution: mark an alert resolved when its shipment leaves ON_HOLD, so a lag-induced alert (D-064) is visibly closed
- Interview-prep notes written only from what is actually built

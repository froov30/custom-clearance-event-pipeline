# Tasks (resume point for any session)

> Synthetic-data project. The phase details are in IMPLEMENTATION_PLAN.md. Tick an item only after its acceptance check has been run and its output shown.
> **Current status:** P0 to P11 done and checked. Waiting for the user's approval of hard checkpoint 3 before P12.
> **Hard rule (D-040):** nothing is written to C:. Repo on D:, all other data under `D:\ccp-data\`. Source `scripts/dev-env.ps1` in every shell.

## Stage 1: Planning
- [x] PRD.md
- [x] TRD.md
- [x] ARCHITECTURE.md
- [x] IMPLEMENTATION_PLAN.md
- [x] DECISIONS.md (D-001 to D-042, plus the environment record)
- [x] TASKS.md
- [x] Cross-doc consistency pass
- [x] Gate 1 risks resolved: D-031 accepted (`PROCESSING_ERROR`), resume bullets reworded (D-038), RAM budget (D-039),
      D:-only storage (D-040), uv Python 3.11 (D-041), CI mirrors local (D-042), all tags and versions verified (D-033)
- [ ] **User action:** move the Docker Desktop disk image to `D:\ccp-data\docker`, then start Docker Desktop
- [x] "start build" (2026-10-11)

## Release 1
- [x] **P0** Scaffold: `scripts/dev-env.ps1`, uv Python 3.11 on D:, pyproject (pinned), ruff, pytest, config, JSON logging, .env.example, Dockerfile skeleton, README stub; C: free space unchanged
- [x] **P1** Reference CSVs (31 ports, 21 HS codes), ISO 6346, Pydantic schema, unit tests (34 pass; mutation-checked)
- [x] **P2** Pure fold, exhaustive transition tests, permutation test (55 pass; mutation-checked)
- [x] **P3** Compose infra (tags verified), topic bootstrap, listeners documented, integration infra test (4 integration pass; Kafka UI up; see DECISIONS P3 results)
- [x] **P4** Simulator with fault injection and `--expect`, determinism test, faults seen in Kafka UI (live runs match manifests; see DECISIONS P4 results)
- [x] **P5** Validator with reason codes, signals, heartbeat and backoff; route unit tests and integration test (19 unit + 1 integration; backlog counts exact; see DECISIONS P5 results)
- [x] **P6** Processor with dedupe, guarded upsert and manual commits (see DECISIONS P6 results)
  - [x] replaying twice changes nothing · [x] shuffled order gives the same states · [x] kill and restart heals
  - [x] Six known limits fixed and mutation-checked (DECISIONS: "Phase 6 limit fixes", D-051 to D-055)
  - [x] **HARD CHECKPOINT 1 approved** (user, 2026-10-14)
- [x] **P7** Monitor: one alert per hold across restarts; DLQ → rejections without double counting (5 integration; see DECISIONS P7 results)
- [x] **P8** API endpoints, pagination cap, integration tests, Swagger checked (16 integration; live check; see DECISIONS P8 results)
- [x] **P9** `python -m ccp.demo` verified summary, smoke test, README v1 (MATCH, exit 0; 38 integration pass; see DECISIONS P9 results)
  - [x] **HARD CHECKPOINT 2 approved** (user, 2026-10-14)
- [x] **P10** Slim image (268 MB), Compose `app` profile (containerised demo MATCH), CI workflow written (see DECISIONS P10 results)
  - [x] **CI green after the user pushes** (run 38542228617 on `c48a0bf`: lint-and-unit, integration 39 passed, image + demo MATCH). **Release 1 done.**

## Release 2
- [x] **P11** kind manifests, topic Job, probes, ConfigMap/Secret, processor ×3, rebalance recorded, demo on kind (see DECISIONS "P11 results")
  - [x] Prepared without a cluster: manifests, `ccp.common.groups` (+ test), rebalance logging (+ test), `deploy/k8s/README.md` (D-060)
  - [x] kind v0.33.0 downloaded with the user's approval (2026-10-14), SHA-256 matched the published file
  - [x] Cluster run: all pods Running, topics Job complete, demo on kind `RESULT: MATCH`, processor ×3 rebalance recorded, node memory recorded; bug found and fixed on the way (D-061)
  - [x] Follow-ups requested at checkpoint 3: Kafka log on a PVC (D-062), A13 under load + pod kills + liveness restart + broker restart via `scripts/kind-chaos.ps1` (D-063, exit 0), `kind load` root cause (D-062); found and handled on the way: lag-induced hold alerts (D-064, documented) and a manifest bug for shipments with no valid event (D-065, fixed)
  - [x] Debug of the eight open issues (DECISIONS "Chaos rerun and debug results", D-066 to D-070):
    - [x] 1. Lag-induced false alerts: alerts now close when their hold ends; open alerts = holds under chaos at a 20 s threshold (D-069). The raised count still includes them (the page itself is not fixed; watermark is an add-on)
    - [x] 2. Alerts never retracted: `resolved_at`, `open_alerts` in `/stats` (D-069)
    - [x] 3. Validator re-sends inflate duplicates: the count is right; the demo now accounts for re-emits, proven with 5 injected re-sends on kind (D-067)
    - [x] 4. Broker restart under load: MATCH and 0 stale statuses (D-068)
    - [x] 5. MongoDB restart on kind under load: MATCH and 0 stale statuses; verifier no longer crashes on an API error (D-068)
    - [x] 6. CI on the new code: all three `ci.yml` jobs pass locally; GitHub's run needs the user's push
    - [x] 7. httpx deprecation warning: `httpx2` 2.13.1 in the dev extras; test red first (D-066)
    - [ ] 8. Docker Desktop writes to C:: not fixable without writing to C: (D-070); user actions listed there
  - [ ] **HARD CHECKPOINT 3 approved** (waiting for the user; P12 starts only after this)
- [ ] **P12** README polish, Known limits, resume-claim → evidence table

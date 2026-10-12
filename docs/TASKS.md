# Tasks (resume point for any session)

> Synthetic-data project. The phase details are in IMPLEMENTATION_PLAN.md. Tick an item only after its acceptance check has been run and its output shown.
> **Current status:** Stage 1 docs written and the Gate 1 risks resolved. Waiting for "start build".
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
- [ ] **P3** Compose infra (tags verified), topic bootstrap, listeners documented, integration infra test
- [ ] **P4** Simulator with fault injection and `--expect`, determinism test, faults seen in Kafka UI
- [ ] **P5** Validator with reason codes, signals, heartbeat and backoff; route unit tests and integration test
- [ ] **P6** Processor with dedupe, guarded upsert and manual commits
  - [ ] replaying twice changes nothing · [ ] shuffled order gives the same states · [ ] kill and restart heals
  - [ ] **HARD CHECKPOINT 1 approved**
- [ ] **P7** Monitor: one alert per hold across restarts; DLQ → rejections without double counting
- [ ] **P8** API endpoints, pagination cap, integration tests, Swagger checked
- [ ] **P9** `python -m ccp.demo` verified summary, smoke test, README v1
  - [ ] **HARD CHECKPOINT 2 approved**
- [ ] **P10** Slim image (size recorded), Compose `app` profile, CI workflow, CI green after the user pushes. **Release 1 done.**

## Release 2
- [ ] **P11** kind manifests, topic Job, probes, ConfigMap/Secret, processor ×3, rebalance recorded, demo on kind
  - [ ] **HARD CHECKPOINT 3 approved**
- [ ] **P12** README polish, Known limits, resume-claim → evidence table

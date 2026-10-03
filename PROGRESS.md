# m2s continuation checkpoint

Last recorded: 2026-10-03 UTC. This file is the durable handoff; README defines
the goals and evidence boundaries. Read AGENTS.md before continuing.

## Current checkpoint

- Owner instruction: continue all README work; persist progress frequently so
  development can resume after usage resets. Routine repository operations and
  validated merges are authorized without a new confirmation.
- Starting main: `017285bafd1978cdf129ba2beb9a592d4c63ce48`.
- Active milestone: **V1-01, staged validation and independent run supervision**.
- Active branch: `codex/v1-staged-validation-20261003`, [PR #5](https://github.com/justgo4/m2s/pull/5).
- Implementation checkpoint: `2837773a2018ffcb3e7a70db4abf099edeabf099`;
  base `ffad2701181a475d44622a16c89539a30b675b3c`. Nine local protocol tests,
  canonical P11/gate contracts, syntax and privacy checks pass. Remote CI pending.

First implementation `db96c9193fd04236a040e5ac47f6a77f24f408da`:
[baseline](https://github.com/justgo4/m2s/actions/runs/37083541892) and
[native](https://github.com/justgo4/m2s/actions/runs/37083541616) succeeded;
[E2E](https://github.com/justgo4/m2s/actions/runs/37083541618) reached seven
successful cells before the new revision. [Staged smoke](https://github.com/justgo4/m2s/actions/runs/37083541672)
failed its latency/density gate, with all full-row exactness, dynamic tasks and
recovery correct: P95/P99 15.09s, density 0.664. Artifact `11260180673` preserves
the complete report. This is not a 5/10s performance pass.

The follow-up checkpoint aligns only **protocol smoke** with existing E2E:
sample=0.5s, snapshot chunk=512, fault interval=20s; P95/P99=30/60s and density=0.2.
Small/million/medium/scale/soak retain 5/10s; canonical P11 and formal gate defaults
remain unchanged. plan.json explicitly exposes gate_thresholds. Latest PR CI must
pass independently; do not merge by combining old and new green runs.
- Working files: `validation_profiles.py`, `tools/validation_run.py`,
  `tools/validation_run_test.py`, `tools/longhaul_workload.py`,
  `tools/longhaul_workload_test.py`, `.github/workflows/validation.yml`,
  `.github/workflows/ci.yml`, README.
- Local workspace: `/workspace/m2s-work`; all 174 original tracked blobs were
  checked against GitHub SHA-1 and permissions. A Git index exists for privacy
  checks, but its historical HEAD could not be materialized; it is not a clean
  live-test checkout. Python runtime lacks Arrow/database dependencies. Full
  imports/live database contracts use the real GitHub checkout. Never fabricate
  its revision or declare the local snapshot formal evidence.
- Next action: inspect PR #5 workflow results/logs, fix any failures, and merge
  when required checks pass. Merge automatically starts hosted small/million
  runs. Preserve run IDs/artifacts and inspect real performance before changing
  state layout. Meanwhile continue V1-02 operating procedures.

Local commands completed:

```bash
python tools/validation_run_test.py
python tools/p11_profile_test.py
python tools/longhaul_gate_test.py
python -m compileall -q validation_profiles.py tools/validation_run.py tools/validation_run_test.py tools/longhaul_workload.py tools/longhaul_workload_test.py
python tools/privacy_check.py
```

V1-01 implementation provides named development/P11 plans, new-only private run
directories, atomic status and logs, clean revision and finished-report identity,
PIDFD cancel, real child cleanup, and gate-only resume without reinitialization.
SIGTERM now unwinds the workload's independent daemon group. It does not support
resuming an incomplete workload or surviving host loss. PR runs real hosted smoke;
main runs small/million; no shortened result is called formal P11 certification.

## Completed evidence to preserve

- GC retention and consumer admission now share the SQLite write transaction;
  stale consumer watermarks below min_readable_seq are rejected. Three WAL race
  regressions added. [PR #4](https://github.com/justgo4/m2s/pull/4) merged.
- Fix commit `b30fd98dbf878873247457ac9d9bc2a9ddb09bdd`: [baseline](https://github.com/justgo4/m2s/actions/runs/37078480776),
  [native](https://github.com/justgo4/m2s/actions/runs/37078480762),
  [source-state](https://github.com/justgo4/m2s/actions/runs/37078480742), and
  [eight real E2E jobs](https://github.com/justgo4/m2s/actions/runs/37078480757)
  all successful. Its merge `55fa2a940a3174e0916b2b64b1d6180d8f1527df`
  has the same Git tree. README section 10.5 has details.
- README section 11 separates staged validation, bounded v1, and longer-term
  goals. 72h execution does not itself call an LLM. Existing checkpoint is only
  evidence and does **not** resume the entire workload runner.

## Ordered backlog and exit gates

| ID | State | Work and evidence required |
|---|---|---|
| V1-00 | done | Durable repository progress/authorization/continuation instructions |
| V1-01 | active | Named small/medium/scale/soak/P11 profiles; independent supervised execution; atomic status/evidence; correct cancellation and interrupted-run behavior; local protocol tests and CI |
| V1-02 | queued | Operational status/alert and safe unknown-output, backlog/low-disk/recovery procedures; upgrade/rollback runbook and tested boundaries |
| V1-03 | needs test machine | 100k–1M and 5M–10M fixed-resource measurements, 1/10/100 tasks, JOIN skew/fan-out, dynamic add/drop/rebuild, TEMP/WAL/RSS/GC contention; retain exact revision/artifacts |
| V1-04 | conditional | Address measured bottlenecks only; any state layout/native change needs correctness/migration/recovery A/B evidence |
| V1-05 | needs test machine | Scale-short and soak; source scope/DDL, concurrent replacement/cancel, low space and crash/upgrade combinations |
| V1-06 | needs prior gates | Freeze milestone SHA, run formal isolated 50M/72h with service/resource fingerprint and gate; no shortened evidence relabeling |
| SQL-01 | later | Define and implement MIN/MAX and DISTINCT retract/recovery semantics with full-state oracles |
| SQL-02 | later | LEFT JOIN semantics and transitions; JOIN+aggregate/window/general cross-operator incremental compiler, each with an explicit supported subset |
| STATE-01 | profile gated | Schema-epoch migration, general arrangement/factorized state, whole-graph lifetime costs and stable adaptive policies |
| PRODUCT-01 | later | Versioned deploy/explain/status/cancel, permission boundaries, upgrade/rollback; MCP reuses the same checked control plane |
| BENCH-01 | later | Fair comparable engine benchmarks; no superiority or physical-limit claim without matched evidence |

These are tracked work items, not completed features. Bounded v1 is already
functionally implemented for the SQL subset described in README; production
resource and operating evidence remains to be obtained. Broader research scope
must be split into explicit acceptance criteria before coding.

## External dependencies / known limits

- Owner does not know whether a self-hosted runner exists and authorized checking
  and creating needed infrastructure, including freely using public hosted Actions.
  Existing workflows use standard hosted runners. The connector rejected the
  runner-list endpoint (`GET actions/runners`: unsupported URL), so no definitive
  registration list is available. No cloud identity/host credentials are bound.
  Use configured standard hosted resources now; do not repeatedly ask permission
  or assume public free minutes imply a persistent/unlimited machine. Creating a
  real host/self-hosted registration remains blocked by actual management access.
- Hosted E2E is small smoke, not the formal long-run environment.
- No formal 50M/72h report exists. No CI success implies that certification.
- Existing workload initializes isolated fixture databases, requires an empty
  work directory, and cannot restart mid-workload from checkpoint. A launcher
  must preserve interrupted state and never silently repeat initialization.

## Resume procedure

1. Fetch latest main, AGENTS.md, PROGRESS.md and open PRs. Preserve other changes.
2. Check active branch/PR and CI URLs before rerunning anything; a completed run
   is evidence and does not need a duplicate run without a new reason.
3. Inspect the recorded working files/commands and finish the active milestone.
4. Commit code, tests and this checkpoint together; record remote results/merge.
5. If the next task is blocked by a machine, record the exact dependency and
   continue independent code work. Never mark blocked testing as passed.

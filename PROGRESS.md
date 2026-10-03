# m2s continuation checkpoint

Last recorded: 2026-10-03 UTC. This file is the durable handoff; README defines
the goals and evidence boundaries. Read AGENTS.md before continuing.

## Current checkpoint

- Owner instruction: continue all README work; persist progress frequently so
  development can resume after usage resets. Routine repository operations and
  validated merges are authorized without a new confirmation.
- Starting main: `017285bafd1978cdf129ba2beb9a592d4c63ce48`.
- Active milestone: **V1-02, runner inventory and safe operating procedures**.
- V1-01 merged as `719159d60c54ce510fb5ffabfcc847b068e4bc51` (PR #5).
- Active branch: `codex/v1-cdc-bundle-20261003`, [PR #9](https://github.com/justgo4/m2s/pull/9).
- Candidate head `ea8b2cd7642db856da4b8ca6884775bebdc812d2`: baseline `37086484043`,
  native `37086484091`, source-state `37086484038` passed. all eight E2E `37086484049` and supervised smoke passed. Small `37086484023`
  failed only latency: P95=9.10/P99=11.10, density=.975, all oracles/dynamic/recovery
  passed; artifact `11260204343`. Actual tested PR merge SHA `ee10ce8ee1bc56d44fcf4f75f5bc7183e811a216`
  has exactly the same tree as head `ea8b2cd7`, verified by compare.
- Operations [PR #8](https://github.com/justgo4/m2s/pull/8) merged as `448f618babc28ec2be71b291c5ea0572b2e1cba0`.
- Operations head `9de0453b28181b59e1b0443bdf735f01a9dd6194`: baseline `37085918376`
  and native `37085918360` passed, including ten new operating/backup tests.
- Profile budget [PR #7](https://github.com/justgo4/m2s/pull/7) merged as `7dc814ab4d7e5a6b9e93a701c8ebeab0bfdb5dd6`.
- Profile fix SHA: `52bb1f3686b771f8b4cf3d5683bac26ef375fec1`; baseline `37085454135`,
  native `37085454170` and supervised smoke `37085454133` passed.
- Runner inventory [PR #6](https://github.com/justgo4/m2s/pull/6) merged as `af67f2e3a4b14d32b969ed8ec7ac6d4c1d2f3d05`.
- Previous validated implementation: [PR #5](https://github.com/justgo4/m2s/pull/5).
- Implementation checkpoint: `2837773a2018ffcb3e7a70db4abf099edeabf099`;
  base `ffad2701181a475d44622a16c89539a30b675b3c`. Nine local protocol tests,
  canonical P11/gate contracts, syntax and privacy checks pass. Remote baseline/native/all eight E2E/new supervised smoke passed on that exact SHA.

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
- Next action: inspect [hosted small/million](https://github.com/justgo4/m2s/actions/runs/37084867442)
  on merge SHA `719159d60c54ce510fb5ffabfcc847b068e4bc51`; preserve artifacts and
  inspect actual performance. Inspect fresh small/million on the profile-fix merge, then complete operations PR.
  Implemented drafts: `OPERATIONS.md`, `tools/state_backup*.py`,
  `tools/operational_check*.py`, `.github/workflows/ci.yml`; five real SQLite backup
  tests plus five health/CLI tests pass, privacy PASS. Add README operating link,
  submit focused PR, verify its baseline/native, merge. No cross-version rollback
  or production fault drill has been claimed. Initial million rejected before seed: six hot-adds
  exceed 4GiB at two-core cap; run `37084867442`, artifact `11259906985` retained.
  Draft changes: million uses four hot-adds like small, workflow fixes CPU cap=2,
  real topology-budget regression covers every named plan at cap=2. Local runner
  nine tests/P11/AST-loaded real topology calculation pass. Full workload test uses CI.
  Backup drafts: `tools/state_backup.py`, `tools/state_backup_test.py`; five real
  SQLite WAL/timeout/tamper/interruption tests pass. Operations committed and merged in PR #8. Cross-version and deployed fault drills remain pending.
  Inventory implementation: `tools/runner_inventory.py`, `tools/runner_inventory_test.py`,
  `.github/workflows/runner-inventory.yml`; four sanitized inventory tests pass.
  Actual inventory [run 37084999017](https://github.com/justgo4/m2s/actions/runs/37084999017)
  returned HTTP 403: registration availability remains unknown. Exact inventory
  head `69e33a794f1d9580b7a5672958b4a882ef547801` passed baseline `37084999011`
  and native `37084999018`. No persistent host was provisioned.

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


Latest exact-head evidence: [baseline](https://github.com/justgo4/m2s/actions/runs/37084288295),
[native](https://github.com/justgo4/m2s/actions/runs/37084288145),
[all eight E2E jobs](https://github.com/justgo4/m2s/actions/runs/37084288201), and
[supervised real-service smoke](https://github.com/justgo4/m2s/actions/runs/37084288352)
passed. Merge differs from tested PR head only in PROGRESS.md, verified by GitHub
compare; runtime/workflow trees are unchanged. Main small/million run `37084867442` failed: million topology preflight before
seed; small full-row/dynamic/recovery correctness passed but P95=21.25s,
P99=41.99s failed. Density=0.899 passed; only latency gates failed. Small artifact `11260627960` retained; exact
performance root cause still under investigation. No performance pass exists.

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

Performance follow-up under investigation: existing CDC bundles cap width at four
lanes even when one writer serves sixteen lanes. Small metrics show ~1.19s VISIBLE
per delivery and four-lane CDC bundles; larger bounded bundles may reduce queue
rotation. Candidate drafts: `j4.py`, `cdc_catalog.py`, `tools/cdc_bundle_test.py`,
`.github/workflows/validation.yml`: configurable CDC bundle cap=16, adapting to
active writer count; existing row/byte/prepared limits and durable memberships
unchanged. Four real-SQLite regressions prepared (full imports require CI).
Syntax and actual AST-loaded width tests pass. Baseline invocation/README added, PR #9 submitted. Require same-head
baseline/native/eight E2E and hosted smoke/small before merge. Remote j4 blob
`678496dd467baecca5438a768ca9a675b2b33b63` matches local after preserving the
API-returned final blank line. Durable old small numeric report committed at
`reports/validation-small-20261003.json`.
No improvement yet claimed; compare against profile-fix main
[run 37085851322](https://github.com/justgo4/m2s/actions/runs/37085851322), completed with small/million failures; inspect artifacts `11261185396` and
`11261215092` before any new rerun.
Profile-fix main `7dc814ab` results: small P95=26.32/P99=33.10, density=0.887
passed but latency gates failed (artifact `11261185396`). Million failed about
180s into run with `source_state_apply_worker` waiting over the 30s busy timeout
at `sync_source_base_catalog` BEGIN IMMEDIATE (artifact `11261215092`): transient
write contention currently terminates the daemon. Exact competing lock holder
not proven. Code inspection finds JOIN bootstrap/follower output staging builds
all pairs/identities under one outer transaction; this is a scale risk and needs
bounded consistent staging or measured set-wise improvement plus crash/fixed-W
proof. Do not merely raise timeouts or mark million passed. CPU snapshot-pause is a legacy-worker flag; shared snapshot worker does not use
it. Its effect on shared backfill was not proven. Do not repeat that causal claim.
Latest local follow-up (not pushed): byte-limited streaming source snapshot read,
shared worker respects configured row cap (was fixed 4096) and detects budget
truncation separately from EOF; `tools/source_snapshot_budget_test.py` covers
cursor/pin/restart/full coverage/singleton. Core CI pins actual PR head SHA.
Update PR #9 and require new same-head checks; do not merge on old green jobs.

## Ordered backlog and exit gates

| ID | State | Work and evidence required |
|---|---|---|
| V1-00 | done | Durable repository progress/authorization/continuation instructions |
| V1-01 | done | Named small/medium/scale/soak/P11 profiles; independent supervised execution; atomic status/evidence; correct cancellation and interrupted-run behavior; local protocol tests and CI |
| V1-02 | active | Operational status/alert and safe unknown-output, backlog/low-disk/recovery procedures; upgrade/rollback runbook and tested boundaries |
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

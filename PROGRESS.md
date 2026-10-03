# m2s continuation checkpoint

Last recorded: 2026-10-03 UTC. Read AGENTS.md and README.md first. This file is
the durable handoff if Codex quota/session ends; it does not promise automatic
AI continuation. Routine repository changes, Actions and validated merges are
authorized by the owner without expiry, subject to actual configured access.

## Immediate next action

1. Correctness [PR #10](https://github.com/justgo4/m2s/pull/10) MERGED as
   `c43f0fc3926cbd1800517599e147cc32a2043ba5`. Head
   `d988e1e5f8b5e1f51a9718ecd70212e97bd19b57` passed baseline37088425843,
   native37088425837, state37088425911, ALL eight E2E37088426468, supervised
   smoke37088425825. Old core PR workflows actually checked merge
   `9fb9fafadd2f244bd75664575a8ebd6766d85ef9`; compare with head differs ONLY
   in PROGRESS. Final merge differs from tested head ONLY README/PROGRESS.
   Runtime/workflow trees therefore match; staged smoke checked actual head.
2. Inspect newly launched main small/million after c43f0fc3. They run outside
   the AI session. Preserve exact SHA/report/artifacts; do not claim passed.
3. [PR #9](https://github.com/justgo4/m2s/pull/9), branch
   `codex/v1-cdc-bundle-20261003`, combined head
   `fbf0784a34dff3609d161e6c7e101534260951ce`, parents
   `bdff6c887dcfacd195103df24d91be1e6337710e` and main merge c43f0fc3.
   Integrated validated contention worker + new test invocation, actual-head
   CI and bounded source snapshot/CDC changes. Preserved main documents.
   Also corrected partition benchmark commit/artifact labels to checkout SHA.
   Require FRESH combined baseline/native/state/all E2E/smoke/small checks.
4. Previous PR #9 bdff6c88 small [37088427281](https://github.com/justgo4/m2s/actions/runs/37088427281)
   completed workload with correct actual identity; full raw/three aggregate/
   three JOIN/dynamic/recovery/drain correct; P95=12.072/P99=14.073s failed
   ONLY latency. density=.981 passed; RSS≈591.5MiB,writes≈3.27GB. Artifact
   `11261726394`, report SHA256
   `13611eae42694efc1ca8e07202657852996d82e7def522b431d2f650d70421ee`.
   Smoke passed. Do NOT merge performance candidate by lowering gates.
5. Next measured performance question: startup snapshot delivery count and
   raw marker latency; configured16384-row pages still take multiple remote
   commits. Preserve group/generation/FIFO/membership/prepared bounds; any
   cross-group batching needs explicit proofs. JOIN all-pair staging remains
   a separate measured-scale risk, not a proven owner of old BUSY.
6. Never combine green runs from different revisions. Do not repeat completed
   runs without a changed implementation or specific new measurement.

## Current implementation boundaries

PR #10 changes:
- `j4.py:source_state_apply_worker`: retry only SQLite BUSY (masked extended
  error code) AFTER rollback/no open transaction; stop-aware 0.2s wait,
  rate-limited generic logs and retry counter. Keep original 30s busy timeout.
- Preserve committed-prefix catalog resync with a dirty flag even if no new
  input follows. FULL/corruption/active transaction errors remain fatal.
- `tools/source_apply_contention_test.py`: four real-connection regressions
  for post-commit catalog lock, release/resync/exactly-once prefix, prompt stop,
  FULL and BUSY with active transaction rollback. Added baseline invocation.
- `tools/longhaul_workload.py/test.py`: authoritative actual Git HEAD, not
  GITHUB_SHA/M2S override when Git works. Explicit validated archive fallback
  only if Git unavailable; event SHA alone is never sufficient. Real temporary
  Git mismatch regression and error/fallback cases. Same fix on PR #9.

PR #9 changes:
- CDC bundle cap configurable `CDC_CDC_BUNDLE_MAX_LANES` default16/max64,
  apportioned by active writers; preserve bytes/rows/prepared budget, lane FIFO,
  durable membership/ack, kind/plan version/generation boundaries.
- Source snapshot streams SQLite cursor and honors configured count and
  serialized PK/payload byte cap. A budget-truncated page is not EOF; singleton
  over-budget rejects retaining pin; cursor/restart/full coverage regressions.
- Shared snapshot replaces fixed4096 cap with configured count plus byte cap.
- Five core workflows explicitly checkout actual PR head SHA.
- `tools/cdc_bundle_test.py` (four real SQLite regressions),
  `tools/source_snapshot_budget_test.py` (three Arrow/SQLite regressions),
  baseline invocations, curated old small numeric report and README.
- JOIN bootstrap/follower still enumerates/caches all pairs/identities under
  outer transaction: no bounded JOIN staging or scale proof yet.

## Completed milestones and evidence

- PR #4 GC/admission lock protocol merged
  `55fa2a940a3174e0916b2b64b1d6180d8f1527df`, identical tree to fix
  `b30fd98dbf878873247457ac9d9bc2a9ddb09bdd`. All baseline/native/state/eight
  E2E passed; README10.5 contains exact run links and three WAL race regressions.
- PR #5 supervised staged validation merged
  `719159d60c54ce510fb5ffabfcc847b068e4bc51`.
  Tested head `2837773a2018ffcb3e7a70db4abf099edeabf099`: baseline37084288295,
  native37084288145, all eight E2E37084288201, smoke37084288352 passed.
  Merge differed only in PROGRESS; runtime tree unchanged.
- PR #6 sanitized runner inventory merged
  `af67f2e3a4b14d32b969ed8ec7ac6d4c1d2f3d05`.
  Head `69e33a794f1d9580b7a5672958b4a882ef547801`: baseline37084999011 and
  native37084999018 passed. Actual inventory37084999017 returned HTTP403:
  registration availability UNKNOWN, not zero. No persistent host created.
- PR #7 hosted topology plan merged
  `7dc814ab4d7e5a6b9e93a701c8ebeab0bfdb5dd6`.
  Head `52bb1f3686b771f8b4cf3d5683bac26ef375fec1`: baseline37085454135,
  native37085454170, smoke37085454133 passed. Million now four dynamic tasks
  at4GiB and explicit hosted CPU cap2; all named plans real resource regression.
- PR #8 operations merged
  `448f618babc28ec2be71b291c5ea0572b2e1cba0`.
  Head `9de0453b28181b59e1b0443bdf735f01a9dd6194`: baseline37085918376/native
  37085918360 passed. Ten real SQLite backup/health/CLI tests. Online backup
  creates NEW private directory and consistent per-file single DB copy
  (DELETE journal), quickcheck/hash/fsync/ready verification and incomplete
  evidence. No blind restore/overwrite/old local state onto advanced remote.
  Status exits0 observation/1 alert/2 unknown. OPERATIONS.md covers startup,
  unknown output isolation, disk/backlog, backup and upgrade/rollback limits;
  deployed drills and real cross-version recovery still pending.

## Failures and performance evidence to preserve

| Revision / run | Result / artifact |
|---|---|
| First supervisor draft db96c919 /37083541672 | Strict smoke correctness all passed, P95/P99≈15.09s,density=.664 failed. Artifact11260180673. Follow-up protocol smoke uses existing E2E30/60s,.2; performance/P11 thresholds unchanged |
| Main719159d6 /37084867442 million | Six dynamic tasks exceeded4GiB before seed. Artifact11259906985; not a performance result |
| Main719159d6 /37084867442 small | All full-row/dynamic/recovery/drain correct, P95=21.25/P99=41.99s failed;density=.899 passed. RSS605MiB,writes2.79GB. Artifact11260627960; numeric summary in PR #9 |
| Main7dc814ab /37085851322 small | Same topology, P95=26.32/P99=33.10s failed;density=.887 passed. Artifact11261185396 |
| Main7dc814ab /37085851322 million | Valid topology; source apply catalog BEGIN IMMEDIATE busy timeout fatal around180s. Artifact11261215092. Competing lock owner NOT proven |
| PR #9 ea8b2cd7 /37086484023 small | Full correctness passed, P95=9.10/P99=11.10s failed;density=.975 passed. Artifact11260204343. Actual old report SHA ee10ce8ee1bc56d44fcf4f75f5bc7183e811a216 has identical Git tree to ea8 head (verified compare). Baseline37086484043/native37086484091/state37086484038/eight E2E37086484049 and smoke passed |
| PR #9 5b382ef4 /37087609367 smoke+small | Workloads completed/full-row oracles correct but supervisor rejected revision mismatch. Artifacts11261655549/11261028417. Small observed11.17/13.19s, NOT accepted gate and no proven batching improvement. Baseline37087609348/native37087609372/state37087609364/eight E2E37087609344 passed |
| PR #9 bdff6c88 /37088427281 small | Correct actual identity, all exactness/dynamic/recovery correct; only12.072/14.073s latency fails,density=.981 passes. Artifact11261726394; no performance pass |\n| PR #10 ea913d53 /37087862226 smoke | Workload completed/full-row correct; same identity rejection. Artifact11261248141. Baseline37087862148/native37087862223/state37087862129 passed; inspect E2E37087862207 separately |

Identity bug: after explicit PR-head checkout, old code_revision() still preferred
GITHUB_SHA (temporary event merge). Both current PR heads fix this; retain old
reports as rejected evidence rather than changing recorded SHA/relabeling passes.

Latency observations across hosted machines are not strict same-machine A/B.
CPU snapshot_pause only controls legacy snapshot, not shared backfill; its role
in shared delay was NOT proven. The ~180s catchup figure follows a moving marker
set: recover_after_fault() keeps adding produced markers and waits for every one
plus log==apply. Constant delivery lag can keep it waiting until source window
ends. Do not equate that whole interval to daemon downtime or one long lock.
Changing recovery measurement needs a separately specified protocol/regression;
formal P11 is unchanged.

## Local workspace and validation

Working directory `/workspace/m2s-work`. All174 original tracked blobs were
SHA-verified; local Git has an index but UNBORN historical HEAD, so it is not a
clean live-test checkout or formal benchmark revision. Arrow/database imports
are unavailable locally; full-runtime tests run against real GitHub checkouts.
Local j4 currently contains BOTH PR drafts, while remote PR #10 contains only
its independent changes. Treat remote per-branch trees as authoritative.

Latest local checks: AST-loaded ORIGINAL code_revision() using real temporary Git
passed divergence/archive/error cases; syntax compileall workload/test passed;
privacy PASS files191. Previous supervisor nine, backup five, health five and
inventory four stdlib/SQLite tests passed; P11/gate contracts passed. Do not
claim AST-only checks are full module tests. Current workload/test local files
match identity fixes pushed to both PR branches.

Commands:
```bash
python tools/validation_run_test.py
python tools/p11_profile_test.py
python tools/longhaul_gate_test.py
python tools/state_backup_test.py
python tools/operational_check_test.py
python tools/runner_inventory_test.py
python tools/privacy_check.py
# Full imports (CI dependencies required)
python tools/longhaul_workload_test.py
python tools/source_apply_contention_test.py
python tools/cdc_bundle_test.py
python tools/source_snapshot_budget_test.py
```

Supervisor named profiles persist plan/status/logs, require a new private run
directory, verify actual code/report identity, use bootID/startticks/PIDFD
cancel and unwind independent daemon groups on SIGTERM. Gate-only retry is
permitted only for successfully completed immutable report with SHA256 match.
It does NOT resume an interrupted workload or guarantee survival of host loss.

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

## Immediate correction checkpoint

PR #9 staged run `37087609367` failed both profiles solely at supervisor report revision identity; artifacts `11261655549` (smoke) and `11261028417` (small). PR #10 staged run `37087862226` similarly failed, artifact `11261248141`. Actual checkout is PR head, but longhaul_workload.code_revision() incorrectly prefers GITHUB_SHA (temporary PR merge) over git HEAD. Do not label these reports accepted or rewrite them. Fix identity discovery to use actual git HEAD first; add regressions for divergent event/checkout and archive fallback. Apply the same focused workload/test fix to both PR branches, then require fresh checks. PR #10 baseline `37087862148`, native `37087862223`, state `37087862129` passed; two E2E cells still pending at this checkpoint. No merge yet. Local PROGRESS refreshed from remote.

## Active measured follow-up draft

After accepted small artifact11261726394 still12/14s fails, inspect snapshot remote commit count. Current claim_snapshot_bundle restricts all members to one page group even when adjacent snapshot pages of the SAME plan/generation are pending. Draft a bounded per-lane contiguous snapshot-prefix claim across pages, stopping at CDC/assigned/different plan/group-less boundary, retaining row/byte/prepared/writer-width limits and all individual group IDs. Existing acknowledge_delivery already finishes every member group via longest completed stage prefix. Add real SQLite multi-group restart, CDC fence, out-of-order group visibility and budget tests before pushing PR #9 again. This is a draft hypothesis, not a proven SLO fix. Preserve current combined head fbf0784a and fresh CI evidence when superseding.

## Stateful contention fix started

Base main83269c14; branch codex/v1-stateful-busy-20261003. Main million
37088943284 artifact11261727031 fails stateful_task_worker with BUSY.
Implement narrowly scoped stop-aware BUSY retry only after rolled-back durable
runner operations; retain post-step result for physical registry sync retry.
FULL/corruption/active transactions remain fatal. Add real SQLite contention,
atomic bootstrap/outbox, post-commit sync and cancellation regressions before CI.
Snapshot prefix remains independent PR9 head87ef3616, awaiting fresh checks.

## Stateful BUSY implementation checkpoint

Four full-import real SQLite tests PASS: rollback-safe actual aggregate bootstrap
resumes to exactly one durable outbox/row and releases its fixed-W pin after target
ack; stop interrupts contention; post-step registry contention retains the committed
result without rerunning the runner; FULL/CORRUPT/LOCKED/open-transaction BUSY stay
fatal. All calls use original30s production busy timeout (only tests10ms).
Existing aggregate runtime, physical registry and hot-add tests PASS locally.
Runner retry returns to outer loop to recheck membership/completeness/retire W;
registry retry retains result. No retry added to remote output or retirement.
Long JOIN bootstrap/outbox lock cost is still unresolved; this is recovery,
not a claim of throughput, million success or formal P11 certification.
Commands: python tools/stateful_worker_contention_test.py; python tools/aggregate_runtime_test.py;
python tools/stateful_physical_registry_test.py; python tools/stateful_hot_add_test.py.
New same-SHA CI still required. PR9 ref87ef3616 updated but GitHub returned no new
workflow runs; reopening once also has not yielded runs. No old run relabeled.

## Validated stateful recovery merged

PR11 head1a01cf3f74a0a1f8b442eae9a21b2a8188f37c2d passed baseline37097520156,
native37097520165, state37097520149, ALL eight E2E37097520171,
supervised smoke37097520134. Merged ef6ea5390850e71025bdea8cce8032331ec8962c;
merge tree must match tested head (verify compare). Production timeout unchanged.
Original worker AST, with real module globals/10ms test timeout, FAILS both lock
and post-step sync regressions; fixed worker passes all4 tests. Local baseline
script contracts83/84 passed; validation_run_test fails here because /proc process
identity is unavailable, while full hosted baseline including it passed.

Snapshot PR9 closed/superseded by PR12; identical runtime tree now head0660e327
with main83269c14 as second parent. Earlier head87ef3616 had PROGRESS merge
conflict (mergeable_state dirty), which blocked pull_request CI; initial event
hypothesis was wrong. Conflict now resolved and fresh runs baseline37097752779,
native37097752778, state37097752801 PASS; E2E37097752807 and staged37097752817
pending. Preserve strict small gate result; merge only if complete checks green.
Old combined fbf0784a small37089010543 artifact11262540394 complete/exact,
P95=11.098/P99=15.098,density=.973 fails ONLY latency; report unchanged.

JOIN stream branch2edcf7e9 checkpoint;5 full SQLite stream tests pass including
old full-pair oracle, exact digest/retry, NULL/bag/projection, midstream rollback
and WAL reader isolation, collision rollback, fixed-W recheck under write lock.
100k output fanout A/B3 repeats exact digest; peak RSS cache~203–205MiB versus
stream~140–142MiB. CPU/wall~2.0–2.45s versus2.27–2.36s: no throughput claim.
1M-pair synthetic A/B pending. Keep atomic seed; total writer duration unbounded.
Do not confuse bounded Python pair memory with resumable chunked outbox/jobs.

## Streamed atomic JOIN seed implemented

Branch codex/v1-stream-join-bootstrap-20261003 now integrates maincb153909a979200d246fb516117758ebdb221d41.
join_state.iter_pairs uses indexed left scan/right equality probes, yields one
pair projection at a time. join_outbox seeds full/projected bootstrap directly
into atomic durable rows and hashes canonical ordered durable PK cursor. Existing
commits verify each source payload/count/digest on exact retry. Rechecks immutable
state identity/spec/bootstrap/W under the write lock. No schema change, no partial
bootstrap publication, no output protocol/consumer/pin/frontier change.
Five real WAL/full-oracle tests PASS; original generation/runtime/job bridge/
shared/subview/100 follower/GC/2000transaction randomized oracle PASS locally.
reports/join-stream-local-20261003.json retains raw synthetic A/B and SHA256 of
algorithm sources. 1M exact pair/digest: cache824152064 RSS/25.751s versus
stream148361216 RSS/28.469s (82.0% less peak process RSS,10.6% slower wall).
100k3 repeats similar CPU/wall and~30% lower RSS. Do not declare faster/lock
resolved: total atomic seed writer time and full job staging remain unbounded.
A 1M output pair ≠ 1M source rows or true daemon performance evidence.
Next: fresh full same-SHA CI before merge; bounded durable seeding/job staging
protocol and real1M/SLO after prior gates. Formal50M72h still absent.

## JOIN candidate CI trigger coverage correction

PR13 runtime head37860bf4 started baseline/native/eight E2E; state/validation
were excluded by existing path filters even though JOIN seed affects real mixed
workloads. Extend validation to source/aggregate/JOIN/stateful runtime changes;
state workflow triggers JOIN and executes stream contract + bounded100k-pair
A/B2 repeats, with explicit actual-head checkout. Require fresh same-head complete
CI after this workflow correction rather than mixing prior SHA passes.
Snapshot PR12 head0660e327 all correctness/smoke passed but strict small failed
in37097752817 artifact11264873012; retain original report and inspect numbers.


## 2026-10-03 checkpoint: JOIN stream merged; measured worker spin fixed locally

- PR #13 exact tested head `0a73f0a0419af765f613d8ee06b358e1fcdae5f5` passed baseline 37098321912, native 37098321871, state 37098321923 (stream contract/A-B included), supervised smoke 37098321907 and all eight real daemon cases 37098321915. Merged as `914c163a46eac2804f8c31925f20706a02add155`; compare tested head to merge has zero changed files. This is a bounded memory change, NOT 50M/72h/SLO certification.
- PR #12 small run 37097752817/artifact 11264873012: all full oracles/recovery/drain pass, latency FAIL P95=10.107533s/P99=14.107730s against unchanged 5s/10s. Do not merge this performance candidate as a success. Reuse logs peaked at 151/354 lines per second, 6,689 and 22,609 duplicates in two daemon logs.
- `codex/v1-stateful-idle-20261003` checkpoint 4c7ca7b records the investigation. Worker now waits 50ms when caught up (including pending target jobs/catchup visibility), or when a lagging follower's consumer watermark did not advance. Bootstrap chunks and progressing CDC prefixes continue immediately. Loader wakeups, stop, retirement, membership and visibility rechecks remain in the loop. Reuse logs emit on binding transitions only.
- Three real SQLite/actual aggregate runner regressions PASS: unacknowledged bootstrap/catchup, ready with pending CDC, shared follower waiting on leader then copying three commits without per-commit waits; output commit counts/frontiers/log transition/wakeups checked. All three FAIL against original `cb153909` worker loaded via AST in actual j4 globals. Existing BUSY4, aggregate runtime/shared/physical registry/rebuild tests and compile/diff checks pass locally. Hosted exact-head checks pending; no SLO improvement claimed yet.
- New main million run 37098049769 (`ef6ea539`, artifact 11264953117) failed later at merge_delivery_worker -> process_merge_lane -> merge_async_delivery -> BEGIN IMMEDIATE (30s SQLite BUSY), not stateful_task_worker. Source log/base reached 66; join retained state ~1,001,724 rows; lock owner not identified. Still requires bounded output staging/write transactions and exact remote-unknown recovery; do not blindly replay HTTP or raise timeout to claim success.


## 2026-10-03 checkpoint: idle pacing merged, known-visible local retry integrated

- PR #14 tested `ff41867dc49b4d83b2bb6ad03af2e7aa8b0e64cc`: baseline37098907314, native37098907256, state37098907301, all eight daemon E2E37098907452 and supervised smoke37098907352 PASS. Merged `7fe6bc05eac4060dac050c5a0c5c0026efe0e1e0`, identical tree to tested head. Main small/million will measure this combined stream+idle revision separately, without declaring a performance pass in advance.
- PR #15 previous standalone `99fa0cad` had native/state green; merging validated idle pacing into the candidate now requires fresh exact combined checks. Keeps main j4/CI/progress plus only known-visible local SQLite retry and merge_visible_contention_test invocation. Original two real-lock regressions fail, fixed three tests/identity/quarantine pass. HTTP/txn/unknown-result contracts unchanged; no timeout or gate changes.
- JOIN bridge streaming branch has four local passing tests (batch rows/bytes, bag/retract payloads, restart/final ack, real truncated spool rollback and bad nrows), and two regressions fail against original bridge. Existing bridge/runtime/shared/projection/generation and all three 100-follower state/subview/GC contracts pass. Benchmark and fresh PR CI pending; atomic output enqueue lock duration remains unbounded by this memory change.

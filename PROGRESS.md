# m2s continuation checkpoint

Last recorded: 2026-10-03 UTC. Read AGENTS.md and README.md first. This file is
the durable handoff if Codex quota/session ends; it does not promise automatic
AI continuation. Routine repository changes, Actions and validated merges are
authorized by the owner without expiry, subject to actual configured access.

## Immediate next action

1. Main `8ccf21e8b33f93183faf17d2df1b7f43630b1aed` includes PR #26 intent-BEGIN recovery, #27 bounded optional SQLite write timing and #28 WAL-snapshot retirement polling. Required CI passed at respective exact heads; see latest checkpoints.
2. PR #29 `dc6500e10e7e00236883aa2fa26fc8a05361526e` is a performance candidate, NOT merged. Local randomized/full-bag/rollback and 1M synthetic reference A/B pass. Hosted baseline/native/state/eight E2E/smoke PASS; run37139384246 strict small FAIL latency27.130/37.835s. Keep unmerged. Next validate the focused CDC lane candidate below on fresh exact-head full CI and unchanged strict 5s/10s small.
3. Main436a6f6 small run37138133724/artifact11279537301 has all seven full-output oracles, all four hot-adds, strong-exit live-tail recovery and final drain PASS; latency P95=26.107232/P99=32.453451s FAIL. Do not mistake synthetic compute speedup or smoke for end-to-end acceptance. Main019965 optional-timing staged run37138973645 is pending behind earlier main validation; avoid duplicate queues.
4. PR #12/#19 remain unmerged failed-small candidates; re-evaluate only focused changes against latest structural baseline with fresh strict CI. Shared mutable-leader bootstrap/promotion and genuine high fan-out incremental transactions remain scale limitations.
5. Formal unchanged p11-50m-50rps-72h-v4 still requires an independently configured persistent isolated host; none is configured. No final v1/P11 certification claimed. Repository authorization persists, but does not supply host/cloud credentials.

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
- Historical PR #9 cache behavior was superseded by merged #13/#16 streaming.
  Full seed and enqueue transactions still have output-proportional write locks.

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


## 2026-10-03 JOIN bridge stream implementation and measurement checkpoint

- Changes: join_job_bridge cursor enumerates canonical durable rows; mutation batches cap4096 (or lower configured batch_rows) and serialized payload/identity bytes; only singleton may exceed byte budget, existing Arrow max-row check still applies. Each batch routes/spools outside the write lock. Atomic jobs/links/pending_bytes transaction reads one spool record at a time; no complete mutation/Arrow/record cache. Returned job-id list still scales with job count, total enqueue write lock is NOT bounded.
- `python tools/join_bridge_stream_test.py`: four PASS (row/byte caps including singleton, independent bag/retract Arrow payloads, reopen/idempotence, last-job visibility, real truncated spool rollback and bad commit nrows). Original cached bridge fails batch-limit and malformed-header regressions via AST in actual module globals. Existing join_job_bridge/runtime/shared/subview/generation and shared_join_state_scale/shared_join_subview_scale/shared_join_follower_gc_scale contracts all PASS, including100 followers. Compile/diff checks pass.
- Isolated fresh-process old/new A/B, full ALL routed-row bag digest/count exact in both: 100k2repeats cache RSS410,828,800/406,278,144 vs stream233,545,728/237,707,264 bytes, wall2.110/1.976 vs1.517/1.523s; 1M cache1,821,097,984 vs stream241,070,080 bytes (86.8% less), wall18.008 vs14.775s. Jobs256 ->980 for1M; measure daemon/remote commit consequences, don't call this SLO success. Reproducible benchmark includes exact original cb153909 cache functions and algorithm SHA256. Curated synthetic summary reports/join-bridge-stream-local-20261003.json; raw runtime/workload states remain outside git.
- Main914c staged37098826117: million capture_binlog_native failed local BEGIN IMMEDIATE after30s, artifact11265542421. Small artifact11265487699 all seven full output targets, four dynamic ready, sharedfollowers4, finalseq301/pending0/recovery pass; strict latency still fails. No certification and no invented write-lock owner.
- Fresh exact-head CI is required before merging bridge candidate. Next executable structural work: resumable unpublished JOIN output chunks and bounded enqueue batches, ensuring pin/consumer readiness, independent target visibility, partial crash/GC and owner promotion cannot expose incomplete results. Remaining operational/fanout/disk and persistent50M72h acceptance scopes stay open.


## 2026-10-03 checkpoint: idle pacing merged, known-visible local retry integrated

- PR #14 tested `ff41867dc49b4d83b2bb6ad03af2e7aa8b0e64cc`: baseline37098907314, native37098907256, state37098907301, all eight daemon E2E37098907452 and supervised smoke37098907352 PASS. Merged `7fe6bc05eac4060dac050c5a0c5c0026efe0e1e0`, identical tree to tested head. Main small/million will measure this combined stream+idle revision separately, without declaring a performance pass in advance.
- PR #15 previous standalone `99fa0cad` had native/state green; merging validated idle pacing into the candidate now requires fresh exact combined checks. Keeps main j4/CI/progress plus only known-visible local SQLite retry and merge_visible_contention_test invocation. Original two real-lock regressions fail, fixed three tests/identity/quarantine pass. HTTP/txn/unknown-result contracts unchanged; no timeout or gate changes.
- JOIN bridge streaming branch has four local passing tests (batch rows/bytes, bag/retract payloads, restart/final ack, real truncated spool rollback and bad nrows), and two regressions fail against original bridge. Existing bridge/runtime/shared/projection/generation and all three 100-follower state/subview/GC contracts pass. Benchmark and fresh PR CI pending; atomic output enqueue lock duration remains unbounded by this memory change.


## 2026-10-03 integration checkpoint: bridge retains validated local visibility retry

- PR #15 tested `330005b4c60d9150e29c67faaec0d3dfd4410545`: baseline37099529873/native37099529883/state37099529877/all eight E2E37099529894/smoke37099529902 PASS; merged `efd9a0498de5cf8afa8816015b1ccf8db96aac79`, identical tree.
- PR #16 previous d15f009a state/native passed; integrate main recovery change and preserve all tests/docs now. Require fresh complete CI on the integrated head, do not reuse prior-head green checks as final acceptance.
- Main7fe6 small37099489202/artifact11266005315: all full output oracles/recovery/drain pass, latencyFAIL25.344531/33.751959s; CPU120.93s and only4 physical-reuse logs after restart (versus29,298 in older snapshot candidate), RSS618,950,656. Different hosted runs are not controlled A/B. Million same run/artifact11265104301 fails native capture -> commit_spool -> BEGIN IMMEDIATE. Formal thresholds unchanged and lock owner not proven.


## 2026-10-03 checkpoint: stop-aware source capture write-lock acquisition

- Main f586798c includes validated bridge; final batching combination c509d5ae is under fresh CI/strict small. Earlier7052 measurement is separate; validation concurrency preserves evidence.
- Million7fe6 run37099489202/artifact11265104301 proves native capture -> commit_spool -> BEGIN IMMEDIATE failed before entering the transaction. Earlier914c failed the non-transaction-event cursor write at the same boundary. Immutable source spool/cursor have not been consumed/advanced at BEGIN failure; there is no target HTTP operation here.
- Branch codex/v1-capture-begin-busy-20261003 will optionally retry ONLY BEGIN acquisition on SQLite BUSY with no active transaction, stop-aware. Native capture commit_spool/cursor opt in; default behavior unchanged. Never retry transaction body/COMMIT, other storage errors or an active transaction; no replay of one-shot iterators. Verify actual SQLite lock/spool/source cursor/exact prefix, cancellation before body, fatal body/default behavior and original negative regression before hosted CI. This does not bound long JOIN locks or certify latency; resumable bounded publication remains needed.
- First checkpoint API disconnected; verified branch/ref/file remained unchanged before this retry. Never assume an ambiguous write succeeded or blindly repeat it without read-back.


### 2026-10-03 source BEGIN retry implementation/test checkpoint

- j4.state_transaction optional stop retries ONLY BEGIN IMMEDIATE BUSY with no open transaction; default remains immediate error after original SQLite timeout. Native capture opts in at commit_spool and non-transaction-event cursor. Transaction body/COMMIT execute once, body error rolls back and is fatal; source spool is not read before successful BEGIN. Production timeout remains30s, so cancellation during SQLite's own BEGIN wait is still bounded by that timeout; subsequent0.2s backoff is interruptible.
- `python tools/capture_begin_contention_test.py`: four PASS with real writer lock/rolled disk source spool: release consumes sealed parts byte-exact once and source seq/cursor atomically; cancel consumes zero parts and leaves old cursor; default BUSY stays fatal; body/COMMIT/FULL/CORRUPT/LOCKED/BUSY-active never retry. Original BEGIN behavior via AST plus optional-keyword adapter fails both real-lock regressions with actual SQLITE_BUSY (not TypeError).
- Existing source_transaction_spool/source_apply_contention/stateful_worker_contention/merge identity/quarantine plus compile/diff checks PASS locally. Fresh same-SHA baseline/native/state/eight daemon/supervised smoke required. Current source lock contention remains a performance limitation; no SLO, bounded-lock or 1M pass claimed.


## 2026-10-03 validated integration and next measurement checkpoint

- PR #16 tested4e2328a859aa71c233de653e511f3d048e49a5e6: baseline37100123569/native37100123462/state37100123544/eight E2E37100123514/smoke37100123576 PASS; mergedf586798c8624d8f53ed96a326c2928e6288037c4, identical tree.
- PR #17 tested6ce77149b55ac0eaa70c1f1afda92e893093c41c: baseline37101089222/native37101089228/state37101089260/eight E2E37101089263/smoke37101089198 PASS; merged5a0f22f300d0d9afbfaa141dfe19df4d6b75d193, identical tree.
- PR #12 c509d5ae correctness baseline37100671711/native37100671748/state37100671721/eight E2E37100671701 PASS; staged37100671707 smokePASS/strict smallFAIL. Artifact11266017029 actual report SHA verifiedc509: P95=19.065292522/P99=21.066043648s, CPU245.33s/RSS475729920; all output/recovery/drain exact. Earlier7052 artifact11266365888 separately P95=6.147336155/P99=9.118501335s, stillFAIL P95. Neither is certified or controlled A/B.
- Next executable measurement edits tools/join_bridge_stream_benchmark.py to parameterize partition/row/byte budgets; compare current stream and independent cached baseline at production topology with full output bag. No runtime cap change before measured evidence. Then pursue bounded durable staging protocol with crash/last-ack/GC tests. Continue saving before and after milestones.


## Active 2026-10-03 production-topology measurement

Base main3da6cf7152e7ec8fd6a172e98a90e9dafcfa2750; branch
`codex/join-production-topology-20261003`. Fresh README/AGENTS/progress/open
PR reconciliation complete; PR12 stays unmerged after strict latency failure.
First milestone parameterizes isolated JOIN bridge A/B with partitions, configured
row/byte budgets, records effective4096 stream cap and measures production
16 partitions/16MiB/50000 configured rows. Retain independent complete bag oracle,
fresh child processes and original cache reference. Runtime unchanged until
evidence. Next: implement parameters, run local contracts and100k/1M evidence,
commit/checkpoint before bounded durable publication work. No formal SLO claim.

Implementation checkpoint: benchmark accepts partitions/batch rows/bytes/max row
bytes, forwards to fresh workers, reports topology/effective4096 cap. Optional
configured-stream candidate changes only isolated function namespace, never runtime.
First100k production A/B full bag exact: cache16 jobs vs stream400; RSS402–409MB
vs238–239MB. Full1M three-way and100k two-repeat three-way running locally.
Fixture max-row1MiB falsely rejects large cached batches; now explicit64MiB
production default and worker failures preserve stdout/stderr. CI now uploads
JOIN JSON artifacts (previous workflow computed them but omitted upload paths).
Added real process topology forwarding/full bag and failure diagnostic tests.
Next: collect reports, preserve curated numeric evidence, submit focused PR with
fresh state CI. No cap change or bounded enqueue/SLO claim.

Measurement complete: reports/join-production-topology-local-20261003.json
retains100k two repeats and1M one repeat, ALL output bags equal per row count.
1M current stream3920 jobs/252301312 RSS/17.424s; isolated configured-row
candidate320 jobs/368746496 RSS/14.976s; cache16 jobs/1910452224 RSS/18.686s.
Candidate trades bounded extra batch memory for fewer jobs; this is local bridge
evidence, not remote/SLO or bounded-lock proof. No runtime change in this PR.
PR18 head6f8111e9859cb770b6e7f39b0b362328f728ae1f began baseline37102340984,
native37102341012,state37102340983; fresh report/docs commit needs own checks.
Local stream4/benchmark2/job bridge/runtime/privacy/diff checks passed.
Next independent runtime candidate: honor configured row budget up to bounded
50000 cap, maintain byte/singleton/restart/last-ack contracts and require real
daemon strict-small before any performance claim. Bounded durable publication
remains separate structural work; do not conflate larger batch with shorter locks.


## Active local preparation BEGIN recovery

PR18 ef613a90882b62627fb2fe964cb638c6b8128b59 passed baseline37102418772,
native37102418792,state37102418768 and merged e18c543283f16f0ab686187504bd0d7866bc5bf4.
PR19 configured batch0d3ffd80a82e9a93764dae33631972ffce98e98c remains under
full real daemon/smoke/strict-small CI; do not merge before its gates.
Fresh main5a0f validation37101481261 small artifact11265808134: all seven
full output oracles/dynamic4/recovery/drain exact,pending0;strict P95/P99
44.431475206/55.121011750s fails. Million artifact11266297740 now stops at
prepare_delivery->persist_field_overflows->BEGIN IMMEDIATE before any target
HTTP. Lock owner still not established; avoiding prior capture stop revealed
this distinct local boundary, not million success.
Branch codex/prepare-begin-busy-20261003 based e18c: optional stop on local
preparation transaction BEGIN only, including reservation/overflow/final parts.
Pass runtime stop from both delivery protocols/OOM recovery. Default helper
behavior unchanged; no body/COMMIT/HTTP retry. Next real lock tests at acquisition,
post-transform overflow/final parts, cancellation/restart, fatal data/storage;
fresh exact-head full CI required. This is safe recovery, not bounded locking.

Local preparation implementation checkpoint: five real SQLite/Arrow preparation
tests PASS: BEGIN before transform; post-transform overflow evidence; final parts
conversion; stop retains unprepared jobs/reservation with no load_parts and reopen
recovers exact payload; FULL/CORRUPT/LOCKED and body BUSY remain fatal/one-shot.
Successful contention paths run transform once and final parts exactly once;
HTTP is guarded against in these tests. Existing capture4/known-visible3 pass.
The initial fixture omitted jobs.created and routing metadata; corrected fixture
now uses actual checked Arrow journal schema. Original negative regression runs
against HEAD prepare with optional-keyword adapter, not a TypeError comparison.
Main CLI selftest is locally blocked at AF_UNIX socket creation by environment
EPERM; full CI must validate it. Original SQLite30s timeout unchanged; backoff
stop-aware, transaction body/COMMIT/HTTP never replayed.
PR19 baseline37102524135/native37102524124/state37102524127/eight E2E37102524140
PASS;smoke PASS and strict small still running37102524134. No merge yet.

Corrected negative check uses original prepare function in ACTUAL j4 globals:
all three initial/overflow/final regressions fail with SQLITE_BUSY, fixed passes.
Direct cdc_selftest reaches process-resource assertion then fails because this
workspace cannot inspect /proc; AF_UNIX and /proc checks remain hosted evidence.
Next preserve fixed SHA CI without repeated pushes cancelling runs.


## 2026-10-03 validated local preparation recovery and structural handoff

PR20 tested7819653e154dee131f19901d75e4f07e7ba635b7 passed all five workflow
families: baseline37103061827, native37103061774, state37103061855,
all eight real daemon E2E37103061762, supervised smoke37103061767.
Merged146499d2e2093076b22936baaf17055d31ee6def. Compare tested head with
merge: only PROGRESS.md differs; runtime/test/workflow bytes unchanged.
Local/remote j4 blob bec0f29ee31fdb9d6cd3d3adbd8410b5e9eb3fb3 matches.
Fresh main push starts development small/million automatically; its result is
NOT supplied by prior smoke. The recorded main5a0f million failure is preserved.

Immediate coding continuation: implement resumable unpublished JOIN output
building and bounded job publication, not another generic transaction retry.
Current activate_catchup wraps ensure_consumer/seed_bootstrap with generation
finalize and pin release; simply inserting commits per chunk would expose a
partial bootstrap. Introduce durable building/sealed state, retain the fixed-W
pin until final activation, and hide incomplete output from pending_commits/
job claims/readiness/visibility. A cursor must preserve BOTH left and right PK
positions to resume high fan-out. LIMIT on output alone does not bound scans
through unmatched left rows; cap scan work and serialized bytes too.
Job chunk registration must commit jobs/links/cursor/accounting together and
publish without copying all staged payloads in one final write transaction.
The existing _already_staged path must distinguish incomplete versus sealed,
and acknowledge_delivery must never mark a partially staged commit visible.
Include crash after each chunk/seal, concurrent stale cursor retry, drop/GC,
owner promotion, independent target frontiers, full bag/retract and real writer
interleaving tests. Legacy completed commits need explicit upgrade compatibility.
This protocol is a pending design/implementation milestone, not a claimed feature.

PR19 remains unmerged after strict-small latency failure28.651/36.650s despite
exact results; no gate thresholds changed. Formal50M72h and final stable-v1
acceptance still require configured persistent isolated resources. Chat/session
loss stops AI reasoning; repository checkpoints and Actions do not claim automatic
continuation or final certification.


## Active 2026-10-03 bounded JOIN publication continuation

Owner renewed full m2s repository authorization. Read latest README review at
main83b57faf94aa01d2b10838df7e955f8e8f0d3707 and reconciled open PR1/2/3/12/19.
Branch codex/join-bounded-publication-20261003 uses that main as base.
Latest main runtime146499d2 validation37103497183 completed BOTH workloads and
failed the evidence gate (not the old pre-HTTP storage crash): small job111147445471
checkpoint P95/P99=26.047165585/36.822977392s, pending0; million job111147445348
checkpoint P95/P99=165.226829191/207.964109828s, pending0, workload_passed=true.
Artifacts11267510189/11267972137 are being inspected for complete report/gate
identity. Next implement bounded durable job registration with a sealed manifest,
claim/ack isolation and restart/real-writer tests; then bootstrap chunking with
fixed-W pins. Existing candidates remain unmerged; formal thresholds unchanged.
No generic BEGIN retry is presented as throughput or final certification.


### 2026-10-03 bounded job publication implementation checkpoint

Both complete artifacts were downloaded and gate/workload identities verified at
146499d2: small11267510189 and million11267972137 fail ONLY latency_p95/p99.
All seven full output targets, four dynamic tasks, strong-exit recovery and drain
pass. Sample densities0.901890/0.971664; daemon write bytes2,780,401,664 and
33,826,349,056 respectively. Million is now completed development correctness
evidence, not a performance pass or formal50M72h. Lock owner remains unproven.

Implementation in j4.py/join_job_bridge.py: persistent spool fingerprint, cursor,
record count and sealed manifest; at most32 jobs/16MiB payload per write chunk
(config may lower), with a singleton bounded by existing max_row_bytes. Spool
validation/Arrow routing/read happen outside the write transaction. Jobs,links,
cursor and pending bytes commit together. active_jobs hides incomplete chunks;
ack rejects even manually assigned unsealed jobs; final seal is a metadata update.
Exact recreated spool resumes prior jobs; stale registration is a no-op and changed
spool fails closed. Legacy completed links remain accepted; view upgrade preserves
existing data; manifest is cascade-deleted with its output commit. This bounds
write work, not elapsed lock wait or disk latency. Bootstrap seed remains unbounded
and is the next separate structural milestone.

Local Python3.12/SQLite3.53.1, pinned DuckDB1.5.5/sqlglot30.18.0:
python tools/join_publication_test.py (15 tests including existing4 twice and
7 new WAL/crash/ack/upgrade contracts), job_bridge/runtime/shared/generation,
100 exact/subview/follower-GC and physical registry contracts PASS.
Original83b57fa bridge fails real independent-writer spool-read regression with
actual SQLite BUSY. Benchmark subprocess tests2 PASS;100k production-topology
cache/current full bag exact, current400jobs/RSS246624256bytes/wall1.410s,
cache16jobs/RSS418217984bytes/wall1.546s; local synthetic only.
Direct cdc_selftest still blocked at resource_tree_stats /proc assertion locally;
required hosted baseline/native/state/eight E2E/smoke must validate full change.
Next commit focused job-publication PR and preserve its exact SHA while CI runs;
continue fixed-W bootstrap staging on a separate branch.


## Active resumable fixed-W JOIN output build

PR21 https://github.com/justgo4/m2s/pull/21 tests cb181e3cd0c2fb0d47e714574963711a81a03c62;
all five workflow families running37114618420/37114618497/37114618465/37114618445/37114618424.
Do not rewrite that head during CI. Branch codex/join-output-build-20261003 starts
at cb181e3 for the separate bootstrap milestone. Implement bounded unpublished
primary-generation pair enumeration with left+right durable cursors, input scan
and byte caps, output commit sealed state, canonical digest outside write lock,
and fixed-W pin retained until atomic consumer/generation activation. Shared
follower initialization still uses its existing atomic path until its mutable
leader frontier can be safely frozen; do not pretend primary chunks bound it.
Next files join_output_build.py/join_outbox.py/join_generation.py/
join_log_consumer.py/join_runtime.py and real WAL/high-fanout/pin/crash tests.


### 2026-10-03 primary output build implementation and PR21 integration

PR21 tested cb181e3 all five workflow families PASS: baseline37114618420,
native37114618497, state37114618465, eight E2E37114618445, smoke37114618424.
Merged30058ba22fb37277d2dd48a048d5bbd1d6fc46f1; its tree is identical to tested head.
New output-build branch retains that main in its ancestry at final commit.

join_output_build.py now supplies private-generation fixed-W seed chunks with
both left/right cursors, max1000 output rows/4000 scan work/16MiB serialized
bytes per chunk (singleton <= configured max_row_bytes, hard64MiB). Indexed
left pagination and right range cursors include unmatched/NULL rows in scan
budgets. Reads/projection happen outside writes; identities/output rows/cursor/
count commit together. Canonical full-row digest runs outside the write lock;
final seal updates metadata only. Outbox sealed=0 hides incomplete commits from
pending/read/copy/stage/visibility; legacy rows migrate sealed=1. Daemon private
JOIN activation opts in; the fixed-W source pin remains until sealed output,
source consumer and generation activation form the final atomic handoff.
Cancellation uses an abandoned manifest to fence stale builders, deletes rows
and identities in restartable row/byte bounded transactions before pin release.
Shared follower initialization and owner promotion keep their original atomic
contracts: their mutable leader needs a separate durable snapshot/version protocol.
This PR does not claim those paths or incremental fan-out state transactions bounded.

python tools/join_output_build_test.py:11 PASS, real WAL/high fan-out/NULL and
unmatched work bound, byte singleton/rejection, partial reads/claims/copies/ack
fences, chunk/body/seal crash and stale CAS, exact full bag, canonical read
interleaving, old sealed-column migration, pin retained across source advance
and handoff crash, actual retirement cleanup crash+intent+pin+restart.
Original atomic seed reproduces actual SQLite BUSY for a competing writer during
canonical digest; new chunks/digest allow that writer. Runtime/generation/shared,
100 exact/subview/follower GC and physical registry contracts PASS; privacy/diff/
compile checks PASS. Curated100k independent cached/stream/chunked process
report reports/join-output-build-local-20261003.json has all digests/counts exact:
cache 1.894347s max txn / 1.894362s total; stream 1.959653s max txn / 1.959669s total; chunked 0.017705s max txn / 1.967813s total. These measured transaction intervals include BEGIN wait and
COMMIT/fsync return; they are observations, not a universal duration guarantee
or daemon/SLO evidence. Full hosted CI on the final SHA is still required.
Next submit focused PR22, preserve its head during CI, and inspect current main
small/million while profiling remaining shared bootstrap/promotion and hot-path
write amplification. Formal50M72h remains open pending persistent isolated host.


### 2026-10-03 streaming/reconnection checkpoint

PR22 https://github.com/justgo4/m2s/pull/22 head
a47179b529859190fec81776c72d21a1dcc0f8cb is submitted; preserve its head during CI.
Baseline37115887643, native37115887559, state37115887577 and staged smoke
37115887652 PASS. Actual daemon37115887635: seven of eight jobs PASS;
transaction/OFF/OFF mixed P11 smoke still running at this observation.
Merge only after all required workflows on that exact head pass.

Main30058ba staged development37115278373 million job111180796242 FAILS
before complete oracle/gate: successful HTTP Merge Commit response returned
TxnId/Label, then submit_merge_async's local BEGIN IMMEDIATE to save TxnId and
clear merge_uncertain raised SQLite BUSY. Artifact11271405731 retains the
failure. This is distinct from prepare-BEGIN contention and from the older
146499d2 million correctness PASS/strict latency FAIL. Do not combine evidence.
Lock ownership is not established by this traceback.

Next branch codex/merge-accepted-persistence-20261003 starts at PR22 head.
Implement only safe local accepted-response identity persistence retry:
never repeat HTTP because local SQLite is busy, never retry transaction body
or COMMIT, retain uncertainty marker if persistence cannot complete, preserve
unknown-output quarantine. Add real WAL contention/single-send/restart and
failure tests; inspect existing state_transaction and response exception scope
before editing. Then required same-head CI and focused PR. Primary chunks
remain separate evidence; shared bootstrap/promotion/fan-out and strict SLO
remain open. Formal50M72h still needs a configured persistent isolated host.

### 2026-10-03 parallel recovery/performance milestone

Owner explicitly requests parallel work until recovery/long-write and strict
latency items finish. PR22 tested a47179b ALL five required workflows PASS,
including full eight E2E37115887635; merged9f4560b19510f1096a235ceb529f8f7b314d55e7.
Accepted-response branch includes that main plus883c5b0 durable checkpoint.

j4.submit_merge_async now exits HTTP retry handling after parsed acceptance.
record_merge_acceptance retries only real SQLITE_BUSY at BEGIN before entering
any body, then writes TxnId/removes UNKNOWN atomically once. Body/COMMIT/fatal
storage/active-transaction failures never trigger HTTP or local body replay.
Shutdown saves acceptance if lock is immediately available; cancellation while
contended leaves the pre-request UNKNOWN marker for restart quarantine.
Identity conflicts fail closed. New merge_accepted_contention_test:6 PASS real
WAL writer contention/single-send/restart reuse/cancel quarantine/graceful stop,
actual trigger body rollback and injected COMMIT/fatal-BEGIN boundaries.
merge_visible_contention_test:3 PASS; merge_commit_identity_test and
merge_quarantine_test PASS; compile/diff PASS. Network uncertainty contract
requires --isolated and is running with that explicit flag. Required hosted CI
on the submitted recovery head remains pending; no SLO certification implied.

Independent performance work investigates shared follower idle observation
writes. New telemetry optimization is being validated separately and is not
included in the focused acceptance-persistence PR. Next submit recovery PR,
complete same-head checks; submit verified idle-write reduction separately and
compare full strict small workload on integrated main. Remaining mutable
shared initialization/owner promotion and high fan-out transactions still need
bounded/versioned protocols if profile establishes material cost.


## Active SQLite write timing (2026-10-03)

Base05e8f168396179d74427bbe663877201ce158d67; independent branch
codex/sqlite-write-timing-20261003. PR26 request-intent recovery head
b8e6d7ab19c4eea6abd1ff5bbaee2effe2e3e96b stays frozen while required
CI37132902557/37132902485/37132902475/37132902430/37132902472 runs.
Remaining lock owner is unproven. Add opt-in bounded in-process transaction
timing for explicit BEGIN IMMEDIATE acquisition/COMMIT-or-rollback residence,
with operation names only (no SQL, paths, values). Preserve original connection
when disabled; report coverage boundaries and open transactions separately.
Next real WAL contention, rollback/fatal/commit boundaries and privacy tests.

Timing implementation: optional TimingConnection records explicit writer acquisition
and transaction residence, cumulative count/total/max per operation, bounded128
buckets and128 active descriptions; scripts/deferred/cursor/implicit coverage is
explicitly excluded. Original connection when flag disabled. Periodic/final daemon
reports include diagnostics when enabled; staged development enables the flag.
9 real SQLite/module tests PASS, with WAL busy/writer interleaving, rollback and
failed COMMIT, automatic rollback, script exclusion, bounds/privacy and original
connection defaults. Instrumented accepted-response6/output-build11/publication15
contracts PASS; privacy/diff PASS. No throughput or lock-owner conclusion yet.

PR26 baseline/native/state/smoke PASS atb8e6d7a. E2E transaction/ON/ON failed
37132902430 job111231406263 artifact11277780160: retirement status observed
before asynchronous follower-binding GC. Retire removes consumer/status/intent
atomically but binding row intentionally remains for gc_retired_followers.
wait_stateful_retired currently waits only status/drain/intent then immediately
asserts binding count; no binding-GC predicate. Unchanged transaction path cannot
execute this PR's Merge Commit change. Retrying that isolated job on the same SHA
after diagnosing evidence; independently fix the test polling boundary rather
than weaken the remaining-subview assertion or modify runtime retirement.
## Active request-intent lock recovery (2026-10-03)

Owner renewed repository authorization in this session. Reconciled main
05e8f168396179d74427bbe663877201ce158d67 and open PR1/2/3/12/19;
PR23/24/25 are merged. Branch codex/merge-request-intent-busy-20261003.
Latest staged37118283696 at0932b9c: small completed workload but gate failed;
million artifact11272977528 fails begin_merge_request BEGIN IMMEDIATE BEFORE
HTTP, no complete oracle/gate. Do not conflate with accepted-response recovery.
Implement optional stop-aware BEGIN-only acquisition using existing transaction
helper; body/COMMIT remain one-shot and request intent must commit before HTTP.
Next real WAL contention/cancel/restart and fatal/body/commit tests, then exact
head hosted CI. Strict SLO and persistent-host50M72h remain open.

Implementation: begin_merge_request accepts optional stop and opts into existing
BEGIN-only BUSY retry; submit_merge_async supplies runtime stop before HTTP scope.
Local pinned dependencies installed in /workspace/m2s-deps. Commands:
PYTHONPATH=/workspace/m2s-deps python tools/merge_accepted_contention_test.py
(11 tests); merge_visible_contention_test.py(3), merge_commit_identity_test.py,
merge_quarantine_test.py, privacy_check.py and git diff --check PASS. Tests cover
real WAL independent writer, intent visible before single send, cancel with no
UNKNOWN/no send and exact restart, body rollback, COMMIT BUSY and fatal/active
BEGIN. Original pre-request path cannot retry acquisition; no claim of bounded
lock duration or improved SLO. Next submit focused PR and require same-head CI.


### Integrated recovery and timing verification

PR26 headb8e6d7a passed baseline37132902557/native37132902485/
state37132902475/smoke37132902472 and eight E2E37132902430 after
isolated re-run of known retirement polling race; merged436a6f6b0fe332c21cddb630521e3cff697ec9b2.
PR27 eb3bb3a passed all five workflow families37137479058/37137479027/
37137479025/37137479082/37137479054. Integrate latest main without
changing either behavior, preserve both PROGRESS histories and require fresh
exact-head checks. PR28 db1eea passed baseline37137663125/native37137663110/
eight E2E37137663128; its main integration is separate.
Synthetic100k inserts/100 FULL WAL commits: diagnostic off0.1369s/on0.2553s
with exact digest equality. Explicit profiling adds overhead, especially short
per-row calls; no fair SLO comparison to uninstrumented runs is claimed.


## Active retirement evidence polling correction (2026-10-03)

Base05e8f168396179d74427bbe663877201ce158d67; branch
codex/retired-follower-gc-wait-20261003. PR26 E2E37132902430 failed
transaction/ON/ON at wait_stateful_retired: retired descriptor observed while
JOIN shared binding awaited background GC. Artifact11277780160 retained.
Runtime intentionally leaves binding until outbox/consumer drain is verified by
GC; immediate binding-count assertion raced that boundary. Keep exact remaining
subview assertions and timeout. Require binding cleanup as part of the existing
retirement wait and read each multi-table observation in one SQLite snapshot.
Next real WAL regressions for retired-before-GC, exact task IDs, owner references,
consistent snapshot and bounded timeout; then hosted E2E on focused test PR.
PR26 remains frozen; PR27 telemetry head eb3bb3a is independently submitted.

Polling implementation complete: each state() observation starts a read-only
SQLite transaction; wait_stateful_retired also requires no binding whose exact
follower/leader task ID matches a retired sink descriptor. Existing timeout,
drain/intent checks, remaining-subview assertions and full-row oracles unchanged.
PYTHONPATH=/workspace/m2s-deps python tools/stateful_retirement_poll_test.py:
5 PASS actual WAL fixtures (both operator kinds retired-before-GC, owner
promotion references, unrelated exact IDs, stuck-binding timeout, concurrent
writer committing between two reads). Original helper fails delayed-GC and
snapshot consistency regressions. Next focused PR and real eight E2E required.


PR28 f714d735 passed baseline37138496463/native37138496455/eight E2E37138496449. Integrate current main01996589 (validated optional timing PR27) and retain all checkpoint history. Fresh same-head checks required.

## 2026-10-03 checkpoint: validated recovery and timing merged, strict candidate pending

- PR #26 tested b8e6d7ab19c4eea6abd1ff5bbaee2effe2e3e96b; baseline37132902557/native37132902485/state37132902475/E2E37132902430/smoke37132902472 PASS; merged436a6f6b0fe332c21cddb630521e3cff697ec9b2.
- PR #27 final integrated head1218c28fc35a8f2d14919007d262ae776d50efb1: baseline37138453977/native37138454012/state37138453924/E2E37138453925/smoke37138453943 PASS; merged01996589c90b90b985e62b17cc0f3926c3f9b3cf. Optional timing records explicit Connection.execute BEGIN IMMEDIATE/EXCLUSIVE only; scripts/cursor/deferred/implicit transactions are outside coverage. Counters do not identify an external lock owner.
- PR #28 final integrated headfca66009ddc271083586964d31769cf1918d070b: baseline37139311021/native37139310939/eight real E2E37139310959 PASS; merged8ccf21e8b33f93183faf17d2df1b7f43630b1aed. Both five new WAL tests and old-helper negative controls validate polling race. Runtime unchanged.
- Inspected main436a6f6 artifact11279537301 gate.json directly: only latency_p95/latency_p99 fail; P95 26.107232331s/P99 32.453450671s, 113 healthy samples /121.433s, all seven full oracles exact, four hot-adds ready, final deliveries/pending zero, recovery catchup196.716s with source continuing. Peak daemonRSS427339776 bytes, CPU204.84s, writes3310665728 bytes. Catchup is moving-tail recovery, not daemon restart downtime.
- PR #29 dc6500e1 has baseline37139384369/native37139384198/state37139384220 and smoke job111250474785 PASS; eight-E2E37139384235 and strict-small job111250474900 pending at this checkpoint. Preserve this head while checking. No synthetic/short-run evidence implies SLO/P11.
- README current review reconciled to actual merged heads and explicit unfinished work. Next inspect exact-head strict candidate result and optional write-timing artifacts, retain any failed candidate unmerged, then narrow the demonstrated hot path. No gates/profile changes.

## Active JOIN affected-row read measurement (2026-10-03)

Base436a6f6b0fe332c21cddb630521e3cff697ec9b2; branch
codex/perf-join-affected-row-reads-20261003. Main canonical workload joins events
to1024 dimensions on bucket. apply_transaction projects only changed PK pairs
but _rows_for_keys_locked reads ALL rows on both sides for each affected key,
before and after. Small50-row left transactions at1M therefore repeatedly read
~50k unchanged left rows although each changed left matches one dimension.
Candidate: snapshot changed PKs by key; scan the whole opposite side only when
changed rows on that key can affect it. Retain full opposite fan-out for right
updates, bilateral net diffs, rekey/NULL/bag semantics and atomic state/outbox.
First independent full-state oracle/randomized and actual row-read measurement;
then exact-head contracts and strict small before performance merge. No claim
that legitimate high fan-out or shared initialization transactions are bounded.

Implementation/measurement: changed PK point reads grouped by key, complete
opposite ranges only where required. 4 new tests PASS, including150 randomized
bilateral source transactions with repeated PK/NULL/rekey/delete/insert, full bag
and independent net-delta oracle,11 fault rollbacks/retries, left/right asymmetric
read bounds. Actual original helper in join_state globals fails both forbidden
unchanged-range scan tests. Existing state/incremental/runtime/shared/subview
contracts PASS. Attempted join_log_consumer_test.py does not exist; runtime test
exercises that layer instead.
Fresh independent child full-range/candidate100k and1M full bags match complete
independent oracle. For ten50-left-row commits at1M, unnecessary left RANGE
rows977000->0 (changed PKs still point-read), right rows1000 both; total apply
2.486598s->0.034148s, max txn0.312853s->0.003753s, FULL WAL commits.
100k0.175836s->0.039498s. Report reports/join-affected-reads-local-20261003.json.
This is synthetic compute only, not daemon/remote/SLO or a bounded fan-out claim.
Performance branches codex/perf-* now run smoke+strict small as PR checks; formal
thresholds unchanged. Next submit exact-head PR and retain failed gates if any.
PR27 integrated1218c28 passed all five families and merged01996589c90b90b985e62b17cc0f3926c3f9b3cf.
PR28 integrationf714d735 remains under baseline/native/eight E2E checks.


Performance candidate is based on retirement-poll integration fca66009ddc271083586964d31769cf1918d070b (PR28), which includes validated main01996589/PR27 timing. Submit with strict small PR gate. Preserve exact head during checks. These histories refer to their own SHAs; no combined certification.

## 2026-10-03 checkpoint: JOIN compute candidate failed strict gate; narrow CDC lane candidate

- PR29 exact dc6500e10e7e00236883aa2fa26fc8a05361526e passed baseline37139384369/native37139384198/state37139384220/all eight E2E37139384235/smoke job111250474785. Strict small run37139384246/job111250474900/artifact11279428154 FAIL only latency_p95/latency_p99: 27.130015355/37.835452662s,110 samples,density0.9083. All seven full-output oracles/four dynamic tasks/strong-exit recovery/drain pass. Keep PR29 unmerged; synthetic benefit does not establish SLO. This optional-instrumented hosted run is not controlled A/B against uninstrumented main436a6f6.
- Optional telemetry after restart captured join_shared_runtime:try_bind hold max6.407146766s, count5621,total22.992190079s; its acquire total48.812321637s. capture_binlog_native acquire max6.435430805s and mark_merge_transaction_visible max5.936379005s. These process-lifetime aggregate timings do not prove a particular waiting call was blocked by that same hold; shared follower bootstrap remains a demonstrated long transaction candidate.
- Gate markers measure raw events queryable latency (tools/longhaul_workload.py visible_markers), not each stateful target SLO. Raw output cdc_age P95≈41.929s, visible commit avg1.251s; unchanged hard lane-width4 can split sixteen-partition CDC into serial physical deliveries with constrained writers. Narrow follow-on codex/perf-cdc-lane-cap-20261003 combines the tested affected-row code with configurable CDC_CDC_BUNDLE_MAX_LANES default16/max64 and existing automatic ceil(partitions/active_writers). Row/byte/prepared budgets, per-lane FIFO, kind/plan barriers and visible watermarks stay intact; no snapshot page or JOIN batch changes from old PR12/#19 imported.
- Local PYTHONPATH=/workspace/m2s-deps python tools/cdc_bundle_test.py: five actual SQLite/Arrow tests PASS: wide durable membership/restart/ack fence, row/byte/reservation budgets, plan/snapshot barriers, explicit cap/writer parallelism/catalog config, actual Arrow preparation/OOM lane shrink/restart/full-row visibility guard. Old four-lane helper fails wide membership negative control as expected. prepare_begin_contention5/merge_visible_contention3/merge_accepted_contention11/source_pipeline_metrics and diff check PASS.
- PR30 docs exact1bbe6412900b297b1f86a5d32bc62b0795aab286 baseline37139977210/native37139977214 PASS; merged622d39ef2473c3c752c561c712ab2e64226af5d4. Preserve that README/progress in fresh combined performance tree and both parent histories. Next exact-head all contracts + strict small; do not merge until the unchanged gate passes. Formal persistent50M72h and shared bootstrap/promotion still open.

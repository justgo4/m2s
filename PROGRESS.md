# m2s continuation checkpoint

Last recorded: 2026-10-03 UTC. Read AGENTS.md and README.md first. This file is
the durable handoff if Codex quota/session ends; it does not promise automatic
AI continuation. Routine repository changes, Actions and validated merges are
authorized by the owner without expiry, subject to actual configured access.

## Immediate next action

1. Main `5a0f22f300d0d9afbfaa141dfe19df4d6b75d193` includes validated PR #11/#13/#14/#15/#16/#17. Inspect fresh main small/million runs; native capture now retries only safe BEGIN acquisition, not transaction bodies or COMMIT.
2. PR #12 head `c509d5ae58dbf4829e38a0685b1e692e912d8b53` has green correctness/smoke but strict small FAILED: P95=19.065292522s/P99=21.066043648s (run37100671707, artifact11266017029). All seven full output oracles, four dynamic tasks, recovery/drain exact; pending0. Do not merge or relax thresholds.
3. Next measurement: JOIN bridge4096 row cap creates many small IPC jobs. Benchmark configurable production topology (16 partitions,16MiB,50000 configured rows), retaining independent full-bag oracle and isolated processes. Current historical benchmark used4 partitions/1MiB/4096 rows. Different hosted revisions/runs are not controlled A/B.
4. Structural remaining work: resumable unpublished JOIN output chunks and bounded atomic enqueue, proving fixed-W pin/consumer/visibility/GC/crash contracts. Current streaming bounds memory, not total write-lock duration. Follower promotion/copy must remain safe.
5. Formal unchanged `p11-50m-50rps-72h-v4` lacks persistent configured resources; inventory403 means UNKNOWN. No SLO/P11/final certification claimed.

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

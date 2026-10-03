# m2s working instructions

## Authorization and continuation

The repository owner authorized Codex on 2026-10-03 to continue the README
roadmap, modify code/docs/tests/workflows, create branches and PRs, run available
CI, and merge validated changes. The authorization has no stated expiry. Do not
ask again for routine repository operations. Follow later owner instructions
and the platform's actual permissions; repository authorization does not grant
access to a machine or credentials that are not configured.

At the start of every work session, read README.md and PROGRESS.md, inspect main
and open PRs, and reconcile their exact commit SHAs before editing. Continue the
next actionable task rather than repeating completed audits. Preserve concurrent
changes and never force-push main to overwrite another contributor.

## Durable progress

Update PROGRESS.md before starting a milestone and after each implementation,
test result, PR, or blocker. Record the branch, base SHA, files, exact commands,
CI run URLs, failure, and immediate next action. Persist checkpoints to GitHub,
not only the chat or an ephemeral workspace. Do not label an item done until its
required evidence exists. No agent can automatically continue reasoning after
its session/usage stops; the repository record is the handoff to the next turn.

## Implementation and evidence

Python follows the existing function/underscore style; do not introduce typing
or logging. Use print(..., flush=True) for runtime output. Keep formal
p11-50m-50rps-72h-v4 unchanged; smaller profiles are development evidence.
Preserve transaction, fixed-W pin/consumer/GC, generation, and unknown-output
fail-closed contracts. Keep genuine full-row oracles and failure artifacts.

Run meaningful checks for changed behavior. Submit focused code PRs, verify
the required CI on that change, and merge when green under the owner's existing
authorization. Record which SHA was tested; do not combine different revisions
into one certification. Use small profiles first and measure before replacing
the state engine or native hot paths. Long-running workloads require isolated
test services and persistent resources; never initialize a workload over an old
run directory or production databases.

Only synthetic code/config/data may enter GitHub. Credentials, production
addresses/data/logs, SQLite/WAL, run directories and metrics stay outside it.

# Task Ledger / Task Bus / Mission Control Automation Roadmap

## Purpose

Turn the task-ledger/task-bus stack into a more automatic, trustworthy operational spine for OpenClaw, while documenting the portable pieces we want to carry into Mercury.

This roadmap is based on real operator pain observed during Story 4 / Story 5 work:

- task truth lagging behind agent reality
- Mission Control accurately reflecting stale ledger state
- old blocked tasks dominating current agent status
- handoffs depending too much on manual task transitions
- dashboards mixing explicit truth with inferred truth without saying which is which

## Core principle

Make the **ledger more authoritative** and Mission Control more of a **live projection** over that authority.

That means:

- agents should publish lifecycle changes more automatically
- heartbeats should carry stronger current-task context
- Mission Control should project the ledger honestly, with freshness/confidence cues
- reconciliation should happen in a controlled operational layer, not as silent UI magic

## Current gap

Today the biggest issue is not that Mission Control is "just wrong."
The bigger issue is that the upstream task-ledger/task-bus flow is only partially automated.

That creates a recurring failure mode:

1. agent starts real work
2. task state stays `todo` or old blocked work remains attached
3. Mission Control projects the ledger faithfully
4. operator perceives the dashboard as stale/untrustworthy

So the roadmap focus should be upstream automation first, then projection quality, then operator controls.

---

# P0 — Must-have automation and truth hardening

## P0.1 Automatic task lifecycle publishing from agent execution

### Goal

Stop depending on humans to manually reconcile the most common task transitions.

### Required behavior

When an agent enters a real scoped implementation lane, the runtime should be able to publish:

- `todo -> in_progress`
- `in_progress -> blocked` with reason
- `in_progress -> qa`
- `qa -> done`
- task notes with verification context

### Suggested shape

Introduce a thin standard command surface around the ledger publisher:

- `task.start(...)`
- `task.block(...)`
- `task.note(...)`
- `task.qa(...)`
- `task.done(...)`

These should route through the canonical ledger write path, not bypass it.

### Why this matters

This removes the biggest source of Mission Control drift: real work starts, but the canonical task state never changes.

### Mercury portability

Portable almost as-is. Mercury should also have a small task command API around its canonical publisher.

---

## P0.2 Explicit current-task contract in agent heartbeats

### Goal

Make agent activity reliably attributable to current work.

### Required heartbeat fields

Every meaningful agent heartbeat should carry:

- `agentId`
- `status`
- `currentTaskId`
- `lane`
- `sessionKey`
- `worktree`
- `branch`
- `summary`
- `phase` / run metadata where relevant

### Why this matters

Mission Control should not have to guess which task an agent is carrying from a mix of old assignments and latest activity.

### Mercury portability

Portable conceptually. Names can differ, but the same contract should exist.

---

## P0.3 Controlled task-reality reconciliation

### Goal

Catch and repair obvious truth drift between session reality and ledger reality.

### Example drift cases

- agent has an active run, but assigned task is still `todo`
- task is `in_progress`, but assigned agent has been idle for hours
- agent has old blocked tasks plus one newer active task
- worktree/branch exists for a task, but no `in_progress` transition was published
- agent heartbeat `currentTaskId` disagrees with latest task assignment reality

### Required behavior

Create a reconciliation layer that can:

- detect drift
- emit warnings
- suggest safe fixes
- optionally apply deterministic safe reconciliations

Escalation and reassignment rules should stay ledger-native:

- activation misses should have explicit thresholds, not vague "follow up later" residue
- repeated proof-checkpoint misses / status loops should escalate on a fixed threshold and become reassignment-eligible on a later threshold
- stale or superseded ownership should define a ledger takeover path: publish the ownership change in the task ledger first, then let the gaining owner heartbeat the task
- Mission Control actions should remain a control surface over those ledger writes, never a second truth store

### Important rule

Do not hide this inside silent UI reads.
Reconciliation should be an explicit operational behavior with explainable output.

### Mercury portability

Portable and desirable. This is one of the strongest candidates to carry into Mercury early.

---

## P0.4 Mission Control freshness + confidence cues

### Goal

Make the operator surface honest about projection quality.

### UI should show

- snapshot age
- last ledger event age
- per-panel freshness
- whether current task is:
  - explicit from heartbeat
  - explicit from task state
  - inferred fallback
- whether projection looks stale or reconciled

### Why this matters

If the dashboard may lag, the UI should say so clearly instead of implying unwarranted certainty.

### Mercury portability

Portable. Any operator dashboard benefits from explicit projection-confidence cues.

---

# P1 — High-leverage structure improvements

## P1.1 First-class task relations

### Goal

Let the ledger express structure instead of treating tasks as isolated cards.

### Fields to add or normalize

- `parentTaskId`
- `dependsOn`
- `blockedBy`
- `supersedes`
- `originTaskId`
- `workstream`

### Why this matters

This makes rollups, blockers, and stale superseded work far easier to reason about.
It also reduces the chance that old blocked tasks visually dominate newer active work.

### Mercury portability

Strongly portable.

---

## P1.2 Better lifecycle metadata for task participation

### Goal

Improve rollups and automation by distinguishing active from non-participating children.

### Important lifecycle modes

- `parked`
- `superseded`
- `cancelled`
- `abandoned`
- `blocked`

### Why this matters

Parent rollups and current-task logic become much safer when non-participating work is clearly marked.

### Mercury portability

Portable and important.

---

## P1.3 Operator-side reconcile actions in Mission Control

### Goal

Let the operator fix obvious state mismatches from the dashboard without inventing dashboard-owned truth.

### Good candidate actions

- mark started
- mark blocked/unblocked
- add note
- mark QA/done
- reconcile stale task state
- reassign task
- open active lane/session/worktree

### Rule

Mission Control remains a control surface over the ledger, not an independent truth store.

### Mercury portability

Portable, but should come after publisher discipline and reconciliation rules exist.

---

# P2 — Broader automation and maintenance

## P2.1 Nightly operational hygiene

### Goal

Use a scheduled maintenance pass to improve both memory quality and operational truth hygiene.

### Candidate checks

- stale `in_progress` tasks
- orphaned blocked tasks
- inactive agent/task mismatches
- stale worktree references
- duplicated or low-value notes
- memory dedupe / summary regeneration / provenance-preserving compaction

### Why this matters

Nightly maintenance should operate over the real episode/fact/task substrate instead of placeholder schemas.

### Mercury portability

Portable in principle; exact maintenance jobs may differ.

---

## P2.2 Smarter rollup and stuck-lane handling

### Goal

Reduce human cleanup for long-running projects.

### Candidate automation

- parent task rollup suggestions
- stuck-lane detection
- no-heartbeat-with-open-task warnings
- blocked-too-long escalation
- QA aging detection

### Mercury portability

Portable, but only after base lifecycle correctness is in place.

---

# Recommended implementation order

## OpenClaw / Mission Control

1. **Automatic lifecycle publishing**
2. **Heartbeat current-task contract hardening**
3. **Drift detection + reconciliation warnings**
4. **Freshness/confidence indicators in Mission Control**
5. **Task relations + lifecycle metadata hardening**
6. **Operator-side reconcile controls**
7. **Nightly operational hygiene expansion**

## Mercury carryover order

1. canonical event log + snapshot
2. task command publisher API
3. explicit agent heartbeat contract
4. drift detection/reconciliation
5. projection UI with freshness/confidence
6. task relations + lifecycle metadata
7. operator controls
8. memory bridge / semantic ingest

---

# Suggested acceptance criteria by layer

## Ledger / publisher layer

- agents can publish start/block/qa/done through a stable API
- duplicate publishes remain idempotent
- canonical snapshot updates deterministically
- provenance is preserved for every meaningful transition

## Agent activity layer

- heartbeats include `currentTaskId`, lane, session, worktree, branch
- latest active work is distinguishable from older blocked assignments
- agent status no longer depends on ambiguous fallback inference alone

## Mission Control layer

- board reflects canonical task state without manual reconciliation drift
- agent lanes show current task truth and freshness/confidence
- activity feed remains historical, not mistaken for current truth
- reconcile warnings surface when source/projection drift is detected

## Operator workflow layer

- operator can correct safe state mismatches without editing raw data
- all control actions publish back through the ledger
- UI never becomes the canonical source of truth

---

# Mercury carryover guidance

The most important thing to carry into Mercury is not a specific OpenClaw file layout.
It is the discipline:

- canonical append-only event log
- materialized snapshot
- task command publisher API
- separate agent activity from task truth
- explicit lifecycle metadata
- projection-first UI
- memory as downstream consumer, not truth owner

If Mercury adopts those principles early, it avoids a huge class of “board says one thing, reality says another” failures.

---

# Bottom line

To make the task-ledger/task-bus/Mission Control stack better and more automated:

- automate more lifecycle truth at the source
- enrich heartbeats with explicit current-work context
- add controlled reconciliation instead of silent guessing
- make the operator surface honest about freshness and confidence
- keep Mission Control as a projection/control layer, not the truth store itself

That is the path that improves both OpenClaw now and Mercury later.

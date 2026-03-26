# Task Ledger / Task Bus Design

## Purpose

Document how OpenClaw currently models coordinated work so the pattern can be
ported into other systems (including Mercury) without copying accidental
implementation details.

## Core mental model

- **Task ledger is canonical state + history for coordinated work.**
- **Task bus is the event stream / topic discipline around that ledger.**
- **Mission Control is a projection/operator surface, not the source of truth.**
- **Ethos is a searchable memory layer, not the operational truth.**

That separation matters:
- ledger = what happened / what is active
- Mission Control = what operators see
- Ethos = what agents can recall semantically later

## Current implementation shape

Main implementation lives in:
- `src/infra/task-ledger.ts`

Primary schemas:
- `openclaw.task-ledger.event.v1`
- `openclaw.task-ledger.snapshot.v1`

### Canonical files

The ledger materializes into two durable artifacts:

1. **append-only events log**
   - every accepted task/agent event is written here
   - preserves ordering and provenance

2. **snapshot**
   - latest materialized task/agent state
   - includes recent events for cheap consumers

This gives us:
- durable history
- cheap reads
- replay/debug path
- projection-friendly architecture

## Entities

### Task entity

A task carries the coordination state for real work.

Important fields today:
- `id`
- `title`
- `description`
- `state`
- `priority`
- `source`
- `externalRef`
- `ledgerRef`
- `busTopic`
- `assignedAgent`
- `requestedBy`
- `blockedReason`
- `sessionKey`
- `worktree`
- `metadata`
- `lastEventAt`

Core states:
- `backlog`
- `todo`
- `in_progress`
- `qa`
- `done`
- `blocked`

### Agent activity entity

Agent activity is tracked separately from tasks so the operator surface can show
who is alive/working without conflating that with task truth.

Important fields today:
- `id`
- `name`
- `status`
- `lane`
- `currentTaskId`
- `summary`
- `sessionKey`
- `heartbeatAt`
- `lastSeenAt`
- `metadata`

Core statuses:
- `idle`
- `running`
- `waiting`
- `blocked`

## Event types

### Task-side

Current task event kinds:
- `created`
- `started`
- `state_changed`
- `qa`
- `blocked`
- `note`
- `sync`

### Agent-side

Current agent event kinds:
- `heartbeat`

## Publishing model

The main write API is:
- `publishTaskLedgerEvents(...)`

Input forms today are essentially:
- task upsert
- task transition
- task note
- agent heartbeat

### Important design rule

The publisher is the authoritative place for:
- validation
- id generation
- idempotency handling
- record normalization
- snapshot materialization
- safe write ordering

This is where task-state hygiene belongs.

## Bus semantics

The ledger also acts like a task bus through a stable topic field:
- default topic: `shared.task.ledger`

That topic is not a full pub/sub system by itself; it is the routing/provenance
hint carried with task events so downstream projections and bridges know which
operational stream they are consuming.

### Practical meaning

The “task bus” in our current design is:
- event publication discipline
- stable event schema
- bus topic field
- snapshot/projection consumers

It is not yet a standalone broker abstraction.

## Materialization / projection pattern

The write path produces a materialized snapshot from the canonical event stream.
Consumers should prefer:
- **snapshot for operator/UI reads**
- **events for history/replay/integration**

Mission Control follows this pattern:
- reads task/agent state from the snapshot/projection
- shows board + activity + detail views
- never becomes the source of truth itself

## Rollup / lifecycle hygiene

Recent hardening work established a few important rules:

### Parent/child rollups should be write-path behavior

If a parent task should roll forward based on child story/QA progress, that
happens on the write path, not by mutating state during arbitrary reads.

Why:
- read-path mutation is hard to reason about
- it creates process-safety issues
- it can create duplicate synthetic events

### Explicitly blocked parents are authoritative

If a parent task is explicitly marked `blocked`, rollups must not silently
promote it to `qa`/`done` just because children progressed.

### Parked / superseded / cancelled children should not participate

Lifecycle metadata matters. Children marked as:
- `parked`
- `superseded`
- `cancelled`
- `abandoned`

should not drive parent completion rollups.

### Avoid clever silent healing

Prefer deterministic, explicit rollup rules over magical self-healing reads.
Operational truth should be explainable from the ledger events.

## Mission Control integration

Mission Control consumes the ledger as an operator projection.

### It should rely on the ledger for:
- board columns / task state
- assigned agent and worktree context
- blockers and notes
- agent heartbeats / current task

### It should not invent its own truth for:
- task completion
- stale task cleanup
- lane ownership
- parent/child lifecycle semantics

That logic should remain in the ledger/runtime layer, with Mission Control
surfacing the result.

## Ethos integration

The ledger is operational truth.
Ethos is the searchable memory layer.

So the correct bridge pattern is:
- select high-signal ledger events
- transform them into concise episodic records
- preserve provenance back to ledger event ids / task ids
- keep task truth in the ledger

This is exactly the Story 2 direction.

## What should carry into Mercury

If we port this into Mercury, the portable ideas are:

### 1. Canonical append-only event log + materialized snapshot

Do not make the dashboard/UI the source of truth.
Keep:
- append-only event history
- materialized snapshot for cheap reads

### 2. Stable task schema

Mercury should keep the same conceptual fields:
- stable task id
- state
- priority
- owner/agent
- requested by
- provenance refs
- metadata for integration-specific context

### 3. Separate agent activity from task truth

Agent heartbeats/current task are useful, but they should remain distinct from
actual task state transitions.

### 4. Explicit lifecycle metadata

The port should preserve lifecycle semantics for:
- parked
- superseded
- blocked
- cancelled/abandoned

This is where many “smart board” bugs come from.

### 5. Event-driven projections

Mercury can use a different UI, but the pattern should stay:
- ledger events
- snapshot projection
- operator-facing surface built on projections

### 6. Memory bridge as a downstream consumer

If Mercury gains memory/recall, it should ingest from the operational ledger as a
consumer, not merge memory records into the canonical task log itself.

## What is OpenClaw-specific vs portable

### OpenClaw-specific details
- current schema names
- current file paths
- Mission Control implementation details
- OpenClaw session keys / worktree conventions
- Ethos hook plumbing

### Portable concepts
- append-only operational event log
- materialized snapshot
- stable task states
- separate agent activity stream
- parent/child rollup discipline
- lifecycle metadata
- projection-first UI model
- memory as downstream semantic consumer

## Recommended Mercury adaptation checklist

If Mercury adopts this pattern, start with:

1. define Mercury task + agent schemas
2. define canonical event log + snapshot format
3. build publisher API with idempotency + validation
4. implement basic board projection
5. add parent/child rollup rules carefully
6. add lifecycle metadata before fancy auto-healing
7. only after that, add memory/semantic ingest bridge

## Follow-on roadmap

For the automation / reconciliation / Mission Control hardening roadmap that should also carry into Mercury, see:
- `docs/design/task-ledger-mission-control-automation-roadmap.md`

## Bottom line

The task ledger / task bus is not just “a board backend.”
It is the operational spine that lets:
- agents coordinate safely
- dashboards stay honest
- memory systems ingest meaningful episodes
- long-running work survive chat/session fragility

That is the part worth carrying into Mercury.

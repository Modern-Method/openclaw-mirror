# OpenClaw + Ethos Pre-Prompt Recall Design

## Overview

This document defines the next memory architecture slice for OpenClaw + Ethos:
a runtime-owned pre-prompt recall path that pulls scoped memory **before** the
model sees the inbound user message, plus the follow-on ingestion,
observability, and maintenance work needed to make that recall trustworthy.

The immediate objective is to move OpenClaw from **agent-remembered recall**
("the model/tool call must remember to search") to **runtime-managed recall**
("the system injects bounded, policy-safe recall at the right time").

## Current Reality

Today OpenClaw already has several building blocks:

- `src/agents/pi-embedded-runner/run/attempt.ts` logs pre-prompt context
  diagnostics immediately before `activeSession.prompt(...)`.
- `src/infra/task-ledger.ts` persists structured task and agent events to
  `~/.openclaw/shared/task-ledger/`.
- `src/gateway/task-ledger-agent-activity.ts` and `src/gateway/server.impl.ts`
  already publish agent lifecycle activity into the task ledger.
- Ethos already handles scoped ingest + search, but OpenClaw does not yet own a
  first-class runtime recall orchestrator that decides what memory gets injected
  into model context.

That means we can already **record truth** and **search truth**, but we still
lack the orchestration layer that turns those capabilities into reliable,
defensible model context.

## Goals

1. Inject relevant recall **before** the model sees the new user turn.
2. Preserve strict scope/sender safety so recall never leaks across users,
   channels, or unrelated sessions.
3. Keep the model-facing recall block small, legible, and provenance-light.
4. Treat the task ledger as operational source-of-truth and Ethos as the recall
   and retrieval layer.
5. Make retrieval decisions inspectable by operators without forcing them to
   read raw prompt assembly logs.
6. Roll out safely behind canary gates and fail open when dependencies are
   unavailable.

## Non-Goals

- Replacing workspace Markdown memory (`MEMORY.md`, `memory/YYYY-MM-DD.md`).
- Dumping raw Ethos metadata or full search payloads directly into prompts.
- Making Ethos the system of record for task execution state.
- Deleting historical memory aggressively as part of the initial rollout.

## Proposed Architecture

### Story 1 — Runtime pre-prompt recall canary

Add a new recall orchestration step in the prompt path before
`activeSession.prompt(...)`.

Recommended flow:

1. Build a **recall request** from the current run context:
   - `sessionKey`
   - `senderId` / author identity (required for live recall)
   - channel/chat type
   - agent id
   - sanitized inbound user text
2. Evaluate a **deny-by-default canary gate**:
   - default off unless an explicit allowlist or flag enables it
   - skip recall when `senderId` is missing
   - keep group/shared surfaces stricter than DM/private surfaces
3. Query Ethos for candidate memories using the smallest viable scope.
4. Rank and trim candidates into a bounded **recall envelope**.
5. Inject only a short model-facing block, for example:

   ```text
   Relevant memory:
   - Michael is in Metro Manila (recently confirmed).
   - Forge is currently implementing Story 1 of the pre-prompt recall epic.
   ```

6. Log a structured trace describing:
   - why recall ran or was skipped
   - what scope was applied
   - candidate count vs injected count
   - truncation / budget decisions
   - dependency failures or safety denials

#### Guardrails

- No raw Ethos metadata in model-facing prompt text.
- Minimal provenance only (enough to support trust, not enough to leak internals).
- Hard character/item budgets.
- Fail open: if Ethos is down, prompt execution continues without recall.
- Regression tests must prove no-leak behavior across scope boundaries.

#### Recommended OpenClaw surface

Add a small internal module such as:

- `src/memory/preprompt-recall.ts`

Responsibilities:

- policy gating
- query construction
- result filtering/ranking
- prompt block rendering
- structured trace emission

Keep prompt assembly ownership in `src/agents/pi-embedded-runner/run/attempt.ts`,
but move recall logic into a focused helper so it stays testable.

### Story 2 — Task-ledger episodic ingest bridge

Important task-ledger events should become searchable episodic memory in Ethos,
without making Ethos the task ledger itself.

#### Why

The task ledger already captures valuable operational truth:

- task creation
- transitions (`todo`, `in_progress`, `qa`, `blocked`, `done`)
- blockers and summaries
- agent lifecycle heartbeats
- handoffs and completion notes

Those events are exactly the kind of cross-session context that helps an agent
answer questions like:

- "What is Forge working on right now?"
- "Why is Pixel blocked?"
- "What did Neko already decide this morning?"

#### Design

- Keep `~/.openclaw/shared/task-ledger/` as source-of-truth.
- Add a publisher/bridge that selects important ledger events and ingests them
  into Ethos as **episodes**.
- Store enough provenance to link back to the originating ledger record or task
  id, but do not overload prompts with that provenance.
- Preserve event time, actor, task id, state transition, and concise summary.

#### Ingestion policy

Ingest by default:

- task `created`
- task `started`
- `state_changed`
- `blocked`
- `qa`
- high-signal `note`
- agent heartbeat changes that materially change operator understanding

Do not ingest by default:

- noisy repetitive heartbeats with no semantic state change
- raw internal debug blobs
- giant payloads that duplicate ledger truth verbatim

### Story 3 — Durable fact lifecycle semantics

Not all memory should be treated as equally current. Durable facts need
lifecycle state so stale truth stops outranking live truth.

Recommended lifecycle states:

- `active`
- `superseded`
- `disputed`
- `archived`

Recommended metadata:

- `verifiedAt`
- `supersededAt`
- `lastAccessedAt`
- `replacedBy`
- `sourceKind` (`workspace_memory`, `episodic_ingest`, `operator_fact`, etc.)

Retrieval should prefer `active` facts over `superseded` facts unless the user
explicitly asks for history.

### Story 4 — Mission Control observability

Operators need to see **why** the system recalled something, not just that it
happened.

Mission Control should expose:

- whether pre-prompt recall ran
- the applied scope / safety gate outcome
- top matched memories or episodes
- what was injected vs withheld
- truncation / budget outcomes
- dependency errors / Ethos unavailability

This should be operator-facing observability, not raw prompt dump theater.
The goal is faster debugging and trust calibration.

### Story 5 — Conservative nightly maintenance

Once episodic ingest is live, memory volume will rise. We need conservative,
auditable cleanup rather than silent decay.

Maintenance should:

- dedupe low-value near-duplicate episodes
- consolidate obvious repeats
- refresh summaries/indexes if needed
- never erase important provenance or lineage
- emit logs/reports for operator review

## Data Model Guidance

### Recall envelope returned to prompt assembly

Suggested shape:

```ts
{
  ran: boolean;
  skippedReason?: string;
  scope: {
    sessionKey?: string;
    senderId?: string;
    channelClass?: "dm" | "group" | "unknown";
  };
  candidatesConsidered: number;
  injected: Array<{
    text: string;
    provenance?: string;
  }>;
  injectedChars: number;
}
```

### Episodic ingest record guidance

Each Ethos episode derived from the task ledger should preserve:

- `taskId`
- event kind / transition
- actor id/name
- timestamp
- short summary
- source pointer to the ledger event id
- scope fields required for safe later retrieval

## Rollout Plan

### Phase 1

- Land Story 1 behind an explicit canary/allowlist.
- Log decisions even when recall is skipped.
- Prefer private/DM surfaces first.

### Phase 2

- Add Story 2 episodic ingest from task ledger into Ethos.
- Validate search quality on real operator questions before widening rollout.

### Phase 3

- Add fact lifecycle semantics and Mission Control observability.
- Only then consider broader default enablement.

### Phase 4

- Add nightly maintenance with auditable summaries.

See also: `docs/design/task-ledger-task-bus.md` for the operational substrate this memory architecture is building on.

## Current Implementation Status (2026-03-24)

### Completed

- **Story 1 — Runtime pre-prompt recall canary** is implemented, locally reviewed,
  promoted into the live bundled hook path, and QA-cleared for the Neko-only
  canary lane.
- The Neko-only canary now has explicit evidence for:
  - non-canary denial on Forge
  - owner-session fallback when `senderId` is missing
  - fail-open behavior when Ethos is unavailable
  - bounded/minimal injection behavior
  - local Cubic review cleanliness

### In Progress

- **Story 2 — Task-ledger episodic ingest bridge** is the current active backend
  slice. The implementation lane is centered on canonical task-ledger events and
  the existing `ethos-ingest` transport path.

### Queued / Depends On Story 2

- **Story 3 — Durable fact lifecycle semantics** should follow Story 2, because
  episodic ingest provides the first production-scale stream of memory records
  that will benefit from explicit lifecycle ranking.
- **Story 4 — Mission Control observability** depends on Story 1 + Story 2:
  without real recall traces and episodic ingest, the UI would only be mock
  observability.
- **Story 5 — Nightly maintenance** depends on Story 2 + Story 3 so maintenance
  can reason over actual episodes/facts instead of placeholder record classes.

## Contract Guidance For The Next Stories

### Story 2 — Episodic ingest contract

Each high-signal task-ledger event selected for Ethos ingest should preserve the
following shape at the bridge boundary:

```ts
{
  kind: "task_episode";
  taskId: string;
  taskTitle: string;
  eventKind: string;
  state?: string;
  summary: string;
  ts: string;
  actor?: { id?: string; name?: string; type?: string };
  source: {
    ledgerEventId: string;
    ledgerTaskId: string;
    busTopic?: string;
  };
  scope: {
    requestedBy?: string;
    assignedAgent?: string;
    channelClass?: "dm" | "group" | "unknown";
  };
}
```

Rules:
- preserve provenance back to the ledger event id
- keep the episode concise and semantic
- do not dump full task objects or raw debug metadata into Ethos
- treat the task ledger as canonical, Ethos as searchable memory

### Story 3 — Fact lifecycle contract

Durable facts should carry explicit freshness/authority semantics:

```ts
{
  kind: "fact";
  status: "active" | "superseded" | "disputed" | "archived";
  verifiedAt?: string;
  supersededAt?: string;
  replacedBy?: string;
  sourceKind: "workspace_memory" | "episodic_ingest" | "operator_fact";
}
```

Retrieval policy should prefer:
1. `active`
2. `disputed` (visible but clearly marked)
3. `superseded`
4. `archived`

### Story 4 — Observability contract

Mission Control should not scrape prompts directly. The runtime should emit a
structured recall trace record that the UI can display safely:

```ts
{
  ts: string;
  sessionKey: string;
  agentId: string;
  ran: boolean;
  skippedReason?: string;
  scope: {
    senderId?: string;
    channelClass?: "dm" | "group" | "unknown";
  };
  candidatesConsidered: number;
  injectedCount: number;
  injectedChars: number;
  withheldCount?: number;
  dependencyStatus: "ok" | "timeout" | "error" | "skipped";
}
```

Rules:
- operator-visible, not model-visible
- enough detail for debugging
- no raw prompt dump theater
- no raw Ethos metadata leakage into Mission Control cards

Finalized runtime contract in OpenClaw:
- emit these records through the existing task-ledger/event substrate
- use `entity: "recall"` + `kind: "trace"` rather than inventing a separate sink
- keep Mission Control consumption on ledger snapshot/event reads, not prompt scraping

## Story 3 — Durable Fact Lifecycle Implementation Checklist

### Goal

Introduce explicit lifecycle semantics for durable facts so retrieval can prefer
current truth over stale truth without losing provenance.

### Likely files to touch

- `src/hooks/bundled/ethos-context/handler.ts`
  - retrieval ranking / filtering logic should respect lifecycle status when
    deciding what becomes prompt-facing recall
- `src/hooks/bundled/ethos-context/handler.test.ts`
  - regression coverage for lifecycle-aware retrieval ordering
- `src/hooks/bundled/ethos-ingest/handler.ts`
  - if promoted episodes/facts use the same ingest surface, this is the likely
    bridge point for fact-shaped records
- `src/hooks/bundled/ethos-ingest/handler.test.ts`
- `src/infra/task-ledger.ts`
  - only if task-ledger-derived durable facts become an explicit output class;
    otherwise keep Story 3 out of the ledger path
- `src/infra/task-ledger.test.ts`
  - only if Story 3 adds ledger-produced fact promotion hints
- `docs/design/openclaw-ethos-preprompt-recall.md`
  - update with the final retrieval precedence once implemented

### Required implementation behaviors

1. **Lifecycle field model exists**
   - `active`
   - `superseded`
   - `disputed`
   - `archived`

2. **Retrieval ranking prefers active facts**
   - active facts should outrank superseded/archived ones even if older items
     are still semantically similar

3. **Superseded facts remain searchable but not preferred**
   - keep lineage/provenance
   - do not silently erase history

4. **Prompt-facing recall stays minimal**
   - lifecycle should influence selection, not blow up the size of the injected
     memory block

5. **Promotion remains explicit**
   - Story 3 should not silently promote every episode to a durable fact

### Acceptance gates

- lifecycle schema is represented in the runtime-facing record model
- retrieval code has deterministic precedence rules for lifecycle state
- superseded facts never outrank active facts in prompt-facing recall
- disputed facts are visible in operator/debug traces without being silently
  treated as canonical truth
- tests cover:
  - active vs superseded precedence
  - disputed fact behavior
  - archived fact non-preference
  - lineage preserved after supersession
- local Cubic review: `No issues found`

## Story 4 — Mission Control Retrieval Observability Checklist

### Goal

Expose memory retrieval and injection decisions in Mission Control without
turning the UI into a prompt dump or leaking raw Ethos metadata.

### Likely files to touch

#### OpenClaw source
- `src/hooks/bundled/ethos-context/handler.ts`
  - emit structured recall trace records/events
- `src/hooks/bundled/ethos-context/handler.test.ts`
  - ensure trace emission is correct and safe
- `src/infra/agent-events.ts`
  - likely destination for a first-class trace event shape if the runtime does
    not already expose one
- `src/infra/task-ledger.ts`
  - only if observability is routed through the ledger/event substrate instead
    of a dedicated trace channel

#### Mission Control
- gateway task-ledger sync surfaces
  - `src/gateway/server-methods/tasks.ts`
  - extend `tasks.snapshot` / `tasks.events` consumers if Mission Control needs
    to show recall traces alongside the existing ledger projection
- gateway task-ledger write path
  - `src/infra/task-ledger.ts`
  - keep recall observability on the canonical ledger/event substrate rather
    than introducing a dashboard-specific ingest API
- Mission Control/gateway tests:
  - `src/gateway/server-methods/tasks.test.ts`
  - `src/commands/orchestrator-smoke.test.ts`

### Required implementation behaviors

1. **Trace record emitted by runtime**
   - whether recall ran
   - skip reason when it didn’t
   - candidate count
   - injected count
   - injected chars
   - dependency status (`ok` / `timeout` / `error` / `skipped`)

2. **Mission Control can display traces without raw prompt dumps**
   - no giant prependContext blobs
   - no raw Ethos metadata object dumps
   - no hidden instruction leakage

3. **Operator can understand why recall did or did not happen**
   - sender/scope hints
   - whether canary gating blocked it
   - whether there were zero candidates
   - whether dependency failure forced fail-open

4. **Trace UI stays distinct from task/activity feed**
   - this should feel like memory observability, not generic noise in the
     existing activity feed

### Acceptance gates

- runtime emits a structured recall trace record for every recall decision
- Mission Control sync path can ingest/display those traces through the
  existing `tasks.snapshot` / `tasks.events` surfaces
- If Mercury later uses Phoenix for its dashboard, preserve the same contract:
  Phoenix can be the projection/operator UI, but the canonical activity/task
  truth should still flow through a ledger publisher + snapshot/events model
- UI shows at minimum:
  - agent
  - session/thread context
  - ran vs skipped
  - skip reason / dependency status
  - injected count / chars
- UI does **not** show raw prompt dumps or raw Ethos metadata
- tests cover:
  - canary skip visibility
  - zero-candidate visibility
  - fail-open visibility
  - successful injection trace visibility
- local Cubic review on any code implementation: `No issues found`

## Suggested execution order after Story 2

1. **Finish Story 2 handoff**
2. **Start Story 3** to lock durable fact lifecycle semantics into the runtime
   before the memory corpus grows
3. **Start Story 4** once Story 2 traces/episodes provide real substrate for UI
   observability
4. **Keep Story 5 last** so nightly maintenance operates on the real episode/fact
   model instead of a temporary placeholder schema

## Validation Plan

### Story 1 tests

- recall skipped when `senderId` is missing
- canary disabled => no recall injection
- cross-scope memory never injects
- injected block never includes raw Ethos metadata
- prompt budget clamp works on large candidate sets
- Ethos failure does not block the user turn

### Story 2 tests

- important task-ledger transitions ingest successfully
- duplicate/idempotent ledger events do not create noisy episode spam
- retrieved episode provenance links back to ledger truth

### Story 3 tests

- `active` facts outrank `superseded` facts
- disputed facts are visible but clearly marked

### Story 4 tests

- Mission Control trace matches the runtime recall decision record
- operator UI shows injected vs withheld outcomes without raw prompt leakage

### Story 5 tests

- maintenance logs are reviewable
- dedupe improves clutter without destroying lineage

## Files / Surfaces Likely to Change

### OpenClaw

- `src/agents/pi-embedded-runner/run/attempt.ts`
- `src/memory/preprompt-recall.ts` (new)
- config/docs for canary gating + debug output
- task-ledger-to-Ethos bridge module(s)

### Ethos

- ingest/search schema for episodic task-ledger events
- fact lifecycle fields + retrieval ranking semantics
- tests proving scope-safe retrieval and active-vs-superseded ordering

### Mission Control

- retrieval trace ingestion/display
- task/agent drill-down views for recall observability

## Bottom Line

The right move is not "more memory" in the abstract. The right move is a
**runtime-owned memory orchestrator** with sharp safety boundaries, searchable
operational episodes, and operator-visible reasoning traces.

That gets us closer to trustworthy long-horizon agents without turning prompt
assembly into an unbounded memory dump.

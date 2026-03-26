import type { VerboseLevel } from "../auto-reply/thinking.js";

export type AgentEventStream = "lifecycle" | "tool" | "assistant" | "error" | (string & {});

export type AgentEventPayload = {
  runId: string;
  seq: number;
  stream: AgentEventStream;
  ts: number;
  data: Record<string, unknown>;
  sessionKey?: string;
  runContext?: AgentRunContext;
};

export type AgentRunContext = {
  sessionKey?: string;
  verboseLevel?: VerboseLevel;
  isHeartbeat?: boolean;
  lane?: string;
  currentTaskId?: string;
  worktree?: string;
  branch?: string;
  /** Whether control UI clients should receive chat/agent updates for this run. */
  isControlUiVisible?: boolean;
};

// Keep per-run counters so streams stay strictly monotonic per runId.
type AgentEventsRuntimeState = {
  seqByRun: Map<string, number>;
  listeners: Set<(evt: AgentEventPayload) => void>;
  runContextById: Map<string, AgentRunContext>;
};

const AGENT_EVENTS_STATE_KEY = "__openclaw_agent_events_state__";
const globalState =
  (globalThis as unknown as { [AGENT_EVENTS_STATE_KEY]?: AgentEventsRuntimeState })[
    AGENT_EVENTS_STATE_KEY
  ] ?? {
    seqByRun: new Map<string, number>(),
    listeners: new Set<(evt: AgentEventPayload) => void>(),
    runContextById: new Map<string, AgentRunContext>(),
  };

if (!(globalThis as unknown as { [AGENT_EVENTS_STATE_KEY]?: AgentEventsRuntimeState })[
  AGENT_EVENTS_STATE_KEY
]) {
  (globalThis as unknown as { [AGENT_EVENTS_STATE_KEY]: AgentEventsRuntimeState })[
    AGENT_EVENTS_STATE_KEY
  ] = globalState;
}

const seqByRun = globalState.seqByRun;
const listeners = globalState.listeners;
const runContextById = globalState.runContextById;

function trimToUndefined(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

export function registerAgentRunContext(runId: string, context: AgentRunContext) {
  if (!runId) {
    return;
  }
  const normalizedContext: AgentRunContext = {
    ...context,
    sessionKey: trimToUndefined(context.sessionKey),
    lane: trimToUndefined(context.lane),
    currentTaskId: trimToUndefined(context.currentTaskId),
    worktree: trimToUndefined(context.worktree),
    branch: trimToUndefined(context.branch),
  };
  const existing = runContextById.get(runId);
  if (!existing) {
    runContextById.set(runId, normalizedContext);
    return;
  }
  if (normalizedContext.sessionKey && existing.sessionKey !== normalizedContext.sessionKey) {
    existing.sessionKey = normalizedContext.sessionKey;
  }
  if (normalizedContext.verboseLevel && existing.verboseLevel !== normalizedContext.verboseLevel) {
    existing.verboseLevel = normalizedContext.verboseLevel;
  }
  if ("lane" in context) {
    existing.lane = normalizedContext.lane;
  }
  if ("currentTaskId" in context) {
    existing.currentTaskId = normalizedContext.currentTaskId;
  }
  if ("worktree" in context) {
    existing.worktree = normalizedContext.worktree;
  }
  if ("branch" in context) {
    existing.branch = normalizedContext.branch;
  }
  if (normalizedContext.isControlUiVisible !== undefined) {
    existing.isControlUiVisible = normalizedContext.isControlUiVisible;
  }
  if (
    normalizedContext.isHeartbeat !== undefined &&
    existing.isHeartbeat !== normalizedContext.isHeartbeat
  ) {
    existing.isHeartbeat = normalizedContext.isHeartbeat;
  }
}

export function getAgentRunContext(runId: string) {
  return runContextById.get(runId);
}

export function clearAgentRunContext(runId: string) {
  runContextById.delete(runId);
}

export function resetAgentRunContextForTest() {
  runContextById.clear();
}

export function resetAgentEventsForTest() {
  listeners.clear();
  seqByRun.clear();
  runContextById.clear();
}

export function emitAgentEvent(event: Omit<AgentEventPayload, "seq" | "ts">) {
  const nextSeq = (seqByRun.get(event.runId) ?? 0) + 1;
  seqByRun.set(event.runId, nextSeq);
  const context = runContextById.get(event.runId);
  const isControlUiVisible = context?.isControlUiVisible ?? true;
  const eventSessionKey =
    typeof event.sessionKey === "string" && event.sessionKey.trim() ? event.sessionKey : undefined;
  const sessionKey = isControlUiVisible ? (eventSessionKey ?? context?.sessionKey) : undefined;
  const enriched: AgentEventPayload = {
    ...event,
    sessionKey,
    ...(context ? { runContext: { ...context } } : {}),
    seq: nextSeq,
    ts: Date.now(),
  };
  for (const listener of listeners) {
    try {
      listener(enriched);
    } catch {
      /* ignore */
    }
  }
}

export function onAgentEvent(listener: (evt: AgentEventPayload) => void) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

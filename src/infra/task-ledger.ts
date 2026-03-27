import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { resolveStateDir } from "../config/paths.js";
import { createAsyncLock, readJsonFile, writeJsonAtomic } from "./json-files.js";

export const TASK_LEDGER_SCHEMA = "openclaw.task-ledger.event.v1" as const;
export const TASK_LEDGER_SNAPSHOT_SCHEMA = "openclaw.task-ledger.snapshot.v1" as const;
export const TASK_STATES = ["backlog", "todo", "in_progress", "qa", "done", "blocked"] as const;
export const TASK_PRIORITIES = ["low", "medium", "high", "critical"] as const;
export const AGENT_ACTIVITY_STATUSES = ["idle", "running", "waiting", "blocked"] as const;

const DEFAULT_BUS_TOPIC = "shared.task.ledger";
const DEFAULT_SOURCE = "openclaw";
const DEFAULT_RECENT_EVENT_LIMIT = 200;
const DEFAULT_IDEMPOTENCY_WINDOW_MS = 10_000;
const RECONCILE_ACTOR_ID = "task-ledger-reconciler";
const RECONCILE_ACTOR_NAME = "Task ledger reconciler";
const RECONCILE_IDEMPOTENCY_PREFIX = "reconcile";
const RECONCILE_AGENT_STALE_MS = 15 * 60_000;

export class TaskLedgerPublishInputError extends Error {}
const taskLedgerLock = createAsyncLock();

type TaskLedgerTaskPatch = Partial<Omit<TaskLedgerTask, "id" | "lastEventAt">> & {
  title?: string;
};

type MaterializedLedgerState = {
  tasks: Map<string, TaskLedgerTask>;
  agents: Map<string, TaskLedgerAgentActivity>;
  appliedRecords: TaskLedgerRecord[];
  lastRecordByEntity: Map<string, TaskLedgerRecord>;
  seenIdempotencyKeysByEntity: Map<string, Set<string>>;
};

export type TaskState = (typeof TASK_STATES)[number];
export type TaskPriority = (typeof TASK_PRIORITIES)[number];
export type AgentActivityStatus = (typeof AGENT_ACTIVITY_STATUSES)[number];
export type TaskActorType = "agent" | "operator" | "system";
export type TaskEventKind =
  | "created"
  | "started"
  | "state_changed"
  | "qa"
  | "blocked"
  | "note"
  | "sync";
export type TaskLedgerAgentEventKind = "heartbeat";

export type TaskLedgerActor = {
  type: TaskActorType;
  id?: string;
  name?: string;
};

export type TaskLedgerTask = {
  id: string;
  title: string;
  description?: string;
  state: TaskState;
  priority: TaskPriority;
  source: string;
  externalRef?: string;
  ledgerRef?: string;
  busTopic: string;
  assignedAgent?: string;
  requestedBy?: string;
  blockedReason?: string;
  sessionKey?: string;
  worktree?: string;
  lastEventAt: string;
  metadata: Record<string, unknown>;
};

export type TaskLedgerAgentActivity = {
  id: string;
  name: string;
  status: AgentActivityStatus;
  lane?: string;
  currentTaskId?: string;
  worktree?: string;
  branch?: string;
  summary: string;
  sessionKey?: string;
  heartbeatAt: string;
  lastSeenAt: string;
  metadata: Record<string, unknown>;
};

export type TaskLedgerTaskRecord = {
  schema: typeof TASK_LEDGER_SCHEMA;
  id: string;
  ts: string;
  entity: "task";
  kind: TaskEventKind;
  taskId: string;
  summary: string;
  actor: TaskLedgerActor;
  fromState?: TaskState;
  toState?: TaskState;
  task?: Partial<Omit<TaskLedgerTask, "id" | "lastEventAt">>;
  idempotencyKey?: string;
};

export type TaskLedgerAgentRecord = {
  schema: typeof TASK_LEDGER_SCHEMA;
  id: string;
  ts: string;
  entity: "agent";
  kind: TaskLedgerAgentEventKind;
  agentId: string;
  name?: string;
  status: AgentActivityStatus;
  lane?: string | null;
  currentTaskId?: string | null;
  sessionKey?: string | null;
  worktree?: string | null;
  branch?: string | null;
  summary: string;
  metadata: Record<string, unknown>;
  idempotencyKey?: string;
};

export type TaskLedgerRecallRecord = {
  schema: typeof TASK_LEDGER_SCHEMA;
  id: string;
  ts: string;
  entity: "recall";
  kind: "trace";
  sessionKey: string;
  agentId: string;
  ran: boolean;
  skippedReason?: string;
  scope?: Record<string, unknown>;
  candidatesConsidered: number;
  injectedCount: number;
  injectedChars: number;
  withheldCount?: number;
  dependencyStatus: "ok" | "timeout" | "error" | "skipped";
  idempotencyKey?: string;
};

export type TaskLedgerRecord =
  | TaskLedgerTaskRecord
  | TaskLedgerAgentRecord
  | TaskLedgerRecallRecord;

export type TaskLedgerSnapshot = {
  schema: typeof TASK_LEDGER_SNAPSHOT_SCHEMA;
  generatedAt: string;
  lastEventId?: string;
  paths: {
    rootDir: string;
    eventsFile: string;
    snapshotFile: string;
  };
  tasks: TaskLedgerTask[];
  agents: TaskLedgerAgentActivity[];
  recentEvents: TaskLedgerRecord[];
};

export type TaskLedgerTaskUpsertInput = {
  entity: "task";
  kind: "upsert";
  task: {
    id: string;
    title?: string;
    description?: string;
    state?: TaskState;
    priority?: TaskPriority;
    source?: string;
    externalRef?: string;
    ledgerRef?: string;
    busTopic?: string;
    assignedAgent?: string;
    requestedBy?: string;
    blockedReason?: string;
    sessionKey?: string;
    worktree?: string;
    metadata?: Record<string, unknown>;
  };
  summary?: string;
  actor?: Partial<TaskLedgerActor>;
  ts?: string;
  idempotencyKey?: string;
};

export type TaskLedgerTaskTransitionInput = {
  entity: "task";
  kind: "transition";
  taskId: string;
  state: TaskState;
  summary?: string;
  actor?: Partial<TaskLedgerActor>;
  ts?: string;
  task?: TaskLedgerTaskPatch;
  idempotencyKey?: string;
};

export type TaskLedgerTaskNoteInput = {
  entity: "task";
  kind: Exclude<TaskEventKind, "created" | "state_changed" | "sync">;
  taskId: string;
  summary: string;
  actor?: Partial<TaskLedgerActor>;
  ts?: string;
  state?: TaskState;
  task?: TaskLedgerTaskPatch;
  idempotencyKey?: string;
};

export type TaskLedgerAgentHeartbeatInput = {
  entity: "agent";
  kind: "heartbeat";
  agent: {
    id: string;
    name?: string;
    status?: AgentActivityStatus;
    lane?: string | null;
    currentTaskId?: string | null;
    sessionKey?: string | null;
    worktree?: string | null;
    branch?: string | null;
    summary?: string;
    metadata?: Record<string, unknown>;
  };
  ts?: string;
  idempotencyKey?: string;
};

export type TaskLedgerRecallTraceInput = {
  entity: "recall";
  kind: "trace";
  sessionKey: string;
  agentId: string;
  ran: boolean;
  skippedReason?: string;
  scope?: Record<string, unknown>;
  candidatesConsidered: number;
  injectedCount: number;
  injectedChars: number;
  withheldCount?: number;
  dependencyStatus: "ok" | "timeout" | "error" | "skipped";
  ts?: string;
  idempotencyKey?: string;
};

export type TaskLedgerPublishInput =
  | TaskLedgerTaskUpsertInput
  | TaskLedgerTaskTransitionInput
  | TaskLedgerTaskNoteInput
  | TaskLedgerAgentHeartbeatInput
  | TaskLedgerRecallTraceInput;

export type TaskLedgerPublishResult = {
  accepted: number;
  events: TaskLedgerRecord[];
  snapshot: TaskLedgerSnapshot;
};

export type ReadTaskLedgerEventsOptions = {
  stateDir?: string;
  limit?: number;
  taskId?: string;
  agentId?: string;
};

function trimToUndefined(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function normalizeTimestamp(value?: string): string {
  const trimmed = trimToUndefined(value);
  if (!trimmed) {
    return new Date().toISOString();
  }
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) {
    return new Date().toISOString();
  }
  return parsed.toISOString();
}

function normalizePersistedTimestamp(value: unknown): string | null {
  const trimmed = trimToUndefined(value);
  if (!trimmed) {
    return null;
  }
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString();
}

function isTaskState(value: unknown): value is TaskState {
  return typeof value === "string" && (TASK_STATES as readonly string[]).includes(value);
}

function isTaskPriority(value: unknown): value is TaskPriority {
  return typeof value === "string" && (TASK_PRIORITIES as readonly string[]).includes(value);
}

function isAgentActivityStatus(value: unknown): value is AgentActivityStatus {
  return (
    typeof value === "string" && (AGENT_ACTIVITY_STATUSES as readonly string[]).includes(value)
  );
}

function normalizeActor(input?: Partial<TaskLedgerActor>): TaskLedgerActor {
  const type =
    input?.type === "agent" || input?.type === "operator" || input?.type === "system"
      ? input.type
      : "system";
  return {
    type,
    ...(trimToUndefined(input?.id) ? { id: trimToUndefined(input?.id) } : {}),
    ...(trimToUndefined(input?.name) ? { name: trimToUndefined(input?.name) } : {}),
  };
}

function normalizeMetadata(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return { ...(value as Record<string, unknown>) };
}

function hasInvalidLedgerIdCharacter(value: string): boolean {
  for (const char of value) {
    if (/\s/u.test(char)) {
      return true;
    }
    const codePoint = char.codePointAt(0) ?? 0;
    if (codePoint <= 0x1f || codePoint === 0x7f) {
      return true;
    }
  }
  return false;
}

function normalizeLedgerId(value: unknown, label: string): string {
  const trimmed = trimToUndefined(value);
  if (!trimmed) {
    throw new Error(`${label} required`);
  }
  if (hasInvalidLedgerIdCharacter(trimmed)) {
    throw new Error(`${label} must not contain whitespace or control characters`);
  }
  return trimmed;
}

function normalizePersistedLedgerId(value: unknown): string | null {
  try {
    return normalizeLedgerId(value, "ledger id");
  } catch {
    return null;
  }
}

function normalizeIdempotencyKey(value: unknown): string | undefined {
  return trimToUndefined(value);
}

function stableValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((entry) => stableValue(entry));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .toSorted(([left], [right]) => left.localeCompare(right))
        .map(([key, entry]) => [key, stableValue(entry)]),
    );
  }
  return value;
}

function stableStringify(value: unknown): string {
  return JSON.stringify(stableValue(value));
}

function resolveTaskLedgerPaths(stateDir = resolveStateDir()) {
  const rootDir = path.join(stateDir, "shared", "task-ledger");
  return {
    rootDir,
    eventsFile: path.join(rootDir, "events.jsonl"),
    snapshotFile: path.join(rootDir, "snapshot.json"),
  };
}

function defaultTaskRecord(params: {
  id: string;
  title: string;
  ts: string;
  task?: Partial<Omit<TaskLedgerTask, "id" | "title" | "lastEventAt">>;
}): TaskLedgerTask {
  return {
    id: params.id,
    title: params.title,
    description: trimToUndefined(params.task?.description),
    state: isTaskState(params.task?.state) ? params.task.state : "backlog",
    priority: isTaskPriority(params.task?.priority) ? params.task.priority : "medium",
    source: trimToUndefined(params.task?.source) ?? DEFAULT_SOURCE,
    externalRef: trimToUndefined(params.task?.externalRef),
    ledgerRef: trimToUndefined(params.task?.ledgerRef),
    busTopic: trimToUndefined(params.task?.busTopic) ?? DEFAULT_BUS_TOPIC,
    assignedAgent: trimToUndefined(params.task?.assignedAgent),
    requestedBy: trimToUndefined(params.task?.requestedBy),
    blockedReason: trimToUndefined(params.task?.blockedReason),
    sessionKey: trimToUndefined(params.task?.sessionKey),
    worktree: trimToUndefined(params.task?.worktree),
    lastEventAt: params.ts,
    metadata: normalizeMetadata(params.task?.metadata),
  };
}

function mergeTaskRecord(
  current: TaskLedgerTask | undefined,
  patch: TaskLedgerTaskPatch,
  ts: string,
  fallbackTitle?: string,
): TaskLedgerTask {
  const title = trimToUndefined(patch.title) ?? current?.title ?? trimToUndefined(fallbackTitle);
  if (!title) {
    throw new Error("task title required for first event");
  }
  const base =
    current ??
    defaultTaskRecord({
      id: "pending",
      title,
      ts,
      task: patch,
    });
  return {
    ...base,
    title,
    description:
      patch.description === undefined
        ? base.description
        : (trimToUndefined(patch.description) ?? undefined),
    state: isTaskState(patch.state) ? patch.state : base.state,
    priority: isTaskPriority(patch.priority) ? patch.priority : base.priority,
    source: trimToUndefined(patch.source) ?? base.source,
    externalRef:
      patch.externalRef === undefined ? base.externalRef : trimToUndefined(patch.externalRef),
    ledgerRef: patch.ledgerRef === undefined ? base.ledgerRef : trimToUndefined(patch.ledgerRef),
    busTopic: trimToUndefined(patch.busTopic) ?? base.busTopic,
    assignedAgent:
      patch.assignedAgent === undefined ? base.assignedAgent : trimToUndefined(patch.assignedAgent),
    requestedBy:
      patch.requestedBy === undefined ? base.requestedBy : trimToUndefined(patch.requestedBy),
    blockedReason:
      patch.blockedReason === undefined ? base.blockedReason : trimToUndefined(patch.blockedReason),
    sessionKey:
      patch.sessionKey === undefined ? base.sessionKey : trimToUndefined(patch.sessionKey),
    worktree: patch.worktree === undefined ? base.worktree : trimToUndefined(patch.worktree),
    metadata: patch.metadata === undefined ? base.metadata : normalizeMetadata(patch.metadata),
    lastEventAt: ts,
  };
}

async function ensureLedgerDir(stateDir = resolveStateDir()) {
  const paths = resolveTaskLedgerPaths(stateDir);
  await fs.mkdir(paths.rootDir, { recursive: true, mode: 0o700 });
  return paths;
}

async function appendLedgerEvents(stateDir: string, events: TaskLedgerRecord[]) {
  if (!events.length) {
    return;
  }
  const { eventsFile } = await ensureLedgerDir(stateDir);
  const payload = `${events.map((event) => JSON.stringify(event)).join("\n")}\n`;
  await fs.appendFile(eventsFile, payload, { encoding: "utf8", mode: 0o600 });
  try {
    await fs.chmod(eventsFile, 0o600);
  } catch {
    // best-effort
  }
}

function sortTasks(tasks: Iterable<TaskLedgerTask>) {
  return [...tasks].toSorted((a, b) => {
    const timeDelta = Date.parse(b.lastEventAt) - Date.parse(a.lastEventAt);
    if (timeDelta !== 0) {
      return timeDelta;
    }
    return a.id.localeCompare(b.id);
  });
}

function sortAgents(agents: Iterable<TaskLedgerAgentActivity>) {
  return [...agents].toSorted((a, b) => {
    const timeDelta = Date.parse(b.heartbeatAt) - Date.parse(a.heartbeatAt);
    if (timeDelta !== 0) {
      return timeDelta;
    }
    return a.id.localeCompare(b.id);
  });
}

function applyRecordToMaps(params: {
  record: TaskLedgerRecord;
  tasks: Map<string, TaskLedgerTask>;
  agents: Map<string, TaskLedgerAgentActivity>;
}) {
  const { record, tasks, agents } = params;
  if (record.entity === "task") {
    const existing = tasks.get(record.taskId);
    const title = trimToUndefined(record.task?.title) ?? existing?.title;
    if (!title) {
      throw new Error(`task title required for ${record.taskId}`);
    }
    let next = mergeTaskRecord(existing, record.task ?? {}, record.ts, title);
    next.id = record.taskId;
    if (record.toState) {
      next.state = record.toState;
    }
    if (record.kind === "blocked" && !trimToUndefined(next.blockedReason)) {
      next.blockedReason = record.summary;
    }
    if (record.kind !== "blocked" && record.toState && record.toState !== "blocked") {
      next.blockedReason = undefined;
    }
    tasks.set(record.taskId, next);
    return;
  }

  if (record.entity === "agent") {
    const existing = agents.get(record.agentId);
    const heartbeatAt = record.ts;
    const name = trimToUndefined(record.name) ?? existing?.name ?? record.agentId;
    agents.set(record.agentId, {
      id: record.agentId,
      name,
      status: record.status,
      lane: record.lane === undefined ? existing?.lane : trimToUndefined(record.lane),
      currentTaskId:
        record.currentTaskId === undefined
          ? existing?.currentTaskId
          : trimToUndefined(record.currentTaskId),
      sessionKey:
        record.sessionKey === undefined ? existing?.sessionKey : trimToUndefined(record.sessionKey),
      worktree:
        record.worktree === undefined ? existing?.worktree : trimToUndefined(record.worktree),
      branch: record.branch === undefined ? existing?.branch : trimToUndefined(record.branch),
      summary: trimToUndefined(record.summary) ?? existing?.summary ?? "Heartbeat",
      heartbeatAt,
      lastSeenAt: heartbeatAt,
      metadata:
        Object.keys(record.metadata).length > 0
          ? normalizeMetadata(record.metadata)
          : (existing?.metadata ?? {}),
    });
  }
}

function getRecordEntityKey(record: TaskLedgerRecord): string {
  if (record.entity === "task") {
    return `task:${record.taskId}`;
  }
  if (record.entity === "agent") {
    return `agent:${record.agentId}`;
  }
  return `recall:${record.sessionKey}:${record.agentId}`;
}

function getSeenIdempotencyKeysForEntity(
  seenIdempotencyKeysByEntity: Map<string, Set<string>>,
  entityKey: string,
): Set<string> {
  let seen = seenIdempotencyKeysByEntity.get(entityKey);
  if (!seen) {
    seen = new Set<string>();
    seenIdempotencyKeysByEntity.set(entityKey, seen);
  }
  return seen;
}

function getRecordDedupSignature(record: TaskLedgerRecord): string {
  if (record.entity === "task") {
    return stableStringify({
      entity: record.entity,
      kind: record.kind,
      taskId: record.taskId,
      summary: record.summary,
      actor: record.actor,
      toState: record.toState,
      task: record.task ?? {},
    });
  }
  if (record.entity === "agent") {
    return stableStringify({
      entity: record.entity,
      kind: record.kind,
      agentId: record.agentId,
      name: record.name,
      status: record.status,
      lane: record.lane,
      currentTaskId: record.currentTaskId,
      sessionKey: record.sessionKey,
      worktree: record.worktree,
      branch: record.branch,
      summary: record.summary,
      metadata: record.metadata,
    });
  }
  return stableStringify({
    entity: record.entity,
    kind: record.kind,
    sessionKey: record.sessionKey,
    agentId: record.agentId,
    ran: record.ran,
    skippedReason: record.skippedReason,
    scope: record.scope ?? {},
    candidatesConsidered: record.candidatesConsidered,
    injectedCount: record.injectedCount,
    injectedChars: record.injectedChars,
    withheldCount: record.withheldCount,
    dependencyStatus: record.dependencyStatus,
    ts: record.ts,
  });
}

function isIdempotentDuplicateRecord(
  record: TaskLedgerRecord,
  previous?: TaskLedgerRecord,
): boolean {
  if (!previous || getRecordEntityKey(previous) !== getRecordEntityKey(record)) {
    return false;
  }
  if (
    record.idempotencyKey &&
    previous.idempotencyKey &&
    record.idempotencyKey === previous.idempotencyKey
  ) {
    return true;
  }
  if (getRecordDedupSignature(record) !== getRecordDedupSignature(previous)) {
    return false;
  }
  const deltaMs = Math.abs(Date.parse(record.ts) - Date.parse(previous.ts));
  return Number.isFinite(deltaMs) && deltaMs <= DEFAULT_IDEMPOTENCY_WINDOW_MS;
}

function hasSeenEntityIdempotencyKey(
  record: TaskLedgerRecord,
  seenIdempotencyKeysByEntity: Map<string, Set<string>>,
): boolean {
  const idempotencyKey = normalizeIdempotencyKey(record.idempotencyKey);
  if (!idempotencyKey) {
    return false;
  }
  return getSeenIdempotencyKeysForEntity(
    seenIdempotencyKeysByEntity,
    getRecordEntityKey(record),
  ).has(idempotencyKey);
}

function rememberAcceptedRecord(
  record: TaskLedgerRecord,
  materialized: Pick<MaterializedLedgerState, "lastRecordByEntity" | "seenIdempotencyKeysByEntity">,
) {
  const entityKey = getRecordEntityKey(record);
  materialized.lastRecordByEntity.set(entityKey, record);
  const idempotencyKey = normalizeIdempotencyKey(record.idempotencyKey);
  if (!idempotencyKey) {
    return;
  }
  getSeenIdempotencyKeysForEntity(materialized.seenIdempotencyKeysByEntity, entityKey).add(
    idempotencyKey,
  );
}

// Idempotency keys are scoped to the logical entity (task or agent id). First accepted record wins:
// once a record with a given key has been accepted for that entity, later replays with the same key
// are ignored during both publish-time appends and log re-materialization, even if newer records
// intervened.
function shouldSkipDuplicateRecord(
  record: TaskLedgerRecord,
  materialized: Pick<MaterializedLedgerState, "lastRecordByEntity" | "seenIdempotencyKeysByEntity">,
): boolean {
  if (hasSeenEntityIdempotencyKey(record, materialized.seenIdempotencyKeysByEntity)) {
    return true;
  }
  return isIdempotentDuplicateRecord(
    record,
    materialized.lastRecordByEntity.get(getRecordEntityKey(record)),
  );
}

function materializeLedgerState(records: TaskLedgerRecord[]): MaterializedLedgerState {
  const tasks = new Map<string, TaskLedgerTask>();
  const agents = new Map<string, TaskLedgerAgentActivity>();
  const appliedRecords: TaskLedgerRecord[] = [];
  const lastRecordByEntity = new Map<string, TaskLedgerRecord>();
  const seenIdempotencyKeysByEntity = new Map<string, Set<string>>();

  for (const record of records) {
    try {
      if (shouldSkipDuplicateRecord(record, { lastRecordByEntity, seenIdempotencyKeysByEntity })) {
        continue;
      }
      applyRecordToMaps({ record, tasks, agents });
      appliedRecords.push(record);
      rememberAcceptedRecord(record, { lastRecordByEntity, seenIdempotencyKeysByEntity });
    } catch {
      // Ignore malformed persisted records so the append-only log stays authoritative
      // and snapshot rebuilds can recover from bad out-of-band lines.
    }
  }

  return { tasks, agents, appliedRecords, lastRecordByEntity, seenIdempotencyKeysByEntity };
}

function createSnapshotFromRecords(params: {
  stateDir: string;
  records: TaskLedgerRecord[];
  recentEventLimit?: number;
}): TaskLedgerSnapshot {
  const materialized = materializeLedgerState(params.records);
  const recentLimit =
    typeof params.recentEventLimit === "number" && params.recentEventLimit > 0
      ? Math.floor(params.recentEventLimit)
      : DEFAULT_RECENT_EVENT_LIMIT;
  const recentEvents = materialized.appliedRecords.slice(-recentLimit);
  return {
    schema: TASK_LEDGER_SNAPSHOT_SCHEMA,
    generatedAt: new Date().toISOString(),
    lastEventId: materialized.appliedRecords[materialized.appliedRecords.length - 1]?.id,
    paths: resolveTaskLedgerPaths(params.stateDir),
    tasks: sortTasks(materialized.tasks.values()),
    agents: sortAgents(materialized.agents.values()),
    recentEvents,
  };
}

function isReconcileTaskNote(record: TaskLedgerRecord): record is TaskLedgerTaskRecord {
  return (
    record.entity === "task" &&
    record.kind === "note" &&
    record.actor.type === "system" &&
    record.actor.id === RECONCILE_ACTOR_ID
  );
}

function buildReconcileIdempotencyKey(kind: string, parts: Array<string | undefined>): string {
  return `${RECONCILE_IDEMPOTENCY_PREFIX}:${kind}:${parts
    .map((part) => trimToUndefined(part) ?? "none")
    .join(":")}`;
}

function createReconcileTaskNote(params: {
  taskId: string;
  ts: string;
  summary: string;
  idempotencyKey: string;
}): TaskLedgerTaskRecord {
  return {
    schema: TASK_LEDGER_SCHEMA,
    id: randomUUID(),
    ts: params.ts,
    entity: "task",
    kind: "note",
    taskId: params.taskId,
    summary: params.summary,
    actor: {
      type: "system",
      id: RECONCILE_ACTOR_ID,
      name: RECONCILE_ACTOR_NAME,
    },
    idempotencyKey: params.idempotencyKey,
  };
}

function buildReconciliationRecords(materialized: MaterializedLedgerState): TaskLedgerTaskRecord[] {
  if (materialized.appliedRecords.length === 0) {
    return [];
  }

  const lastSubstantiveTaskRecordById = new Map<string, TaskLedgerTaskRecord>();
  const lastAgentRecordById = new Map<string, TaskLedgerAgentRecord>();
  const emittedIdempotencyKeys = new Set<string>();

  for (const record of materialized.appliedRecords) {
    if (record.entity === "task") {
      if (!isReconcileTaskNote(record)) {
        lastSubstantiveTaskRecordById.set(record.taskId, record);
      }
      continue;
    }
    if (record.entity === "agent") {
      lastAgentRecordById.set(record.agentId, record);
    }
  }

  const notes: TaskLedgerTaskRecord[] = [];
  const activeWorkByAgentId = new Map<
    string,
    { taskId: string; tsMs: number; referenceId: string; source: "task" | "heartbeat" }
  >();

  const queueTaskNote = (taskId: string, summary: string, idempotencyKey: string, ts?: string) => {
    if (!materialized.tasks.has(taskId) || emittedIdempotencyKeys.has(idempotencyKey)) {
      return;
    }
    emittedIdempotencyKeys.add(idempotencyKey);
    notes.push(
      createReconcileTaskNote({
        taskId,
        ts: ts ?? materialized.appliedRecords.at(-1)?.ts ?? new Date().toISOString(),
        summary,
        idempotencyKey,
      }),
    );
  };

  const considerActiveWork = (
    agentId: string,
    candidate: {
      taskId: string;
      tsMs: number;
      referenceId: string;
      source: "task" | "heartbeat";
    },
  ) => {
    if (!Number.isFinite(candidate.tsMs)) {
      return;
    }
    const current = activeWorkByAgentId.get(agentId);
    if (!current || candidate.tsMs > current.tsMs) {
      activeWorkByAgentId.set(agentId, candidate);
    }
  };

  for (const task of materialized.tasks.values()) {
    const assignedAgent = trimToUndefined(task.assignedAgent);
    const taskRecord = lastSubstantiveTaskRecordById.get(task.id);
    const taskRecordId = taskRecord?.id;
    const taskTsMs = taskRecord ? Date.parse(taskRecord.ts) : Date.parse(task.lastEventAt);

    if (task.state === "in_progress" && assignedAgent) {
      considerActiveWork(assignedAgent, {
        taskId: task.id,
        tsMs: taskTsMs,
        referenceId: taskRecordId ?? task.id,
        source: "task",
      });

      const agent = materialized.agents.get(assignedAgent);
      const agentRecord = lastAgentRecordById.get(assignedAgent);

      if (!agent || !agentRecord) {
        queueTaskNote(
          task.id,
          `Reconcile residue: task is still marked in progress for assigned agent ${assignedAgent}, but no agent heartbeat is recorded. This usually means stale residue from earlier work; verify whether the task should remain in progress or be reassigned.`,
          buildReconcileIdempotencyKey("in-progress-agent-missing", [
            task.id,
            assignedAgent,
            task.state,
          ]),
        );
        continue;
      }

      if (agent.status === "idle") {
        queueTaskNote(
          task.id,
          `Reconcile residue: task is still marked in progress for assigned agent ${assignedAgent}, but the latest heartbeat reports the agent idle. This usually means the task state or agent context was not cleaned up; verify whether the task should pause or the agent context should be updated.`,
          buildReconcileIdempotencyKey("in-progress-agent-idle", [
            task.id,
            assignedAgent,
            agent.status,
          ]),
        );
        continue;
      }

      const taskReferenceTsMs = taskRecord ? Date.parse(taskRecord.ts) : Date.parse(task.lastEventAt);
      const staleDeltaMs = taskReferenceTsMs - Date.parse(agent.lastSeenAt);
      if (Number.isFinite(staleDeltaMs) && staleDeltaMs >= RECONCILE_AGENT_STALE_MS) {
        queueTaskNote(
          task.id,
          `Reconcile residue: task is still marked in progress for assigned agent ${assignedAgent}, but the latest heartbeat is stale (${agent.lastSeenAt}). This looks like stale residue unless work is still active; verify whether the task should remain in progress or refresh the agent task context.`,
          buildReconcileIdempotencyKey("in-progress-agent-stale", [
            task.id,
            assignedAgent,
            "stale",
          ]),
          Number.isFinite(taskReferenceTsMs) ? new Date(taskReferenceTsMs).toISOString() : undefined,
        );
      }
    }
  }

  for (const agent of materialized.agents.values()) {
    const currentTaskId = trimToUndefined(agent.currentTaskId);
    if (!currentTaskId) {
      continue;
    }
    const task = materialized.tasks.get(currentTaskId);
    const agentRecord = lastAgentRecordById.get(agent.id);
    if (!task || !agentRecord) {
      continue;
    }

    considerActiveWork(agent.id, {
      taskId: currentTaskId,
      tsMs: Date.parse(agentRecord.ts),
      referenceId: agentRecord.id,
      source: "heartbeat",
    });

    const assignedAgent = trimToUndefined(task.assignedAgent);

    if (agent.status !== "idle" && (task.state === "backlog" || task.state === "todo")) {
      queueTaskNote(
        task.id,
        `Reconcile mismatch: agent ${agent.id} reports active task context for this task (${agent.status}), but the task is still ${task.state}. Move the task to in progress or clear the stale agent context.`,
        buildReconcileIdempotencyKey("active-context-task-not-started", [
          task.id,
          task.state,
          agent.id,
          agent.status,
        ]),
      );
    }

    if (assignedAgent !== agent.id) {
      queueTaskNote(
        task.id,
        assignedAgent
          ? `Reconcile mismatch: agent ${agent.id} heartbeat claims this task as current work, but the task is assigned to ${assignedAgent}. Fix task ownership or clear the stale heartbeat context.`
          : `Reconcile mismatch: agent ${agent.id} heartbeat claims this task as current work, but the task is currently unassigned. Fix task ownership or clear the stale heartbeat context.`,
        buildReconcileIdempotencyKey("heartbeat-task-assignment-mismatch", [
          task.id,
          agent.id,
          assignedAgent ?? "unassigned",
          task.state,
        ]),
      );
    }
  }

  for (const task of materialized.tasks.values()) {
    const assignedAgent = trimToUndefined(task.assignedAgent);
    if (task.state !== "blocked" || !assignedAgent) {
      continue;
    }
    const blockedRecord = lastSubstantiveTaskRecordById.get(task.id);
    const blockedTsMs = blockedRecord ? Date.parse(blockedRecord.ts) : Date.parse(task.lastEventAt);
    const activeWork = activeWorkByAgentId.get(assignedAgent);
    if (!activeWork || activeWork.taskId === task.id || !Number.isFinite(blockedTsMs)) {
      continue;
    }
    if (activeWork.tsMs <= blockedTsMs) {
      continue;
    }
    queueTaskNote(
      task.id,
      `Reconcile residue: blocked task still belongs to ${assignedAgent}, but newer active work exists on ${activeWork.taskId}. Review whether this task is still blocked, should be reassigned, or can be reopened.`,
      buildReconcileIdempotencyKey("blocked-task-superseded", [
        task.id,
        assignedAgent,
        activeWork.taskId,
      ]),
    );
  }

  return notes;
}

function withRecentEventLimit(
  snapshot: TaskLedgerSnapshot,
  recentEventLimit?: number,
): TaskLedgerSnapshot {
  if (typeof recentEventLimit !== "number" || recentEventLimit <= 0) {
    return snapshot;
  }
  return {
    ...snapshot,
    recentEvents: snapshot.recentEvents.slice(-Math.floor(recentEventLimit)),
  };
}

function shouldRewriteSnapshot(
  cached: TaskLedgerSnapshot | null,
  canonical: TaskLedgerSnapshot,
): boolean {
  if (cached?.schema !== TASK_LEDGER_SNAPSHOT_SCHEMA) {
    return true;
  }
  if (cached.lastEventId !== canonical.lastEventId) {
    return true;
  }
  if (!canonical.lastEventId) {
    return cached.tasks.length > 0 || cached.agents.length > 0 || cached.recentEvents.length > 0;
  }

  // Repair on-disk drift even when the latest event id matches the canonical log.
  return (
    stableStringify({
      tasks: cached.tasks,
      agents: cached.agents,
      recentEvents: cached.recentEvents,
    }) !==
    stableStringify({
      tasks: canonical.tasks,
      agents: canonical.agents,
      recentEvents: canonical.recentEvents,
    })
  );
}

async function writeSnapshotFile(snapshot: TaskLedgerSnapshot) {
  await writeJsonAtomic(snapshot.paths.snapshotFile, snapshot, {
    mode: 0o600,
    trailingNewline: true,
  });
}

function normalizeTaskPatch(patch: TaskLedgerTaskPatch | undefined): TaskLedgerTaskPatch {
  if (!patch || typeof patch !== "object" || Array.isArray(patch)) {
    return {};
  }
  return {
    ...(trimToUndefined(patch.title) ? { title: trimToUndefined(patch.title) } : {}),
    ...(patch.description !== undefined ? { description: patch.description } : {}),
    ...(isTaskState(patch.state) ? { state: patch.state } : {}),
    ...(isTaskPriority(patch.priority) ? { priority: patch.priority } : {}),
    ...(patch.source !== undefined ? { source: patch.source } : {}),
    ...(patch.externalRef !== undefined ? { externalRef: patch.externalRef } : {}),
    ...(patch.ledgerRef !== undefined ? { ledgerRef: patch.ledgerRef } : {}),
    ...(patch.busTopic !== undefined ? { busTopic: patch.busTopic } : {}),
    ...(patch.assignedAgent !== undefined ? { assignedAgent: patch.assignedAgent } : {}),
    ...(patch.requestedBy !== undefined ? { requestedBy: patch.requestedBy } : {}),
    ...(patch.blockedReason !== undefined ? { blockedReason: patch.blockedReason } : {}),
    ...(patch.sessionKey !== undefined ? { sessionKey: patch.sessionKey } : {}),
    ...(patch.worktree !== undefined ? { worktree: patch.worktree } : {}),
    ...(patch.metadata !== undefined ? { metadata: normalizeMetadata(patch.metadata) } : {}),
  };
}

function parsePersistedTaskRecord(value: Record<string, unknown>): TaskLedgerTaskRecord | null {
  const id = normalizePersistedLedgerId(value.id);
  const ts = normalizePersistedTimestamp(value.ts);
  const taskId = normalizePersistedLedgerId(value.taskId);
  if (!id || !ts || !taskId) {
    return null;
  }
  const kind = value.kind;
  if (
    kind !== "created" &&
    kind !== "started" &&
    kind !== "state_changed" &&
    kind !== "qa" &&
    kind !== "blocked" &&
    kind !== "note" &&
    kind !== "sync"
  ) {
    return null;
  }
  const task = normalizeTaskPatch(
    value.task && typeof value.task === "object" && !Array.isArray(value.task)
      ? (value.task as TaskLedgerTaskPatch)
      : undefined,
  );
  return {
    schema: TASK_LEDGER_SCHEMA,
    id,
    ts,
    entity: "task",
    kind,
    taskId,
    summary: trimToUndefined(value.summary) ?? `${kind} for ${taskId}`,
    actor: normalizeActor(
      value.actor && typeof value.actor === "object" && !Array.isArray(value.actor)
        ? (value.actor as Partial<TaskLedgerActor>)
        : undefined,
    ),
    ...(isTaskState(value.fromState) ? { fromState: value.fromState } : {}),
    ...(isTaskState(value.toState) ? { toState: value.toState } : {}),
    ...(Object.keys(task).length > 0 ? { task } : {}),
    ...(normalizeIdempotencyKey(value.idempotencyKey)
      ? { idempotencyKey: normalizeIdempotencyKey(value.idempotencyKey) }
      : {}),
  };
}

function parsePersistedAgentRecord(value: Record<string, unknown>): TaskLedgerAgentRecord | null {
  const id = normalizePersistedLedgerId(value.id);
  const ts = normalizePersistedTimestamp(value.ts);
  const agentId = normalizePersistedLedgerId(value.agentId);
  if (!id || !ts || !agentId || value.kind !== "heartbeat") {
    return null;
  }
  const status = isAgentActivityStatus(value.status) ? value.status : null;
  if (!status) {
    return null;
  }
  return {
    schema: TASK_LEDGER_SCHEMA,
    id,
    ts,
    entity: "agent",
    kind: "heartbeat",
    agentId,
    ...(trimToUndefined(value.name) ? { name: trimToUndefined(value.name) } : {}),
    status,
    ...(Object.hasOwn(value, "lane") ? { lane: normalizeHeartbeatField(value.lane) } : {}),
    ...(Object.hasOwn(value, "currentTaskId")
      ? { currentTaskId: normalizeHeartbeatField(value.currentTaskId) }
      : {}),
    ...(Object.hasOwn(value, "sessionKey")
      ? { sessionKey: normalizeHeartbeatField(value.sessionKey) }
      : {}),
    ...(Object.hasOwn(value, "worktree")
      ? { worktree: normalizeHeartbeatField(value.worktree) }
      : {}),
    ...(Object.hasOwn(value, "branch") ? { branch: normalizeHeartbeatField(value.branch) } : {}),
    summary: trimToUndefined(value.summary) ?? "Heartbeat",
    metadata: normalizeMetadata(value.metadata),
    ...(normalizeIdempotencyKey(value.idempotencyKey)
      ? { idempotencyKey: normalizeIdempotencyKey(value.idempotencyKey) }
      : {}),
  };
}

function parsePersistedRecallRecord(value: Record<string, unknown>): TaskLedgerRecallRecord | null {
  const id = normalizePersistedLedgerId(value.id);
  const ts = normalizePersistedTimestamp(value.ts);
  const sessionKey = normalizePersistedLedgerId(value.sessionKey);
  const agentId = normalizePersistedLedgerId(value.agentId);
  if (!id || !ts || !sessionKey || !agentId || value.kind !== "trace") {
    return null;
  }
  if (typeof value.ran !== "boolean") {
    return null;
  }
  const dependencyStatus =
    value.dependencyStatus === "ok" ||
    value.dependencyStatus === "timeout" ||
    value.dependencyStatus === "error" ||
    value.dependencyStatus === "skipped"
      ? value.dependencyStatus
      : null;
  if (!dependencyStatus) {
    return null;
  }

  const scope = normalizeRecallScope(value.scope);

  return {
    schema: TASK_LEDGER_SCHEMA,
    id,
    ts,
    entity: "recall",
    kind: "trace",
    sessionKey,
    agentId,
    ran: value.ran,
    ...(trimToUndefined(value.skippedReason)
      ? { skippedReason: trimToUndefined(value.skippedReason) }
      : {}),
    ...(scope ? { scope } : {}),
    candidatesConsidered: normalizeNonNegativeInteger(value.candidatesConsidered),
    injectedCount: normalizeNonNegativeInteger(value.injectedCount),
    injectedChars: normalizeNonNegativeInteger(value.injectedChars),
    ...(typeof value.withheldCount === "number"
      ? { withheldCount: normalizeNonNegativeInteger(value.withheldCount) }
      : {}),
    dependencyStatus,
    ...(normalizeIdempotencyKey(value.idempotencyKey)
      ? { idempotencyKey: normalizeIdempotencyKey(value.idempotencyKey) }
      : {}),
  };
}

function parsePersistedTaskLedgerRecord(value: unknown): TaskLedgerRecord | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  const record = value as Record<string, unknown>;
  if (record.schema !== TASK_LEDGER_SCHEMA) {
    return null;
  }
  if (record.entity === "task") {
    return parsePersistedTaskRecord(record);
  }
  if (record.entity === "agent") {
    return parsePersistedAgentRecord(record);
  }
  if (record.entity === "recall") {
    return parsePersistedRecallRecord(record);
  }
  return null;
}

async function readAllTaskLedgerRecords(stateDir: string): Promise<TaskLedgerRecord[]> {
  const { eventsFile } = resolveTaskLedgerPaths(stateDir);
  let raw = "";
  try {
    raw = await fs.readFile(eventsFile, "utf8");
  } catch {
    return [];
  }
  return raw
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean)
    .flatMap((line) => {
      try {
        const parsed = JSON.parse(line) as unknown;
        const record = parsePersistedTaskLedgerRecord(parsed);
        return record ? [record] : [];
      } catch {
        return [];
      }
    });
}

export async function readTaskLedgerEvents(
  options: ReadTaskLedgerEventsOptions = {},
): Promise<TaskLedgerRecord[]> {
  const stateDir = options.stateDir ?? resolveStateDir();
  const records = (await readAllTaskLedgerRecords(stateDir)).filter((record) => {
    if (options.taskId && record.entity === "task" && record.taskId !== options.taskId) {
      return false;
    }
    if (options.taskId && record.entity !== "task") {
      return false;
    }
    if (
      options.agentId &&
      (record.entity === "agent" || record.entity === "recall") &&
      record.agentId !== options.agentId
    ) {
      return false;
    }
    if (options.agentId && record.entity !== "agent" && record.entity !== "recall") {
      return false;
    }
    return true;
  });
  const limit =
    typeof options.limit === "number" && options.limit > 0 ? Math.floor(options.limit) : undefined;
  return limit ? records.slice(-limit) : records;
}

export async function readTaskLedgerSnapshot(options?: {
  stateDir?: string;
  recentEventLimit?: number;
}): Promise<TaskLedgerSnapshot> {
  const stateDir = options?.stateDir ?? resolveStateDir();
  const paths = resolveTaskLedgerPaths(stateDir);
  const records = await readAllTaskLedgerRecords(stateDir);
  const canonical = createSnapshotFromRecords({
    stateDir,
    records,
    recentEventLimit: DEFAULT_RECENT_EVENT_LIMIT,
  });
  const cached = await readJsonFile<TaskLedgerSnapshot>(paths.snapshotFile);
  if (shouldRewriteSnapshot(cached, canonical)) {
    await writeSnapshotFile(canonical);
  }
  return withRecentEventLimit(canonical, options?.recentEventLimit);
}

function normalizeTaskUpsertRecord(
  input: TaskLedgerTaskUpsertInput,
  existing: TaskLedgerTask | undefined,
): TaskLedgerTaskRecord {
  const ts = normalizeTimestamp(input.ts);
  const taskId = normalizeLedgerId(input.task.id, "task id");
  const patch = normalizeTaskPatch(input.task);
  const title = trimToUndefined(input.task.title) ?? existing?.title;
  if (!title) {
    throw new Error(`task title required for ${taskId}`);
  }
  const next = mergeTaskRecord(existing, patch, ts, title);
  next.id = taskId;
  const created = !existing;
  return {
    schema: TASK_LEDGER_SCHEMA,
    id: randomUUID(),
    ts,
    entity: "task",
    kind: created ? "created" : "sync",
    taskId,
    summary:
      trimToUndefined(input.summary) ??
      (created ? `Created task ${next.title}` : `Synced task ${next.title}`),
    actor: normalizeActor(input.actor),
    ...(created ? { toState: next.state } : {}),
    task: {
      ...patch,
      title: next.title,
      state: next.state,
      priority: next.priority,
      source: next.source,
      busTopic: next.busTopic,
      ...(next.description ? { description: next.description } : {}),
      ...(next.externalRef ? { externalRef: next.externalRef } : {}),
      ...(next.ledgerRef ? { ledgerRef: next.ledgerRef } : {}),
      ...(next.assignedAgent ? { assignedAgent: next.assignedAgent } : {}),
      ...(next.requestedBy ? { requestedBy: next.requestedBy } : {}),
      ...(next.blockedReason ? { blockedReason: next.blockedReason } : {}),
      ...(next.sessionKey ? { sessionKey: next.sessionKey } : {}),
      ...(next.worktree ? { worktree: next.worktree } : {}),
      metadata: next.metadata,
    },
    ...(normalizeIdempotencyKey(input.idempotencyKey)
      ? { idempotencyKey: normalizeIdempotencyKey(input.idempotencyKey) }
      : {}),
  };
}

function deriveTransitionEventKind(state: TaskState): TaskEventKind {
  if (state === "blocked") {
    return "blocked";
  }
  if (state === "qa") {
    return "qa";
  }
  return "state_changed";
}

function normalizeTaskTransitionRecord(
  input: TaskLedgerTaskTransitionInput,
  existing: TaskLedgerTask | undefined,
): TaskLedgerTaskRecord {
  const ts = normalizeTimestamp(input.ts);
  const taskId = normalizeLedgerId(input.taskId, "task id");
  const patch = normalizeTaskPatch(input.task);
  const title = trimToUndefined(input.task?.title) ?? existing?.title;
  if (!existing && !title) {
    throw new Error(`task ${taskId} not found and no title provided`);
  }
  const next = mergeTaskRecord(existing, { ...patch, state: input.state }, ts, title);
  next.id = taskId;
  return {
    schema: TASK_LEDGER_SCHEMA,
    id: randomUUID(),
    ts,
    entity: "task",
    kind: deriveTransitionEventKind(input.state),
    taskId,
    summary:
      trimToUndefined(input.summary) ??
      `Moved ${next.title} to ${input.state.replaceAll("_", " ")}`,
    actor: normalizeActor(input.actor),
    ...(existing?.state ? { fromState: existing.state } : {}),
    toState: input.state,
    task: {
      ...patch,
      ...(input.state === "blocked" && !trimToUndefined(patch.blockedReason)
        ? { blockedReason: trimToUndefined(input.summary) }
        : {}),
    },
    ...(normalizeIdempotencyKey(input.idempotencyKey)
      ? { idempotencyKey: normalizeIdempotencyKey(input.idempotencyKey) }
      : {}),
  };
}

function normalizeTaskNoteRecord(
  input: TaskLedgerTaskNoteInput,
  existing: TaskLedgerTask | undefined,
): TaskLedgerTaskRecord {
  const ts = normalizeTimestamp(input.ts);
  const taskId = normalizeLedgerId(input.taskId, "task id");
  const patch = normalizeTaskPatch(input.task);
  const title = trimToUndefined(input.task?.title) ?? existing?.title;
  if (!existing && !title) {
    throw new Error(`task ${taskId} not found and no title provided`);
  }
  const toState = isTaskState(input.state) ? input.state : undefined;
  return {
    schema: TASK_LEDGER_SCHEMA,
    id: randomUUID(),
    ts,
    entity: "task",
    kind: input.kind,
    taskId,
    summary: trimToUndefined(input.summary) ?? `${input.kind} for ${title}`,
    actor: normalizeActor(input.actor),
    ...(existing?.state && toState ? { fromState: existing.state } : {}),
    ...(toState ? { toState } : {}),
    task: {
      ...patch,
      ...(toState ? { state: toState } : {}),
      ...(input.kind === "blocked" && !trimToUndefined(patch.blockedReason)
        ? { blockedReason: trimToUndefined(input.summary) }
        : {}),
    },
    ...(normalizeIdempotencyKey(input.idempotencyKey)
      ? { idempotencyKey: normalizeIdempotencyKey(input.idempotencyKey) }
      : {}),
  };
}

function normalizeHeartbeatField(value: unknown): string | null {
  return trimToUndefined(value) ?? null;
}

function normalizeNonNegativeInteger(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.floor(value));
}

function normalizeDependencyStatus(
  value: unknown,
): "ok" | "timeout" | "error" | "skipped" {
  if (value === undefined) {
    return "ok";
  }

  if (
    value === "ok" ||
    value === "timeout" ||
    value === "error" ||
    value === "skipped"
  ) {
    return value;
  }

  throw new TaskLedgerPublishInputError("invalid recall dependencyStatus");
}

function normalizeRecallScope(value: unknown): Record<string, string> | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }

  const raw = normalizeMetadata(value);
  const scope: Record<string, string> = {};
  const senderId = trimToUndefined(raw.senderId);
  if (senderId) {
    scope.senderId = senderId;
  }

  const channelClass = trimToUndefined(raw.channelClass);
  if (channelClass) {
    scope.channelClass = channelClass;
  }

  const threadId = trimToUndefined(raw.threadId);
  if (threadId) {
    scope.threadId = threadId;
  }

  const resourceId = trimToUndefined(raw.resourceId);
  if (resourceId) {
    scope.resourceId = resourceId;
  }

  return Object.keys(scope).length > 0 ? scope : undefined;
}

function normalizeRecallRecord(input: TaskLedgerRecallTraceInput): TaskLedgerRecallRecord {
  const ts = normalizeTimestamp(input.ts);
  const scope = normalizeRecallScope(input.scope);
  return {
    schema: TASK_LEDGER_SCHEMA,
    id: randomUUID(),
    ts,
    entity: "recall",
    kind: "trace",
    sessionKey: normalizeLedgerId(input.sessionKey, "recall session key"),
    agentId: normalizeLedgerId(input.agentId, "recall agent id"),
    ran: input.ran === true,
    ...(trimToUndefined(input.skippedReason)
      ? { skippedReason: trimToUndefined(input.skippedReason) }
      : {}),
    ...(scope ? { scope } : {}),
    candidatesConsidered: normalizeNonNegativeInteger(input.candidatesConsidered),
    injectedCount: normalizeNonNegativeInteger(input.injectedCount),
    injectedChars: normalizeNonNegativeInteger(input.injectedChars),
    ...(typeof input.withheldCount === "number"
      ? { withheldCount: normalizeNonNegativeInteger(input.withheldCount) }
      : {}),
    dependencyStatus: normalizeDependencyStatus(input.dependencyStatus),
    ...(normalizeIdempotencyKey(input.idempotencyKey)
      ? { idempotencyKey: normalizeIdempotencyKey(input.idempotencyKey) }
      : {}),
  };
}

function normalizeAgentRecord(input: TaskLedgerAgentHeartbeatInput): TaskLedgerAgentRecord {
  const ts = normalizeTimestamp(input.ts);
  const agentId = normalizeLedgerId(input.agent.id, "agent id");
  return {
    schema: TASK_LEDGER_SCHEMA,
    id: randomUUID(),
    ts,
    entity: "agent",
    kind: "heartbeat",
    agentId,
    ...(trimToUndefined(input.agent.name) ? { name: trimToUndefined(input.agent.name) } : {}),
    status: isAgentActivityStatus(input.agent.status) ? input.agent.status : "idle",
    lane: normalizeHeartbeatField(input.agent.lane),
    currentTaskId: normalizeHeartbeatField(input.agent.currentTaskId),
    sessionKey: normalizeHeartbeatField(input.agent.sessionKey),
    worktree: normalizeHeartbeatField(input.agent.worktree),
    branch: normalizeHeartbeatField(input.agent.branch),
    summary: trimToUndefined(input.agent.summary) ?? "Heartbeat",
    metadata: normalizeMetadata(input.agent.metadata),
    ...(normalizeIdempotencyKey(input.idempotencyKey)
      ? { idempotencyKey: normalizeIdempotencyKey(input.idempotencyKey) }
      : {}),
  };
}

function normalizePublishRecord(
  input: TaskLedgerPublishInput,
  tasks: Map<string, TaskLedgerTask>,
): TaskLedgerRecord {
  if (input.entity === "task" && input.kind === "upsert") {
    const taskId = normalizeLedgerId(input.task.id, "task id");
    return normalizeTaskUpsertRecord(input, tasks.get(taskId));
  }
  if (input.entity === "task" && input.kind === "transition") {
    const taskId = normalizeLedgerId(input.taskId, "task id");
    return normalizeTaskTransitionRecord(input, tasks.get(taskId));
  }
  if (input.entity === "task") {
    const taskId = normalizeLedgerId(input.taskId, "task id");
    return normalizeTaskNoteRecord(input, tasks.get(taskId));
  }
  if (input.entity === "recall") {
    return normalizeRecallRecord(input);
  }
  const agentId = normalizeLedgerId(input.agent.id, "agent id");
  return normalizeAgentRecord({
    ...input,
    agent: {
      ...input.agent,
      id: agentId,
    },
  });
}

export async function publishTaskLedgerEvents(params: {
  events: TaskLedgerPublishInput[];
  stateDir?: string;
  recentEventLimit?: number;
}): Promise<TaskLedgerPublishResult> {
  const stateDir = params.stateDir ?? resolveStateDir();
  if (!Array.isArray(params.events) || params.events.length === 0) {
    return {
      accepted: 0,
      events: [],
      snapshot: await readTaskLedgerSnapshot({
        stateDir,
        recentEventLimit: params.recentEventLimit,
      }),
    };
  }

  return await taskLedgerLock(async () => {
    const existingRecords = await readAllTaskLedgerRecords(stateDir);
    const materialized = materializeLedgerState(existingRecords);
    const accepted: TaskLedgerRecord[] = [];

    for (const event of params.events) {
      let record: TaskLedgerRecord;
      try {
        record = normalizePublishRecord(event, materialized.tasks);
      } catch (error) {
        if (error instanceof Error) {
          throw new TaskLedgerPublishInputError(error.message);
        }
        throw error;
      }
      if (shouldSkipDuplicateRecord(record, materialized)) {
        continue;
      }
      applyRecordToMaps({
        record,
        tasks: materialized.tasks,
        agents: materialized.agents,
      });
      materialized.appliedRecords.push(record);
      rememberAcceptedRecord(record, materialized);
      accepted.push(record);
    }

    const reconcileRecords = buildReconciliationRecords(materialized);
    for (const record of reconcileRecords) {
      if (shouldSkipDuplicateRecord(record, materialized)) {
        continue;
      }
      applyRecordToMaps({
        record,
        tasks: materialized.tasks,
        agents: materialized.agents,
      });
      materialized.appliedRecords.push(record);
      rememberAcceptedRecord(record, materialized);
      accepted.push(record);
    }

    if (accepted.length > 0) {
      await appendLedgerEvents(stateDir, accepted);
    }

    const canonicalRecords =
      accepted.length > 0 ? await readAllTaskLedgerRecords(stateDir) : existingRecords;
    const canonicalSnapshot = createSnapshotFromRecords({
      stateDir,
      records: canonicalRecords,
      recentEventLimit: DEFAULT_RECENT_EVENT_LIMIT,
    });
    await writeSnapshotFile(canonicalSnapshot);

    return {
      accepted: accepted.length,
      events: accepted,
      snapshot: withRecentEventLimit(canonicalSnapshot, params.recentEventLimit),
    };
  });
}

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
  lane?: string;
  currentTaskId?: string;
  sessionKey?: string;
  summary: string;
  metadata: Record<string, unknown>;
  idempotencyKey?: string;
};

export type TaskLedgerRecord = TaskLedgerTaskRecord | TaskLedgerAgentRecord;

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
    lane?: string;
    currentTaskId?: string;
    sessionKey?: string;
    summary?: string;
    metadata?: Record<string, unknown>;
  };
  ts?: string;
  idempotencyKey?: string;
};

export type TaskLedgerPublishInput =
  | TaskLedgerTaskUpsertInput
  | TaskLedgerTaskTransitionInput
  | TaskLedgerTaskNoteInput
  | TaskLedgerAgentHeartbeatInput;

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

  const existing = agents.get(record.agentId);
  const heartbeatAt = record.ts;
  const name = trimToUndefined(record.name) ?? existing?.name ?? record.agentId;
  agents.set(record.agentId, {
    id: record.agentId,
    name,
    status: record.status,
    lane: trimToUndefined(record.lane) ?? existing?.lane,
    currentTaskId:
      record.currentTaskId === undefined
        ? existing?.currentTaskId
        : trimToUndefined(record.currentTaskId),
    sessionKey:
      record.sessionKey === undefined ? existing?.sessionKey : trimToUndefined(record.sessionKey),
    summary: trimToUndefined(record.summary) ?? existing?.summary ?? "Heartbeat",
    heartbeatAt,
    lastSeenAt: heartbeatAt,
    metadata:
      Object.keys(record.metadata).length > 0
        ? normalizeMetadata(record.metadata)
        : (existing?.metadata ?? {}),
  });
}

function getRecordEntityKey(record: TaskLedgerRecord): string {
  return record.entity === "task" ? `task:${record.taskId}` : `agent:${record.agentId}`;
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
  return stableStringify({
    entity: record.entity,
    kind: record.kind,
    agentId: record.agentId,
    name: record.name,
    status: record.status,
    lane: record.lane,
    currentTaskId: record.currentTaskId,
    sessionKey: record.sessionKey,
    summary: record.summary,
    metadata: record.metadata,
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

  // When the latest event id matches, still repair on-disk drift/corruption in the projection.
  const cachedProjection = stableStringify({
    tasks: cached.tasks,
    agents: cached.agents,
    recentEvents: cached.recentEvents,
  });
  const canonicalProjection = stableStringify({
    tasks: canonical.tasks,
    agents: canonical.agents,
    recentEvents: canonical.recentEvents,
  });
  return cachedProjection !== canonicalProjection;
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
    ...(trimToUndefined(value.lane) ? { lane: trimToUndefined(value.lane) } : {}),
    ...(trimToUndefined(value.currentTaskId)
      ? { currentTaskId: trimToUndefined(value.currentTaskId) }
      : {}),
    ...(trimToUndefined(value.sessionKey) ? { sessionKey: trimToUndefined(value.sessionKey) } : {}),
    summary: trimToUndefined(value.summary) ?? "Heartbeat",
    metadata: normalizeMetadata(value.metadata),
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
    if (options.agentId && record.entity === "agent" && record.agentId !== options.agentId) {
      return false;
    }
    if (options.agentId && record.entity !== "agent") {
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
    ...(trimToUndefined(input.agent.lane) ? { lane: trimToUndefined(input.agent.lane) } : {}),
    ...(trimToUndefined(input.agent.currentTaskId)
      ? { currentTaskId: trimToUndefined(input.agent.currentTaskId) }
      : {}),
    ...(trimToUndefined(input.agent.sessionKey)
      ? { sessionKey: trimToUndefined(input.agent.sessionKey) }
      : {}),
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
      const record = normalizePublishRecord(event, materialized.tasks);
      if (shouldSkipDuplicateRecord(record, materialized)) {
        continue;
      }
      applyRecordToMaps({
        record,
        tasks: materialized.tasks,
        agents: materialized.agents,
      });
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

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
export const TASK_ACTIVATION_SLA_METADATA_KEY = "activationSla" as const;
export const TASK_PROOF_CHECKPOINT_METADATA_KEY = "proofCheckpoint" as const;
export const TASK_OWNERSHIP_ESCALATION_METADATA_KEY = "ownershipEscalation" as const;
export const TASK_ACTIVATION_ACK_DEADLINE_MS = 5 * 60_000;
export const TASK_ACTIVATION_LANE_DEADLINE_MS = 10 * 60_000;
export const TASK_ACTIVATION_START_DEADLINE_MS = 15 * 60_000;

const DEFAULT_BUS_TOPIC = "shared.task.ledger";
const DEFAULT_SOURCE = "openclaw";
const DEFAULT_RECENT_EVENT_LIMIT = 200;
const DEFAULT_IDEMPOTENCY_WINDOW_MS = 10_000;
const RECONCILE_ACTOR_ID = "task-ledger-reconciler";
const RECONCILE_ACTOR_NAME = "Task ledger reconciler";
const RECONCILE_IDEMPOTENCY_PREFIX = "reconcile";
const RECONCILE_AGENT_STALE_MS = 15 * 60_000;
const PROOF_CHECKPOINT_REQUIRED_STATUS_NOTES = 2;
const OWNERSHIP_ACTIVATION_MISSES_TO_ESCALATE = 2;
const OWNERSHIP_ACTIVATION_MISSES_TO_REASSIGN = 3;
const OWNERSHIP_STATUS_ONLY_UPDATES_TO_ESCALATE = PROOF_CHECKPOINT_REQUIRED_STATUS_NOTES + 1;
const OWNERSHIP_STATUS_ONLY_UPDATES_TO_REASSIGN = PROOF_CHECKPOINT_REQUIRED_STATUS_NOTES + 2;
export type TaskProofCheckpointSignalType = "files" | "diffSummary" | "tests" | "reviewSignal";
const TASK_PROOF_CHECKPOINT_SIGNAL_TYPES = [
  "files",
  "diffSummary",
  "tests",
  "reviewSignal",
] as const satisfies readonly TaskProofCheckpointSignalType[];

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

type TaskActivationDisposition = "blocked" | "deferred";

type TaskActivationSla = {
  version: 1;
  assignedAt: string;
  acknowledgeWithinMs: number;
  acknowledgeDeadlineAt: string;
  laneWithinMs: number;
  laneDeadlineAt: string;
  startWithinMs: number;
  startDeadlineAt: string;
  acknowledgedAt?: string;
  lanePinnedAt?: string;
  lane?: string;
  startedAt?: string;
  startDisposition?: TaskActivationDisposition;
  startDispositionAt?: string;
  startDispositionReason?: string;
};

type TaskActivationEvidence = {
  activation: TaskActivationSla;
  acknowledgedAt?: string;
  lanePinnedAt?: string;
  lane?: string;
  startedAt?: string;
  startDisposition?: TaskActivationDisposition;
  startDispositionAt?: string;
  startDispositionReason?: string;
};

export type TaskProofCheckpoint = {
  files?: string[];
  diffSummary?: string;
  tests?: string[];
  reviewSignal?: string;
};

export type TaskProofCheckpointState = {
  version: 1;
  lastCheckpointAt?: string;
  lastCheckpoint?: TaskProofCheckpoint;
  statusOnlyUpdateCount: number;
  lastStatusNoteAt?: string;
  prompt?: {
    required: true;
    reason: "status_loop";
    requestedAt: string;
    requiredSignals: TaskProofCheckpointSignalType[];
  };
};

type TaskProofCheckpointEvidence = TaskProofCheckpointState & {
  currentCycleStartedAt?: string;
};

type TaskActivationMissedCheckpoint = "acknowledge" | "lane" | "start";

type TaskOwnershipEscalationLevel = "watch" | "escalated" | "reassignment_ready";

type TaskOwnershipEscalationTriggerCode =
  | "activation_sla"
  | "proof_checkpoint"
  | "assigned_agent_missing"
  | "assigned_agent_idle"
  | "assigned_agent_stale"
  | "heartbeat_claim_mismatch"
  | "blocked_superseded";

type TaskOwnershipEscalationTrigger = {
  code: TaskOwnershipEscalationTriggerCode;
  level: TaskOwnershipEscalationLevel;
  observedAt: string;
  summary: string;
  activationMisses?: {
    checkpoints: TaskActivationMissedCheckpoint[];
    missCount: number;
  };
  proofCheckpoint?: {
    statusOnlyUpdateCount: number;
    promptRequestedAt?: string;
  };
  ownership?: {
    assignedAgent?: string;
    claimedByAgent?: string;
    staleHeartbeatAt?: string;
    supersededByTaskId?: string;
  };
};

export type TaskOwnershipEscalationState = {
  version: 1;
  sourceOfTruth: "task_ledger";
  level: TaskOwnershipEscalationLevel;
  thresholds: {
    activationMissesToEscalate: number;
    activationMissesToReassign: number;
    statusOnlyUpdatesToPrompt: number;
    statusOnlyUpdatesToEscalate: number;
    statusOnlyUpdatesToReassign: number;
    staleHeartbeatMs: number;
  };
  triggers: TaskOwnershipEscalationTrigger[];
  takeover?: {
    recommended: true;
    through: "task_ledger";
    path: "publish_task_assignment";
    summary: string;
    currentAssignedAgent?: string;
    suggestedAgent?: string;
  };
};

type ActiveTaskWork = {
  taskId: string;
  tsMs: number;
  referenceId: string;
  source: "task" | "heartbeat";
};

type TaskOwnershipObservationContext = {
  reconciliationTs: string;
  reconciliationTsMs: number;
  lastSubstantiveTaskRecordById: Map<string, TaskLedgerTaskRecord>;
  lastAgentRecordById: Map<string, TaskLedgerAgentRecord>;
  activationEvidenceByTaskId: Map<string, TaskActivationEvidence>;
  proofCheckpointByTaskId: Map<string, TaskProofCheckpointEvidence>;
  activeWorkByAgentId: Map<string, ActiveTaskWork>;
  heartbeatClaimantsByTaskId: Map<
    string,
    Array<{ agentId: string; status: AgentActivityStatus; heartbeatAt: string }>
  >;
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
  proofCheckpoint?: TaskProofCheckpoint;
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

type TaskLedgerReconcileNoteRecord = TaskLedgerTaskRecord & {
  kind: "note";
  actor: TaskLedgerActor & {
    type: "system";
    id: typeof RECONCILE_ACTOR_ID;
  };
};

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
  proofCheckpoint?: TaskProofCheckpoint;
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

function normalizeStringList(value: unknown): string[] | undefined {
  if (typeof value === "string") {
    const normalized = trimToUndefined(value);
    return normalized ? [normalized] : undefined;
  }
  if (!Array.isArray(value)) {
    return undefined;
  }
  const normalized = value.flatMap((entry) => {
    const next = trimToUndefined(entry);
    return next ? [next] : [];
  });
  return normalized.length > 0 ? normalized : undefined;
}

function normalizeTaskProofCheckpointInput(value: unknown): TaskProofCheckpoint {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("proof checkpoint must be an object");
  }

  const raw = value as Record<string, unknown>;
  const files = normalizeStringList(raw.files);
  const diffSummary = trimToUndefined(raw.diffSummary);
  const tests = normalizeStringList(raw.tests);
  const reviewSignal = trimToUndefined(raw.reviewSignal ?? raw.review);
  const checkpoint: TaskProofCheckpoint = {
    ...(files ? { files } : {}),
    ...(diffSummary ? { diffSummary } : {}),
    ...(tests ? { tests } : {}),
    ...(reviewSignal ? { reviewSignal } : {}),
  };

  if (!hasConcreteTaskProofCheckpoint(checkpoint)) {
    throw new Error("proof checkpoint requires files, diffSummary, tests, or reviewSignal");
  }

  return checkpoint;
}

function parsePersistedTaskProofCheckpoint(value: unknown): TaskProofCheckpoint | undefined {
  try {
    return normalizeTaskProofCheckpointInput(value);
  } catch {
    return undefined;
  }
}

function hasConcreteTaskProofCheckpoint(
  value: TaskProofCheckpoint | undefined,
): value is TaskProofCheckpoint {
  return (
    value !== undefined &&
    ((value.files?.length ?? 0) > 0 ||
      !!trimToUndefined(value.diffSummary) ||
      (value.tests?.length ?? 0) > 0 ||
      !!trimToUndefined(value.reviewSignal))
  );
}

function addDeadline(ts: string, deltaMs: number): string {
  const parsed = Date.parse(ts);
  if (!Number.isFinite(parsed)) {
    return ts;
  }
  return new Date(parsed + deltaMs).toISOString();
}

function normalizeActivationDisposition(value: unknown): TaskActivationDisposition | undefined {
  return value === "blocked" || value === "deferred" ? value : undefined;
}

function parseTaskActivationSla(value: unknown): TaskActivationSla | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined;
  }

  const raw = value as Record<string, unknown>;
  const assignedAt = normalizePersistedTimestamp(raw.assignedAt);
  const acknowledgeDeadlineAt = normalizePersistedTimestamp(raw.acknowledgeDeadlineAt);
  const laneDeadlineAt = normalizePersistedTimestamp(raw.laneDeadlineAt);
  const startDeadlineAt = normalizePersistedTimestamp(raw.startDeadlineAt);
  if (!assignedAt || !acknowledgeDeadlineAt || !laneDeadlineAt || !startDeadlineAt) {
    return undefined;
  }

  const acknowledgeWithinMs = normalizeNonNegativeInteger(raw.acknowledgeWithinMs);
  const laneWithinMs = normalizeNonNegativeInteger(raw.laneWithinMs);
  const startWithinMs = normalizeNonNegativeInteger(raw.startWithinMs);

  return {
    version: 1,
    assignedAt,
    acknowledgeWithinMs:
      acknowledgeWithinMs > 0 ? acknowledgeWithinMs : TASK_ACTIVATION_ACK_DEADLINE_MS,
    acknowledgeDeadlineAt,
    laneWithinMs: laneWithinMs > 0 ? laneWithinMs : TASK_ACTIVATION_LANE_DEADLINE_MS,
    laneDeadlineAt,
    startWithinMs: startWithinMs > 0 ? startWithinMs : TASK_ACTIVATION_START_DEADLINE_MS,
    startDeadlineAt,
    ...(normalizePersistedTimestamp(raw.acknowledgedAt)
      ? { acknowledgedAt: normalizePersistedTimestamp(raw.acknowledgedAt) ?? undefined }
      : {}),
    ...(normalizePersistedTimestamp(raw.lanePinnedAt)
      ? { lanePinnedAt: normalizePersistedTimestamp(raw.lanePinnedAt) ?? undefined }
      : {}),
    ...(trimToUndefined(raw.lane) ? { lane: trimToUndefined(raw.lane) } : {}),
    ...(normalizePersistedTimestamp(raw.startedAt)
      ? { startedAt: normalizePersistedTimestamp(raw.startedAt) ?? undefined }
      : {}),
    ...(normalizeActivationDisposition(raw.startDisposition)
      ? { startDisposition: normalizeActivationDisposition(raw.startDisposition) }
      : {}),
    ...(normalizePersistedTimestamp(raw.startDispositionAt)
      ? { startDispositionAt: normalizePersistedTimestamp(raw.startDispositionAt) ?? undefined }
      : {}),
    ...(trimToUndefined(raw.startDispositionReason)
      ? { startDispositionReason: trimToUndefined(raw.startDispositionReason) }
      : {}),
  };
}

function createTaskActivationSla(assignedAt: string): TaskActivationSla {
  return {
    version: 1,
    assignedAt,
    acknowledgeWithinMs: TASK_ACTIVATION_ACK_DEADLINE_MS,
    acknowledgeDeadlineAt: addDeadline(assignedAt, TASK_ACTIVATION_ACK_DEADLINE_MS),
    laneWithinMs: TASK_ACTIVATION_LANE_DEADLINE_MS,
    laneDeadlineAt: addDeadline(assignedAt, TASK_ACTIVATION_LANE_DEADLINE_MS),
    startWithinMs: TASK_ACTIVATION_START_DEADLINE_MS,
    startDeadlineAt: addDeadline(assignedAt, TASK_ACTIVATION_START_DEADLINE_MS),
  };
}

function withTaskActivationSlaMetadata(
  metadata: Record<string, unknown>,
  activation: TaskActivationSla | undefined,
): Record<string, unknown> {
  const next = normalizeMetadata(metadata);
  if (!activation) {
    if (Object.hasOwn(next, TASK_ACTIVATION_SLA_METADATA_KEY)) {
      delete next[TASK_ACTIVATION_SLA_METADATA_KEY];
    }
    return next;
  }
  next[TASK_ACTIVATION_SLA_METADATA_KEY] = activation;
  return next;
}

function buildNextTaskActivationSla(params: {
  existing?: TaskLedgerTask;
  next: TaskLedgerTask;
  ts: string;
}): TaskActivationSla | undefined {
  const assignedAgent = trimToUndefined(params.next.assignedAgent);
  if (!assignedAgent) {
    return undefined;
  }
  if (params.next.state === "done" || params.next.state === "qa") {
    return undefined;
  }

  const existingActivation = parseTaskActivationSla(
    params.existing?.metadata[TASK_ACTIVATION_SLA_METADATA_KEY],
  );
  const assignedChanged = assignedAgent !== trimToUndefined(params.existing?.assignedAgent);
  const reopenedIntoTodo =
    (params.next.state === "backlog" || params.next.state === "todo") &&
    params.existing !== undefined &&
    params.existing.state !== params.next.state &&
    (params.existing.state === "in_progress" ||
      params.existing.state === "blocked" ||
      params.existing.state === "qa" ||
      params.existing.state === "done");

  let activation =
    !existingActivation || assignedChanged || reopenedIntoTodo
      ? createTaskActivationSla(params.ts)
      : { ...existingActivation };

  if (params.next.state === "in_progress" && !activation.startedAt) {
    activation.startedAt = params.ts;
  }

  if (params.next.state === "blocked" && !activation.startDisposition) {
    activation.startDisposition = "blocked";
    activation.startDispositionAt = params.ts;
    activation.startDispositionReason =
      trimToUndefined(params.next.blockedReason) ?? activation.startDispositionReason;
  }

  return activation;
}

function buildTaskWithActivationMetadata(
  existing: TaskLedgerTask | undefined,
  patch: TaskLedgerTaskPatch,
  ts: string,
  fallbackTitle?: string,
): TaskLedgerTask {
  const next = mergeTaskRecord(existing, patch, ts, fallbackTitle);
  const activation = buildNextTaskActivationSla({ existing, next, ts });
  next.metadata = withTaskActivationSlaMetadata(next.metadata, activation);
  return next;
}

function metadataChanged(
  existing: TaskLedgerTask | undefined,
  next: TaskLedgerTask,
  patch: TaskLedgerTaskPatch,
): boolean {
  if (patch.metadata !== undefined) {
    return true;
  }
  return stableStringify(existing?.metadata ?? {}) !== stableStringify(next.metadata);
}

function withTaskProofCheckpointMetadata(
  metadata: Record<string, unknown>,
  proofCheckpoint: TaskProofCheckpointState | undefined,
): Record<string, unknown> {
  const next = normalizeMetadata(metadata);
  if (!proofCheckpoint) {
    if (Object.hasOwn(next, TASK_PROOF_CHECKPOINT_METADATA_KEY)) {
      delete next[TASK_PROOF_CHECKPOINT_METADATA_KEY];
    }
    return next;
  }
  next[TASK_PROOF_CHECKPOINT_METADATA_KEY] = proofCheckpoint;
  return next;
}

function withTaskOwnershipEscalationMetadata(
  metadata: Record<string, unknown>,
  escalation: TaskOwnershipEscalationState | undefined,
): Record<string, unknown> {
  const next = normalizeMetadata(metadata);
  if (!escalation) {
    if (Object.hasOwn(next, TASK_OWNERSHIP_ESCALATION_METADATA_KEY)) {
      delete next[TASK_OWNERSHIP_ESCALATION_METADATA_KEY];
    }
    return next;
  }
  next[TASK_OWNERSHIP_ESCALATION_METADATA_KEY] = escalation;
  return next;
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
      proofCheckpoint: record.proofCheckpoint,
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
  applyDerivedTaskActivationEvidence(materialized);
  applyDerivedTaskProofCheckpointEvidence(materialized);
  applyDerivedTaskOwnershipEscalationEvidence(materialized);
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

function isTaskLedgerTaskRecord(record: TaskLedgerRecord): record is TaskLedgerTaskRecord {
  return record.entity === "task";
}

function isReconcileTaskNote(
  record: TaskLedgerRecord | TaskLedgerTaskRecord,
): record is TaskLedgerReconcileNoteRecord {
  return (
    record.entity === "task" &&
    record.kind === "note" &&
    record.actor.type === "system" &&
    record.actor.id === RECONCILE_ACTOR_ID
  );
}

function resolveActivationStartDisposition(
  record: TaskLedgerTaskRecord,
): Pick<TaskActivationEvidence, "startDisposition" | "startDispositionReason"> | null {
  const key = trimToUndefined(record.idempotencyKey);
  if (record.toState === "blocked" || record.kind === "blocked") {
    return {
      startDisposition: "blocked",
      startDispositionReason: trimToUndefined(record.summary),
    };
  }
  if (key?.startsWith("task-milestone:waiting-for-input:")) {
    return {
      startDisposition: "deferred",
      startDispositionReason: trimToUndefined(record.summary),
    };
  }
  if (
    key?.startsWith("task-milestone:unsafe-to-proceed:") ||
    key?.startsWith("task-milestone:repeated-failure:")
  ) {
    return {
      startDisposition: "blocked",
      startDispositionReason: trimToUndefined(record.summary),
    };
  }
  return null;
}

function isActivationStartProof(record: TaskLedgerTaskRecord): boolean {
  const key = trimToUndefined(record.idempotencyKey);
  return (
    key?.startsWith("task-milestone:run-started:") === true ||
    record.toState === "in_progress" ||
    record.kind === "started"
  );
}

function resolveExplicitAgentActivationNoteEvidence(
  record: TaskLedgerTaskRecord,
  assignedAgent: string,
): {
  acknowledged: true;
  started?: true;
  startDisposition?: TaskActivationDisposition;
  startDispositionReason?: string;
} | null {
  if (
    record.kind !== "note" ||
    record.actor.type !== "agent" ||
    trimToUndefined(record.actor.id) !== assignedAgent
  ) {
    return null;
  }

  const summary = trimToUndefined(record.summary)?.toLowerCase();
  if (!summary) {
    return null;
  }

  if (/^(accepted|acknowledged)\b/.test(summary)) {
    return { acknowledged: true };
  }
  if (/^(in[- ]progress|started|starting)\b/.test(summary)) {
    return { acknowledged: true, started: true };
  }
  if (/^(blocked|unsafe to proceed)\b/.test(summary)) {
    return {
      acknowledged: true,
      startDisposition: "blocked",
      startDispositionReason: trimToUndefined(record.summary),
    };
  }
  if (/^(deferred|waiting(?: |-)?for(?: |-)?input)\b/.test(summary)) {
    return {
      acknowledged: true,
      startDisposition: "deferred",
      startDispositionReason: trimToUndefined(record.summary),
    };
  }
  return null;
}

function deriveTaskActivationEvidence(
  task: TaskLedgerTask,
  records: TaskLedgerRecord[],
): TaskActivationEvidence | null {
  const activation = parseTaskActivationSla(task.metadata[TASK_ACTIVATION_SLA_METADATA_KEY]);
  const assignedAgent = trimToUndefined(task.assignedAgent);
  if (!activation || !assignedAgent) {
    return null;
  }

  const assignedAtMs = Date.parse(activation.assignedAt);
  if (!Number.isFinite(assignedAtMs)) {
    return null;
  }

  const evidence: TaskActivationEvidence = {
    activation: { ...activation },
    ...(activation.acknowledgedAt ? { acknowledgedAt: activation.acknowledgedAt } : {}),
    ...(activation.lanePinnedAt ? { lanePinnedAt: activation.lanePinnedAt } : {}),
    ...(activation.lane ? { lane: activation.lane } : {}),
    ...(activation.startedAt ? { startedAt: activation.startedAt } : {}),
    ...(activation.startDisposition ? { startDisposition: activation.startDisposition } : {}),
    ...(activation.startDispositionAt ? { startDispositionAt: activation.startDispositionAt } : {}),
    ...(activation.startDispositionReason
      ? { startDispositionReason: activation.startDispositionReason }
      : {}),
  };

  for (const record of records) {
    const recordTsMs = Date.parse(record.ts);
    if (!Number.isFinite(recordTsMs) || recordTsMs < assignedAtMs) {
      continue;
    }

    if (
      record.entity === "agent" &&
      record.agentId === assignedAgent &&
      trimToUndefined(record.currentTaskId) === task.id
    ) {
      if (!evidence.acknowledgedAt) {
        evidence.acknowledgedAt = record.ts;
      }
      const lane = trimToUndefined(record.lane);
      if (lane && !evidence.lanePinnedAt) {
        evidence.lanePinnedAt = record.ts;
        evidence.lane = lane;
      }
      continue;
    }

    if (
      !isTaskLedgerTaskRecord(record) ||
      record.taskId !== task.id ||
      isReconcileTaskNote(record)
    ) {
      continue;
    }

    const taskRecord = record;
    const explicitAgentEvidence = resolveExplicitAgentActivationNoteEvidence(
      taskRecord,
      assignedAgent,
    );
    const disposition =
      explicitAgentEvidence?.startDisposition !== undefined
        ? {
            startDisposition: explicitAgentEvidence.startDisposition,
            startDispositionReason: explicitAgentEvidence.startDispositionReason,
          }
        : resolveActivationStartDisposition(taskRecord);

    if (
      !evidence.startedAt &&
      (isActivationStartProof(taskRecord) || explicitAgentEvidence?.started)
    ) {
      evidence.startedAt = taskRecord.ts;
    }
    if (disposition && !evidence.startDisposition) {
      evidence.startDisposition = disposition.startDisposition;
      evidence.startDispositionAt = taskRecord.ts;
      evidence.startDispositionReason = disposition.startDispositionReason;
    }
    if (
      !evidence.acknowledgedAt &&
      (explicitAgentEvidence?.acknowledged || evidence.startedAt || disposition)
    ) {
      evidence.acknowledgedAt = taskRecord.ts;
    }
  }

  return evidence;
}

function applyDerivedTaskActivationEvidence(materialized: MaterializedLedgerState) {
  for (const task of materialized.tasks.values()) {
    const evidence = deriveTaskActivationEvidence(task, materialized.appliedRecords);
    if (!evidence) {
      continue;
    }
    task.metadata = withTaskActivationSlaMetadata(task.metadata, {
      ...evidence.activation,
      ...(evidence.acknowledgedAt ? { acknowledgedAt: evidence.acknowledgedAt } : {}),
      ...(evidence.lanePinnedAt ? { lanePinnedAt: evidence.lanePinnedAt } : {}),
      ...(evidence.lane ? { lane: evidence.lane } : {}),
      ...(evidence.startedAt ? { startedAt: evidence.startedAt } : {}),
      ...(evidence.startDisposition ? { startDisposition: evidence.startDisposition } : {}),
      ...(evidence.startDispositionAt ? { startDispositionAt: evidence.startDispositionAt } : {}),
      ...(evidence.startDispositionReason
        ? { startDispositionReason: evidence.startDispositionReason }
        : {}),
    });
  }
}

function resolveTaskRecordState(
  record: TaskLedgerTaskRecord,
  currentState: TaskState | undefined,
): TaskState | undefined {
  if (isTaskState(record.toState)) {
    return record.toState;
  }
  return isTaskState(record.task?.state) ? record.task.state : currentState;
}

function hasTaskPatchFields(record: TaskLedgerTaskRecord): boolean {
  return record.task !== undefined && Object.keys(record.task).length > 0;
}

function isStatusOnlyAgentTaskNote(record: TaskLedgerTaskRecord): boolean {
  return (
    record.kind === "note" &&
    record.actor.type === "agent" &&
    !isReconcileTaskNote(record) &&
    !hasConcreteTaskProofCheckpoint(record.proofCheckpoint) &&
    !record.toState &&
    !hasTaskPatchFields(record)
  );
}

function isProofCheckpointPromptRecord(record: TaskLedgerTaskRecord): boolean {
  return (
    isReconcileTaskNote(record) &&
    trimToUndefined(record.idempotencyKey)?.startsWith("reconcile:proof-checkpoint-required:") ===
      true
  );
}

function deriveTaskProofCheckpointEvidence(
  task: TaskLedgerTask,
  records: TaskLedgerRecord[],
): TaskProofCheckpointEvidence | null {
  let currentState: TaskState | undefined;
  let currentCycleStartedAt: string | undefined;
  let lastCheckpointAt: string | undefined;
  let lastCheckpoint: TaskProofCheckpoint | undefined;
  let statusOnlyUpdateCount = 0;
  let lastStatusNoteAt: string | undefined;
  let prompt: TaskProofCheckpointState["prompt"];

  for (const record of records) {
    if (!isTaskLedgerTaskRecord(record) || record.taskId !== task.id) {
      continue;
    }

    const taskRecord = record;
    const nextState = resolveTaskRecordState(taskRecord, currentState);
    const enteredInProgress = nextState === "in_progress" && currentState !== "in_progress";
    const leftInProgress = currentState === "in_progress" && nextState !== "in_progress";

    if (enteredInProgress) {
      currentCycleStartedAt = taskRecord.ts;
      statusOnlyUpdateCount = 0;
      lastStatusNoteAt = undefined;
      prompt = undefined;
    } else if (leftInProgress) {
      currentCycleStartedAt = undefined;
      statusOnlyUpdateCount = 0;
      lastStatusNoteAt = undefined;
      prompt = undefined;
    }

    currentState = nextState;

    if (hasConcreteTaskProofCheckpoint(taskRecord.proofCheckpoint)) {
      lastCheckpointAt = taskRecord.ts;
      lastCheckpoint = taskRecord.proofCheckpoint;
      statusOnlyUpdateCount = 0;
      lastStatusNoteAt = undefined;
      prompt = undefined;
      continue;
    }

    if (currentState !== "in_progress") {
      continue;
    }

    if (isStatusOnlyAgentTaskNote(taskRecord)) {
      statusOnlyUpdateCount += 1;
      lastStatusNoteAt = taskRecord.ts;
      continue;
    }

    if (isProofCheckpointPromptRecord(taskRecord)) {
      prompt = {
        required: true,
        reason: "status_loop",
        requestedAt: taskRecord.ts,
        requiredSignals: [...TASK_PROOF_CHECKPOINT_SIGNAL_TYPES],
      };
    }
  }

  if (!lastCheckpointAt && statusOnlyUpdateCount === 0 && !prompt) {
    return null;
  }

  return {
    version: 1,
    ...(currentCycleStartedAt ? { currentCycleStartedAt } : {}),
    ...(lastCheckpointAt ? { lastCheckpointAt } : {}),
    ...(lastCheckpoint ? { lastCheckpoint } : {}),
    statusOnlyUpdateCount,
    ...(lastStatusNoteAt ? { lastStatusNoteAt } : {}),
    ...(prompt ? { prompt } : {}),
  };
}

function applyDerivedTaskProofCheckpointEvidence(materialized: MaterializedLedgerState) {
  for (const task of materialized.tasks.values()) {
    const evidence = deriveTaskProofCheckpointEvidence(task, materialized.appliedRecords);
    if (!evidence) {
      task.metadata = withTaskProofCheckpointMetadata(task.metadata, undefined);
      continue;
    }
    const { currentCycleStartedAt: _currentCycleStartedAt, ...persistedEvidence } = evidence;
    task.metadata = withTaskProofCheckpointMetadata(task.metadata, persistedEvidence);
  }
}

function ownershipEscalationLevelRank(level: TaskOwnershipEscalationLevel): number {
  switch (level) {
    case "watch":
      return 1;
    case "escalated":
      return 2;
    case "reassignment_ready":
      return 3;
  }
}

function maxOwnershipEscalationLevel(
  left: TaskOwnershipEscalationLevel,
  right: TaskOwnershipEscalationLevel,
): TaskOwnershipEscalationLevel {
  return ownershipEscalationLevelRank(left) >= ownershipEscalationLevelRank(right) ? left : right;
}

function buildTaskOwnershipEscalationThresholds(): TaskOwnershipEscalationState["thresholds"] {
  return {
    activationMissesToEscalate: OWNERSHIP_ACTIVATION_MISSES_TO_ESCALATE,
    activationMissesToReassign: OWNERSHIP_ACTIVATION_MISSES_TO_REASSIGN,
    statusOnlyUpdatesToPrompt: PROOF_CHECKPOINT_REQUIRED_STATUS_NOTES,
    statusOnlyUpdatesToEscalate: OWNERSHIP_STATUS_ONLY_UPDATES_TO_ESCALATE,
    statusOnlyUpdatesToReassign: OWNERSHIP_STATUS_ONLY_UPDATES_TO_REASSIGN,
    staleHeartbeatMs: RECONCILE_AGENT_STALE_MS,
  };
}

function isFreshAgentHeartbeat(heartbeatAtMs: number, reconciliationTsMs: number): boolean {
  return (
    Number.isFinite(heartbeatAtMs) &&
    Number.isFinite(reconciliationTsMs) &&
    reconciliationTsMs - heartbeatAtMs < RECONCILE_AGENT_STALE_MS
  );
}

function findLatestTaskRecord(
  records: TaskLedgerRecord[],
  taskId: string,
  predicate?: (record: TaskLedgerTaskRecord) => boolean,
): TaskLedgerTaskRecord | undefined {
  for (let index = records.length - 1; index >= 0; index -= 1) {
    const record = records[index];
    if (record?.entity !== "task" || record.taskId !== taskId || isReconcileTaskNote(record)) {
      continue;
    }
    if (!predicate || predicate(record)) {
      return record;
    }
  }
  return undefined;
}

function buildTaskOwnershipObservationContext(
  materialized: MaterializedLedgerState,
): TaskOwnershipObservationContext {
  const lastSubstantiveTaskRecordById = new Map<string, TaskLedgerTaskRecord>();
  const lastAgentRecordById = new Map<string, TaskLedgerAgentRecord>();
  const activationEvidenceByTaskId = new Map<string, TaskActivationEvidence>();
  const proofCheckpointByTaskId = new Map<string, TaskProofCheckpointEvidence>();
  const activeWorkByAgentId = new Map<string, ActiveTaskWork>();
  const heartbeatClaimantsByTaskId = new Map<
    string,
    Array<{ agentId: string; status: AgentActivityStatus; heartbeatAt: string }>
  >();
  const reconciliationTs = materialized.appliedRecords.at(-1)?.ts ?? new Date().toISOString();
  const reconciliationTsMs = Date.parse(reconciliationTs);

  const considerActiveWork = (agentId: string, candidate: ActiveTaskWork) => {
    if (!Number.isFinite(candidate.tsMs)) {
      return;
    }
    const current = activeWorkByAgentId.get(agentId);
    if (!current || candidate.tsMs > current.tsMs) {
      activeWorkByAgentId.set(agentId, candidate);
    }
  };

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

  for (const task of materialized.tasks.values()) {
    const activationEvidence = deriveTaskActivationEvidence(task, materialized.appliedRecords);
    if (activationEvidence) {
      activationEvidenceByTaskId.set(task.id, activationEvidence);
    }

    const proofCheckpointEvidence = deriveTaskProofCheckpointEvidence(
      task,
      materialized.appliedRecords,
    );
    if (proofCheckpointEvidence) {
      proofCheckpointByTaskId.set(task.id, proofCheckpointEvidence);
    }

    const assignedAgent = trimToUndefined(task.assignedAgent);
    if (task.state !== "in_progress" || !assignedAgent) {
      continue;
    }
    const taskRecord = lastSubstantiveTaskRecordById.get(task.id);
    considerActiveWork(assignedAgent, {
      taskId: task.id,
      tsMs: taskRecord ? Date.parse(taskRecord.ts) : Date.parse(task.lastEventAt),
      referenceId: taskRecord?.id ?? task.id,
      source: "task",
    });
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

    const heartbeatAtMs = Date.parse(agentRecord.ts);
    const heartbeatIsFresh = isFreshAgentHeartbeat(heartbeatAtMs, reconciliationTsMs);

    if (heartbeatIsFresh && agent.status !== "idle") {
      considerActiveWork(agent.id, {
        taskId: currentTaskId,
        tsMs: heartbeatAtMs,
        referenceId: agentRecord.id,
        source: "heartbeat",
      });

      const existing = heartbeatClaimantsByTaskId.get(currentTaskId) ?? [];
      if (!existing.some((entry) => entry.agentId === agent.id)) {
        existing.push({
          agentId: agent.id,
          status: agent.status,
          heartbeatAt: agent.heartbeatAt,
        });
        heartbeatClaimantsByTaskId.set(currentTaskId, existing);
      }
    }
  }

  return {
    reconciliationTs,
    reconciliationTsMs,
    lastSubstantiveTaskRecordById,
    lastAgentRecordById,
    activationEvidenceByTaskId,
    proofCheckpointByTaskId,
    activeWorkByAgentId,
    heartbeatClaimantsByTaskId,
  };
}

function deriveTaskOwnershipEscalationState(
  task: TaskLedgerTask,
  materialized: MaterializedLedgerState,
  context: TaskOwnershipObservationContext,
): TaskOwnershipEscalationState | null {
  const thresholds = buildTaskOwnershipEscalationThresholds();
  const triggers: TaskOwnershipEscalationTrigger[] = [];
  let level: TaskOwnershipEscalationLevel = "watch";
  let takeover: TaskOwnershipEscalationState["takeover"];
  const assignedAgent = trimToUndefined(task.assignedAgent);
  const taskRecord = context.lastSubstantiveTaskRecordById.get(task.id);

  const addTrigger = (trigger: TaskOwnershipEscalationTrigger) => {
    triggers.push(trigger);
    level = maxOwnershipEscalationLevel(level, trigger.level);
  };

  const activationEvidence = context.activationEvidenceByTaskId.get(task.id);
  if (
    assignedAgent &&
    activationEvidence &&
    task.state !== "blocked" &&
    Number.isFinite(context.reconciliationTsMs)
  ) {
    const activation = activationEvidence.activation;
    const missedCheckpoints: TaskActivationMissedCheckpoint[] = [];
    if (
      !activationEvidence.acknowledgedAt &&
      Number.isFinite(Date.parse(activation.acknowledgeDeadlineAt)) &&
      context.reconciliationTsMs >= Date.parse(activation.acknowledgeDeadlineAt)
    ) {
      missedCheckpoints.push("acknowledge");
    }
    if (
      !activationEvidence.lanePinnedAt &&
      !activationEvidence.startDisposition &&
      Number.isFinite(Date.parse(activation.laneDeadlineAt)) &&
      context.reconciliationTsMs >= Date.parse(activation.laneDeadlineAt)
    ) {
      missedCheckpoints.push("lane");
    }
    if (
      !activationEvidence.startedAt &&
      !activationEvidence.startDisposition &&
      Number.isFinite(Date.parse(activation.startDeadlineAt)) &&
      context.reconciliationTsMs >= Date.parse(activation.startDeadlineAt)
    ) {
      missedCheckpoints.push("start");
    }

    if (missedCheckpoints.length > 0) {
      const missCount = missedCheckpoints.length;
      addTrigger({
        code: "activation_sla",
        level:
          missCount >= OWNERSHIP_ACTIVATION_MISSES_TO_REASSIGN
            ? "reassignment_ready"
            : missCount >= OWNERSHIP_ACTIVATION_MISSES_TO_ESCALATE
              ? "escalated"
              : "watch",
        observedAt:
          missedCheckpoints.at(-1) === "start"
            ? activation.startDeadlineAt
            : missedCheckpoints.at(-1) === "lane"
              ? activation.laneDeadlineAt
              : activation.acknowledgeDeadlineAt,
        summary: `Missed ${missCount} of 3 activation checkpoints in the current assignment cycle.`,
        activationMisses: {
          checkpoints: missedCheckpoints,
          missCount,
        },
      });
    }
  }

  const proofCheckpointEvidence = context.proofCheckpointByTaskId.get(task.id);
  if (
    task.state === "in_progress" &&
    proofCheckpointEvidence &&
    proofCheckpointEvidence.statusOnlyUpdateCount >= PROOF_CHECKPOINT_REQUIRED_STATUS_NOTES
  ) {
    const statusOnlyUpdateCount = proofCheckpointEvidence.statusOnlyUpdateCount;
    addTrigger({
      code: "proof_checkpoint",
      level:
        statusOnlyUpdateCount >= OWNERSHIP_STATUS_ONLY_UPDATES_TO_REASSIGN
          ? "reassignment_ready"
          : statusOnlyUpdateCount >= OWNERSHIP_STATUS_ONLY_UPDATES_TO_ESCALATE
            ? "escalated"
            : "watch",
      observedAt:
        proofCheckpointEvidence.lastStatusNoteAt ??
        proofCheckpointEvidence.prompt?.requestedAt ??
        task.lastEventAt,
      summary: `Observed ${statusOnlyUpdateCount} consecutive status-only in-progress updates since the last proof checkpoint.`,
      proofCheckpoint: {
        statusOnlyUpdateCount,
        ...(proofCheckpointEvidence.prompt?.requestedAt
          ? { promptRequestedAt: proofCheckpointEvidence.prompt.requestedAt }
          : {}),
      },
    });
  }

  if (task.state === "in_progress" && assignedAgent) {
    const agent = materialized.agents.get(assignedAgent);
    const agentRecord = context.lastAgentRecordById.get(assignedAgent);
    if (!agent || !agentRecord) {
      addTrigger({
        code: "assigned_agent_missing",
        level: "escalated",
        observedAt: taskRecord?.ts ?? task.lastEventAt,
        summary: `Assigned agent ${assignedAgent} has no current heartbeat for this in-progress task.`,
        ownership: { assignedAgent },
      });
    } else if (agent.status === "idle") {
      addTrigger({
        code: "assigned_agent_idle",
        level: "escalated",
        observedAt: agent.lastSeenAt,
        summary: `Assigned agent ${assignedAgent} is idle while the task is still in progress.`,
        ownership: { assignedAgent },
      });
    } else {
      const staleDeltaMs = context.reconciliationTsMs - Date.parse(agent.lastSeenAt);
      if (Number.isFinite(staleDeltaMs) && staleDeltaMs >= RECONCILE_AGENT_STALE_MS) {
        addTrigger({
          code: "assigned_agent_stale",
          level: "escalated",
          observedAt: agent.lastSeenAt,
          summary: `Assigned agent ${assignedAgent} has a stale heartbeat for this in-progress task.`,
          ownership: {
            assignedAgent,
            staleHeartbeatAt: agent.lastSeenAt,
          },
        });
      }
    }
  }

  const claimant = (context.heartbeatClaimantsByTaskId.get(task.id) ?? [])
    .filter((entry) => entry.status !== "idle" && entry.agentId !== assignedAgent)
    .toSorted((left, right) => Date.parse(right.heartbeatAt) - Date.parse(left.heartbeatAt))[0];
  if (claimant) {
    addTrigger({
      code: "heartbeat_claim_mismatch",
      level: "reassignment_ready",
      observedAt: claimant.heartbeatAt,
      summary: `Agent ${claimant.agentId} is actively heartbeating this task while ledger ownership points elsewhere.`,
      ownership: {
        ...(assignedAgent ? { assignedAgent } : {}),
        claimedByAgent: claimant.agentId,
      },
    });
    takeover = {
      recommended: true,
      through: "task_ledger",
      path: "publish_task_assignment",
      summary: assignedAgent
        ? `If ${claimant.agentId} is the real owner, reassign the task in the ledger by updating assignedAgent from ${assignedAgent} to ${claimant.agentId}; otherwise clear ${claimant.agentId}'s heartbeat claim. Mission Control remains a control surface only.`
        : `If ${claimant.agentId} is the real owner, assign the task to ${claimant.agentId} through the ledger; otherwise clear ${claimant.agentId}'s heartbeat claim. Mission Control remains a control surface only.`,
      ...(assignedAgent ? { currentAssignedAgent: assignedAgent } : {}),
      suggestedAgent: claimant.agentId,
    };
  }

  if (task.state === "blocked" && assignedAgent) {
    const blockedRecord = findLatestTaskRecord(
      materialized.appliedRecords,
      task.id,
      (record) => record.kind !== "note",
    );
    const blockedTsMs = blockedRecord ? Date.parse(blockedRecord.ts) : Date.parse(task.lastEventAt);
    const activeWork = context.activeWorkByAgentId.get(assignedAgent);
    if (
      activeWork &&
      activeWork.taskId !== task.id &&
      Number.isFinite(blockedTsMs) &&
      activeWork.tsMs > blockedTsMs
    ) {
      addTrigger({
        code: "blocked_superseded",
        level: "reassignment_ready",
        observedAt: new Date(activeWork.tsMs).toISOString(),
        summary: `Blocked ownership is superseded by newer active work on ${activeWork.taskId}.`,
        ownership: {
          assignedAgent,
          supersededByTaskId: activeWork.taskId,
        },
      });
      takeover ??= {
        recommended: true,
        through: "task_ledger",
        path: "publish_task_assignment",
        summary: `If ${task.id} still needs work, reassign or clear its ownership through the ledger before anyone takes it over, then require the gaining owner to heartbeat currentTaskId ${task.id}. Mission Control remains a control surface only.`,
        currentAssignedAgent: assignedAgent,
      };
    }
  }

  if (triggers.length === 0) {
    return null;
  }

  if (level === "reassignment_ready" && !takeover) {
    takeover = {
      recommended: true,
      through: "task_ledger",
      path: "publish_task_assignment",
      summary: assignedAgent
        ? `Reassign the task through the ledger by updating assignedAgent (or clearing stale ownership), then require the gaining owner to heartbeat currentTaskId ${task.id}. Mission Control remains a control surface only.`
        : `Assign the task through the ledger before anyone takes it over, then require the gaining owner to heartbeat currentTaskId ${task.id}. Mission Control remains a control surface only.`,
      ...(assignedAgent ? { currentAssignedAgent: assignedAgent } : {}),
    };
  }

  return {
    version: 1,
    sourceOfTruth: "task_ledger",
    level,
    thresholds,
    triggers,
    ...(takeover ? { takeover } : {}),
  };
}

function applyDerivedTaskOwnershipEscalationEvidence(materialized: MaterializedLedgerState) {
  const context = buildTaskOwnershipObservationContext(materialized);
  for (const task of materialized.tasks.values()) {
    const escalation = deriveTaskOwnershipEscalationState(task, materialized, context);
    task.metadata = withTaskOwnershipEscalationMetadata(task.metadata, escalation ?? undefined);
  }
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

  const context = buildTaskOwnershipObservationContext(materialized);
  const emittedIdempotencyKeys = new Set<string>();
  const notes: TaskLedgerTaskRecord[] = [];

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

  for (const task of materialized.tasks.values()) {
    const assignedAgent = trimToUndefined(task.assignedAgent);
    const activationEvidence = context.activationEvidenceByTaskId.get(task.id);
    const proofCheckpointEvidence = context.proofCheckpointByTaskId.get(task.id);

    if (task.state === "in_progress" && assignedAgent) {
      const agent = materialized.agents.get(assignedAgent);
      const agentRecord = context.lastAgentRecordById.get(assignedAgent);

      if (!agent || !agentRecord) {
        queueTaskNote(
          task.id,
          `Reconcile residue: task is still marked in progress for assigned agent ${assignedAgent}, but no agent heartbeat is recorded. This is immediate ownership escalation. If ${assignedAgent} is no longer the owner, reassign through the ledger by updating assignedAgent (or clearing it), then require the gaining owner to heartbeat currentTaskId ${task.id}. Mission Control remains a control surface only.`,
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
          `Reconcile residue: task is still marked in progress for assigned agent ${assignedAgent}, but the latest heartbeat reports the agent idle. This is immediate ownership escalation. If ${assignedAgent} does not resume through the ledger, reassign by updating assignedAgent (or clearing stale ownership), then require the gaining owner to heartbeat currentTaskId ${task.id}. Mission Control remains a control surface only.`,
          buildReconcileIdempotencyKey("in-progress-agent-idle", [
            task.id,
            assignedAgent,
            agent.status,
          ]),
        );
        continue;
      }

      const staleDeltaMs = context.reconciliationTsMs - Date.parse(agent.lastSeenAt);
      if (Number.isFinite(staleDeltaMs) && staleDeltaMs >= RECONCILE_AGENT_STALE_MS) {
        queueTaskNote(
          task.id,
          `Reconcile residue: task is still marked in progress for assigned agent ${assignedAgent}, but the latest heartbeat is stale (${agent.lastSeenAt}). This is immediate ownership escalation. If ${assignedAgent} does not refresh task ownership through the ledger, reassign by updating assignedAgent (or clearing stale ownership), then require the gaining owner to heartbeat currentTaskId ${task.id}. Mission Control remains a control surface only.`,
          buildReconcileIdempotencyKey("in-progress-agent-stale", [
            task.id,
            assignedAgent,
            "stale",
          ]),
          Number.isFinite(context.reconciliationTsMs)
            ? new Date(context.reconciliationTsMs).toISOString()
            : undefined,
        );
      }
    }

    if (
      assignedAgent &&
      activationEvidence &&
      Number.isFinite(context.reconciliationTsMs) &&
      task.state !== "blocked"
    ) {
      const activation = activationEvidence.activation;
      const activationCycleId = `${task.id}:${assignedAgent}:${activation.assignedAt}`;
      const acknowledgeDeadlineMs = Date.parse(activation.acknowledgeDeadlineAt);
      if (
        !activationEvidence.acknowledgedAt &&
        Number.isFinite(acknowledgeDeadlineMs) &&
        context.reconciliationTsMs >= acknowledgeDeadlineMs
      ) {
        queueTaskNote(
          task.id,
          `Activation SLA miss: assigned agent ${assignedAgent} has not acknowledged the task in the ledger within ${Math.floor(activation.acknowledgeWithinMs / 60_000)} minutes. Expected explicit task context, blocked state, or deferred progress before ${activation.acknowledgeDeadlineAt}. This is 1 of 3 missed activation checkpoints for the current assignment cycle. Escalate ownership after 2 missed checkpoints; reassign through the ledger after 3 if the task is still silent.`,
          buildReconcileIdempotencyKey("activation-ack-missed", [
            activationCycleId,
            activation.acknowledgeDeadlineAt,
          ]),
          activation.acknowledgeDeadlineAt,
        );
      }

      const laneDeadlineMs = Date.parse(activation.laneDeadlineAt);
      if (
        !activationEvidence.lanePinnedAt &&
        Number.isFinite(laneDeadlineMs) &&
        context.reconciliationTsMs >= laneDeadlineMs &&
        !activationEvidence.startDisposition
      ) {
        queueTaskNote(
          task.id,
          `Activation SLA miss: assigned agent ${assignedAgent} has not pinned a lane for this task within ${Math.floor(activation.laneWithinMs / 60_000)} minutes. Expected a heartbeat with lane context before ${activation.laneDeadlineAt}. This is 2 of 3 missed activation checkpoints for the current assignment cycle. Escalate ownership now. If the final start checkpoint is also missed, reassign through the ledger by updating assignedAgent and requiring the gaining owner to heartbeat currentTaskId ${task.id}.`,
          buildReconcileIdempotencyKey("activation-lane-missed", [
            activationCycleId,
            activation.laneDeadlineAt,
          ]),
          activation.laneDeadlineAt,
        );
      }

      const startDeadlineMs = Date.parse(activation.startDeadlineAt);
      if (
        !activationEvidence.startedAt &&
        !activationEvidence.startDisposition &&
        Number.isFinite(startDeadlineMs) &&
        context.reconciliationTsMs >= startDeadlineMs
      ) {
        queueTaskNote(
          task.id,
          `Activation SLA miss: assigned agent ${assignedAgent} did not show explicit start proof within ${Math.floor(activation.startWithinMs / 60_000)} minutes. Expected a run-start milestone, in-progress transition, or explicit blocked/deferred state before ${activation.startDeadlineAt}. This is 3 of 3 missed activation checkpoints for the current assignment cycle. Reassignment is now appropriate: update assignedAgent through the ledger (or clear stale ownership), then require the gaining owner to heartbeat currentTaskId ${task.id}. Mission Control remains a control surface only.`,
          buildReconcileIdempotencyKey("activation-start-missed", [
            activationCycleId,
            activation.startDeadlineAt,
          ]),
          activation.startDeadlineAt,
        );
      }
    }

    if (
      task.state === "in_progress" &&
      proofCheckpointEvidence &&
      proofCheckpointEvidence.statusOnlyUpdateCount >= PROOF_CHECKPOINT_REQUIRED_STATUS_NOTES &&
      !proofCheckpointEvidence.prompt
    ) {
      queueTaskNote(
        task.id,
        `Proof checkpoint required: task is still in progress, but the latest ${proofCheckpointEvidence.statusOnlyUpdateCount} agent updates are status-only with no concrete proof of work. Record a proof checkpoint with files touched, diff summary, tests run, or review signal before sending another status-only update. Escalate ownership at ${OWNERSHIP_STATUS_ONLY_UPDATES_TO_ESCALATE} consecutive status-only updates and reassign through the ledger at ${OWNERSHIP_STATUS_ONLY_UPDATES_TO_REASSIGN} if no proof is recorded.`,
        buildReconcileIdempotencyKey("proof-checkpoint-required", [
          task.id,
          trimToUndefined(task.assignedAgent) ?? "unassigned",
          proofCheckpointEvidence.lastCheckpointAt ?? proofCheckpointEvidence.currentCycleStartedAt,
        ]),
        proofCheckpointEvidence.lastStatusNoteAt,
      );
    }

    if (
      task.state === "in_progress" &&
      proofCheckpointEvidence &&
      proofCheckpointEvidence.statusOnlyUpdateCount >= OWNERSHIP_STATUS_ONLY_UPDATES_TO_ESCALATE
    ) {
      queueTaskNote(
        task.id,
        `Ownership escalation: task is still in progress with ${proofCheckpointEvidence.statusOnlyUpdateCount} consecutive status-only updates and no proof checkpoint. Escalate the current owner now. If one more status-only update lands without proof, reassign through the ledger by updating assignedAgent and requiring the gaining owner to heartbeat currentTaskId ${task.id}.`,
        buildReconcileIdempotencyKey("proof-checkpoint-escalated", [
          task.id,
          trimToUndefined(task.assignedAgent) ?? "unassigned",
          proofCheckpointEvidence.lastCheckpointAt ?? proofCheckpointEvidence.currentCycleStartedAt,
        ]),
        proofCheckpointEvidence.lastStatusNoteAt,
      );
    }

    if (
      task.state === "in_progress" &&
      proofCheckpointEvidence &&
      proofCheckpointEvidence.statusOnlyUpdateCount >= OWNERSHIP_STATUS_ONLY_UPDATES_TO_REASSIGN
    ) {
      queueTaskNote(
        task.id,
        `Ownership reassignment ready: task is still in progress with ${proofCheckpointEvidence.statusOnlyUpdateCount} consecutive status-only updates and no proof checkpoint. Reassign through the ledger by updating assignedAgent (or clearing stale ownership), then require the gaining owner to heartbeat currentTaskId ${task.id}. Mission Control remains a control surface only.`,
        buildReconcileIdempotencyKey("proof-checkpoint-reassign", [
          task.id,
          trimToUndefined(task.assignedAgent) ?? "unassigned",
          proofCheckpointEvidence.lastCheckpointAt ?? proofCheckpointEvidence.currentCycleStartedAt,
        ]),
        proofCheckpointEvidence.lastStatusNoteAt,
      );
    }
  }

  for (const agent of materialized.agents.values()) {
    const currentTaskId = trimToUndefined(agent.currentTaskId);
    if (!currentTaskId) {
      continue;
    }
    const task = materialized.tasks.get(currentTaskId);
    const agentRecord = context.lastAgentRecordById.get(agent.id);
    if (!task || !agentRecord) {
      continue;
    }

    const heartbeatAtMs = Date.parse(agentRecord.ts);
    if (!isFreshAgentHeartbeat(heartbeatAtMs, context.reconciliationTsMs)) {
      continue;
    }

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
          ? `Reconcile mismatch: agent ${agent.id} heartbeat claims this task as current work, but the task is assigned to ${assignedAgent}. If ${agent.id} is the real owner, reassign through the ledger by updating assignedAgent from ${assignedAgent} to ${agent.id}; otherwise clear ${agent.id}'s heartbeat claim. Mission Control remains a control surface only.`
          : `Reconcile mismatch: agent ${agent.id} heartbeat claims this task as current work, but the task is currently unassigned. If ${agent.id} is the real owner, assign it through the ledger before takeover; otherwise clear ${agent.id}'s heartbeat claim. Mission Control remains a control surface only.`,
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
    const blockedRecord = findLatestTaskRecord(
      materialized.appliedRecords,
      task.id,
      (record) => record.kind !== "note",
    );
    const blockedTsMs = blockedRecord ? Date.parse(blockedRecord.ts) : Date.parse(task.lastEventAt);
    const activeWork = context.activeWorkByAgentId.get(assignedAgent);
    if (!activeWork || activeWork.taskId === task.id || !Number.isFinite(blockedTsMs)) {
      continue;
    }
    if (activeWork.tsMs <= blockedTsMs) {
      continue;
    }
    queueTaskNote(
      task.id,
      `Reconcile residue: blocked task still belongs to ${assignedAgent}, but newer active work exists on ${activeWork.taskId}. Reassignment is appropriate if ${task.id} still needs work: publish the ownership change through the ledger before takeover, then require the gaining owner to heartbeat currentTaskId ${task.id}. Mission Control remains a control surface only.`,
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
  const proofCheckpoint = parsePersistedTaskProofCheckpoint(value.proofCheckpoint);
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
    ...(proofCheckpoint ? { proofCheckpoint } : {}),
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
  const next = buildTaskWithActivationMetadata(existing, patch, ts, title);
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
  const next = buildTaskWithActivationMetadata(
    existing,
    { ...patch, state: input.state },
    ts,
    title,
  );
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
      ...(metadataChanged(existing, next, patch) ? { metadata: next.metadata } : {}),
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
  const proofCheckpoint =
    input.proofCheckpoint === undefined
      ? undefined
      : normalizeTaskProofCheckpointInput(input.proofCheckpoint);
  const next = buildTaskWithActivationMetadata(
    existing,
    {
      ...patch,
      ...(toState ? { state: toState } : {}),
      ...(input.kind === "blocked" && !trimToUndefined(patch.blockedReason)
        ? { blockedReason: trimToUndefined(input.summary) }
        : {}),
    },
    ts,
    title,
  );
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
      ...(metadataChanged(existing, next, patch) ? { metadata: next.metadata } : {}),
    },
    ...(proofCheckpoint ? { proofCheckpoint } : {}),
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

function normalizeDependencyStatus(value: unknown): "ok" | "timeout" | "error" | "skipped" {
  if (value === undefined) {
    return "ok";
  }

  if (value === "ok" || value === "timeout" || value === "error" || value === "skipped") {
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
    ran: input.ran,
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

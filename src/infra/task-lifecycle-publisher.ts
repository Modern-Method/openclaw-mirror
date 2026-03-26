import {
  publishTaskLedgerEvents,
  type TaskLedgerActor,
  type TaskLedgerPublishResult,
  type TaskLedgerTask,
} from "./task-ledger.js";

export type TaskLifecycleAction = "start" | "block" | "note" | "qa" | "done";

export type TaskLifecyclePublishInput = {
  action: TaskLifecycleAction;
  taskId: string;
  summary: string;
  actor?: Partial<TaskLedgerActor>;
  ts?: string;
  idempotencyKey?: string;
  recentEventLimit?: number;
  stateDir?: string;
  task?: Partial<Omit<TaskLedgerTask, "id" | "title" | "lastEventAt">> & {
    title?: string;
  };
  blockedReason?: string;
};

function trimToUndefined(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function normalizeTaskPatch(
  task: TaskLifecyclePublishInput["task"],
  blockedReason?: string,
): TaskLifecyclePublishInput["task"] | undefined {
  if (!task && !blockedReason) {
    return undefined;
  }
  const patch = { ...task };
  const nextBlockedReason = trimToUndefined(blockedReason);
  if (nextBlockedReason !== undefined) {
    patch.blockedReason = nextBlockedReason;
  }
  return patch;
}

export function buildTaskLifecyclePublishInput(params: TaskLifecyclePublishInput) {
  const taskPatch = normalizeTaskPatch(
    params.task,
    params.action === "block" ? (params.blockedReason ?? params.summary) : params.blockedReason,
  );

  switch (params.action) {
    case "start":
      return {
        entity: "task" as const,
        kind: "transition" as const,
        taskId: params.taskId,
        state: "in_progress" as const,
        summary: params.summary,
        ...(params.actor ? { actor: params.actor } : {}),
        ...(params.ts ? { ts: params.ts } : {}),
        ...(taskPatch ? { task: taskPatch } : {}),
        ...(params.idempotencyKey ? { idempotencyKey: params.idempotencyKey } : {}),
      };
    case "block":
      return {
        entity: "task" as const,
        kind: "transition" as const,
        taskId: params.taskId,
        summary: params.summary,
        state: "blocked" as const,
        ...(params.actor ? { actor: params.actor } : {}),
        ...(params.ts ? { ts: params.ts } : {}),
        ...(taskPatch ? { task: taskPatch } : {}),
        ...(params.idempotencyKey ? { idempotencyKey: params.idempotencyKey } : {}),
      };
    case "note":
      return {
        entity: "task" as const,
        kind: "note" as const,
        taskId: params.taskId,
        summary: params.summary,
        ...(params.actor ? { actor: params.actor } : {}),
        ...(params.ts ? { ts: params.ts } : {}),
        ...(taskPatch ? { task: taskPatch } : {}),
        ...(params.idempotencyKey ? { idempotencyKey: params.idempotencyKey } : {}),
      };
    case "qa":
      return {
        entity: "task" as const,
        kind: "transition" as const,
        taskId: params.taskId,
        state: "qa" as const,
        summary: params.summary,
        ...(params.actor ? { actor: params.actor } : {}),
        ...(params.ts ? { ts: params.ts } : {}),
        ...(taskPatch ? { task: taskPatch } : {}),
        ...(params.idempotencyKey ? { idempotencyKey: params.idempotencyKey } : {}),
      };
    case "done":
      return {
        entity: "task" as const,
        kind: "transition" as const,
        taskId: params.taskId,
        state: "done" as const,
        summary: params.summary,
        ...(params.actor ? { actor: params.actor } : {}),
        ...(params.ts ? { ts: params.ts } : {}),
        ...(taskPatch ? { task: taskPatch } : {}),
        ...(params.idempotencyKey ? { idempotencyKey: params.idempotencyKey } : {}),
      };
  }
}

export async function publishTaskLifecycleEvent(
  params: TaskLifecyclePublishInput,
): Promise<TaskLedgerPublishResult> {
  return await publishTaskLedgerEvents({
    events: [buildTaskLifecyclePublishInput(params)],
    ...(params.stateDir ? { stateDir: params.stateDir } : {}),
    ...(params.recentEventLimit ? { recentEventLimit: params.recentEventLimit } : {}),
  });
}

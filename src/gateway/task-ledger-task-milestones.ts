import { resolveAgentIdFromSessionKey } from "../config/sessions.js";
import {
  getAgentRunContext,
  type AgentEventPayload,
  type AgentRunContext,
} from "../infra/agent-events.js";
import type { TaskProofCheckpoint } from "../infra/task-ledger.js";
import {
  publishTaskLifecycleEvent,
  type TaskLifecyclePublishInput,
} from "../infra/task-lifecycle-publisher.js";

const TASK_LEDGER_TOPIC = "tasks.ledger";
const TASK_MILESTONE_ACTOR = {
  type: "system" as const,
  id: "task-milestone-updater",
  name: "Task milestone updater",
};

function trimToUndefined(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function normalizeLifecyclePhase(value: unknown): string | undefined {
  return trimToUndefined(value)?.toLowerCase();
}

function formatModelRef(provider: unknown, model: unknown): string | undefined {
  const normalizedModel = trimToUndefined(model);
  if (!normalizedModel) {
    return undefined;
  }
  const normalizedProvider = trimToUndefined(provider);
  if (!normalizedProvider) {
    return normalizedModel;
  }
  if (normalizedModel.startsWith(`${normalizedProvider}/`)) {
    return normalizedModel;
  }
  return `${normalizedProvider}/${normalizedModel}`;
}

function buildStartSummary(runContext: AgentRunContext): string {
  const details: string[] = [];
  const lane = trimToUndefined(runContext.lane);
  const branch = trimToUndefined(runContext.branch);

  if (lane) {
    details.push(`lane ${lane}`);
  }
  if (branch) {
    details.push(`branch ${branch}`);
  }

  if (details.length === 0) {
    return "Milestone update: active implementation work started.";
  }
  return `Milestone update: active implementation work started in ${details.join(" on ")}.`;
}

function buildRepeatedFailureSummary(error: unknown): string {
  const safeError = trimToUndefined(error);
  if (!safeError) {
    return "Milestone update: the active run hit repeated failures and needs attention.";
  }
  return `Milestone update: the active run hit repeated failures and needs attention. Latest error: ${safeError}`;
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

function normalizeProofCheckpoint(data: Record<string, unknown>): TaskProofCheckpoint | undefined {
  const files = normalizeStringList(data.files);
  const diffSummary = trimToUndefined(data.diffSummary);
  const tests = normalizeStringList(data.tests);
  const reviewSignal = trimToUndefined(data.reviewSignal ?? data.review);
  if (!files && !diffSummary && !tests && !reviewSignal) {
    return undefined;
  }
  return {
    ...(files ? { files } : {}),
    ...(diffSummary ? { diffSummary } : {}),
    ...(tests ? { tests } : {}),
    ...(reviewSignal ? { reviewSignal } : {}),
  };
}

function buildProofCheckpointSummary(proofCheckpoint: TaskProofCheckpoint): string {
  const details: string[] = [];
  if ((proofCheckpoint.files?.length ?? 0) > 0) {
    details.push(`files ${proofCheckpoint.files?.join(", ")}`);
  }
  if (proofCheckpoint.diffSummary) {
    details.push(`diff ${proofCheckpoint.diffSummary}`);
  }
  if ((proofCheckpoint.tests?.length ?? 0) > 0) {
    details.push(`tests ${proofCheckpoint.tests?.join(", ")}`);
  }
  if (proofCheckpoint.reviewSignal) {
    details.push(`review ${proofCheckpoint.reviewSignal}`);
  }
  return `Milestone update: proof checkpoint captured with ${details.join("; ")}.`;
}

function resolveLifecycleMilestone(params: {
  evt: AgentEventPayload;
  runContext: AgentRunContext;
  taskId: string;
}): {
  kind: string;
  summary: string;
  idempotencyKey: string;
  proofCheckpoint?: TaskProofCheckpoint;
} | null {
  const { evt, runContext, taskId } = params;
  const phase = normalizeLifecyclePhase(evt.data?.phase);
  const terminalState = trimToUndefined(evt.data?.terminalState);
  const sessionKey = trimToUndefined(evt.sessionKey ?? runContext.sessionKey);
  const agentId = sessionKey ? resolveAgentIdFromSessionKey(sessionKey) : undefined;

  switch (phase) {
    case "start":
      return {
        kind: "run_started",
        summary: buildStartSummary(runContext),
        idempotencyKey: `task-milestone:run-started:${taskId}:${evt.runId}:${sessionKey ?? agentId ?? "unknown"}`,
      };
    case "fallback": {
      const model = formatModelRef(evt.data?.activeProvider, evt.data?.activeModel);
      const reason = trimToUndefined(evt.data?.reasonSummary);
      const summary =
        model && reason
          ? `Milestone update: the active run switched to fallback model ${model} (${reason}).`
          : model
            ? `Milestone update: the active run switched to fallback model ${model}.`
            : reason
              ? `Milestone update: the active run switched to a fallback model (${reason}).`
              : "Milestone update: the active run switched to a fallback model.";
      return {
        kind: "fallback",
        summary,
        idempotencyKey: `task-milestone:fallback:${taskId}:${evt.runId}:${model ?? "unknown"}:${reason ?? "none"}`,
      };
    }
    case "proof_checkpoint": {
      const proofCheckpoint = normalizeProofCheckpoint(evt.data);
      if (!proofCheckpoint) {
        return null;
      }
      return {
        kind: "proof_checkpoint",
        summary: buildProofCheckpointSummary(proofCheckpoint),
        proofCheckpoint,
        idempotencyKey: `task-milestone:proof-checkpoint:${taskId}:${evt.runId}:${evt.seq}`,
      };
    }
    case "end":
    case "error":
      switch (terminalState) {
        case "blocked_by_input":
          return {
            kind: "waiting_for_input",
            summary:
              "Milestone update: the active run is waiting for user input before it can continue.",
            idempotencyKey: `task-milestone:waiting-for-input:${taskId}:${evt.runId}`,
          };
        case "unsafe_to_proceed":
          return {
            kind: "unsafe_to_proceed",
            summary:
              "Milestone update: the active run stopped because the next step is unsafe without operator approval.",
            idempotencyKey: `task-milestone:unsafe-to-proceed:${taskId}:${evt.runId}`,
          };
        case "repeated_failure":
          return {
            kind: "repeated_failure",
            summary: buildRepeatedFailureSummary(evt.data?.error),
            idempotencyKey: `task-milestone:repeated-failure:${taskId}:${evt.runId}`,
          };
        default:
          return null;
      }
    default:
      return null;
  }
}

export function buildTaskLifecycleMilestoneUpdate(
  evt: AgentEventPayload,
  options?: {
    resolveRunContext?: (runId: string) => AgentRunContext | undefined;
  },
): TaskLifecyclePublishInput | null {
  if (evt.stream !== "lifecycle") {
    return null;
  }

  const runContext =
    evt.runContext ?? options?.resolveRunContext?.(evt.runId) ?? getAgentRunContext(evt.runId);
  if (!runContext || runContext.isHeartbeat) {
    return null;
  }

  const taskId = trimToUndefined(runContext.currentTaskId);
  if (!taskId) {
    return null;
  }

  const milestone = resolveLifecycleMilestone({ evt, runContext, taskId });
  if (!milestone) {
    return null;
  }

  const tsValue = new Date(evt.ts);
  if (Number.isNaN(tsValue.getTime())) {
    return null;
  }

  return {
    action: "note",
    taskId,
    summary: milestone.summary,
    actor: TASK_MILESTONE_ACTOR,
    ts: tsValue.toISOString(),
    ...(milestone.proofCheckpoint ? { proofCheckpoint: milestone.proofCheckpoint } : {}),
    idempotencyKey: milestone.idempotencyKey,
  };
}

export function createTaskLedgerTaskMilestoneListener(params: {
  broadcast: (topic: string, payload: unknown, options: { dropIfSlow: boolean }) => void;
  publish?: typeof publishTaskLifecycleEvent;
  resolveRunContext?: (runId: string) => AgentRunContext | undefined;
  onError?: (error: unknown) => void;
}) {
  const publish = params.publish ?? publishTaskLifecycleEvent;
  return (evt: AgentEventPayload) => {
    const update = buildTaskLifecycleMilestoneUpdate(evt, {
      resolveRunContext: params.resolveRunContext,
    });
    if (!update) {
      return;
    }

    void publish(update)
      .then((result) => {
        for (const event of result.events) {
          params.broadcast(TASK_LEDGER_TOPIC, event, { dropIfSlow: true });
        }
      })
      .catch((error) => {
        params.onError?.(error);
      });
  };
}

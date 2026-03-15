import { resolveAgentIdFromSessionKey } from "../config/sessions.js";
import type { AgentEventPayload } from "../infra/agent-events.js";
import {
  publishTaskLedgerEvents,
  type AgentActivityStatus,
  type TaskLedgerAgentHeartbeatInput,
} from "../infra/task-ledger.js";

const TASK_LEDGER_TOPIC = "tasks.ledger";

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

function mapLifecyclePhaseToStatus(evt: AgentEventPayload): AgentActivityStatus | null {
  const phase = normalizeLifecyclePhase(evt.data?.phase);
  switch (phase) {
    case "start":
    case "fallback":
    case "fallback_cleared":
      return "running";
    case "end":
      return "idle";
    case "error":
      return "blocked";
    default:
      return null;
  }
}

function buildLifecycleSummary(evt: AgentEventPayload): string | null {
  const phase = normalizeLifecyclePhase(evt.data?.phase);
  switch (phase) {
    case "start":
      return "Run started";
    case "end":
      return evt.data?.aborted ? "Run timed out" : "Run finished";
    case "error":
      return trimToUndefined(evt.data?.error) ?? "Run failed";
    case "fallback": {
      const model = formatModelRef(evt.data?.activeProvider, evt.data?.activeModel);
      const reason = trimToUndefined(evt.data?.reasonSummary);
      if (model && reason) {
        return `Using fallback model ${model} (${reason})`;
      }
      if (model) {
        return `Using fallback model ${model}`;
      }
      return reason ? `Using fallback model (${reason})` : "Using fallback model";
    }
    case "fallback_cleared": {
      const selectedModel = formatModelRef(evt.data?.selectedProvider, evt.data?.selectedModel);
      return selectedModel
        ? `Returned to selected model ${selectedModel}`
        : "Returned to selected model";
    }
    default:
      return null;
  }
}

function buildLifecycleIdempotencyKey(evt: AgentEventPayload, agentId: string): string | undefined {
  const phase = normalizeLifecyclePhase(evt.data?.phase);
  if (!phase) {
    return undefined;
  }
  const keyPayload = {
    agentId,
    runId: evt.runId,
    phase,
    aborted: evt.data?.aborted === true,
    error: trimToUndefined(evt.data?.error),
    reasonSummary: trimToUndefined(evt.data?.reasonSummary),
    activeModel: formatModelRef(evt.data?.activeProvider, evt.data?.activeModel),
    selectedModel: formatModelRef(evt.data?.selectedProvider, evt.data?.selectedModel),
  };
  return `agent-lifecycle:${JSON.stringify(keyPayload)}`;
}

export function buildTaskLedgerAgentHeartbeatFromLifecycleEvent(
  evt: AgentEventPayload,
  options?: { resolveAgentId?: (sessionKey: string) => string | undefined },
): TaskLedgerAgentHeartbeatInput | null {
  if (evt.stream !== "lifecycle") {
    return null;
  }
  const sessionKey = trimToUndefined(evt.sessionKey);
  if (!sessionKey) {
    return null;
  }
  const agentId = options?.resolveAgentId?.(sessionKey) ?? resolveAgentIdFromSessionKey(sessionKey);
  if (!agentId) {
    return null;
  }
  const status = mapLifecyclePhaseToStatus(evt);
  const summary = buildLifecycleSummary(evt);
  if (!status || !summary) {
    return null;
  }
  return {
    entity: "agent",
    kind: "heartbeat",
    agent: {
      id: agentId,
      name: agentId,
      status,
      sessionKey,
      summary,
      metadata: {
        runId: evt.runId,
        stream: evt.stream,
        seq: evt.seq,
        phase: normalizeLifecyclePhase(evt.data?.phase),
        ...(formatModelRef(evt.data?.activeProvider, evt.data?.activeModel)
          ? {
              activeModel: formatModelRef(evt.data?.activeProvider, evt.data?.activeModel),
            }
          : {}),
        ...(formatModelRef(evt.data?.selectedProvider, evt.data?.selectedModel)
          ? {
              selectedModel: formatModelRef(evt.data?.selectedProvider, evt.data?.selectedModel),
            }
          : {}),
        ...(trimToUndefined(evt.data?.reasonSummary)
          ? { reasonSummary: trimToUndefined(evt.data?.reasonSummary) }
          : {}),
      },
    },
    ts: new Date(evt.ts).toISOString(),
    idempotencyKey: buildLifecycleIdempotencyKey(evt, agentId),
  };
}

export function createTaskLedgerAgentActivityListener(params: {
  broadcast: (topic: string, payload: unknown, options: { dropIfSlow: boolean }) => void;
  publish?: typeof publishTaskLedgerEvents;
  resolveAgentId?: (sessionKey: string) => string | undefined;
  onError?: (error: unknown) => void;
}) {
  const publish = params.publish ?? publishTaskLedgerEvents;
  return (evt: AgentEventPayload) => {
    const heartbeat = buildTaskLedgerAgentHeartbeatFromLifecycleEvent(evt, {
      resolveAgentId: params.resolveAgentId,
    });
    if (!heartbeat) {
      return;
    }
    void publish({ events: [heartbeat] })
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

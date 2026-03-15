import { describe, expect, it, vi } from "vitest";
import type { AgentEventPayload } from "../infra/agent-events.js";
import type { TaskLedgerAgentRecord } from "../infra/task-ledger.js";
import {
  buildTaskLedgerAgentHeartbeatFromLifecycleEvent,
  createTaskLedgerAgentActivityListener,
} from "./task-ledger-agent-activity.js";

function makeLifecycleEvent(
  phase: string,
  data?: Record<string, unknown>,
  sessionKey = "agent:forge:main",
): AgentEventPayload {
  return {
    runId: "run-1",
    seq: 7,
    stream: "lifecycle",
    ts: Date.parse("2026-03-15T05:10:00.000Z"),
    sessionKey,
    data: { phase, ...data },
  };
}

describe("task-ledger agent activity", () => {
  it("maps fallback lifecycle phases to running summaries instead of blocked/error state", () => {
    const fallback = buildTaskLedgerAgentHeartbeatFromLifecycleEvent(
      makeLifecycleEvent("fallback", {
        activeProvider: "deepinfra",
        activeModel: "moonshotai/Kimi-K2.5",
        reasonSummary: "rate limit",
      }),
      {
        resolveAgentId: () => "forge",
      },
    );
    const cleared = buildTaskLedgerAgentHeartbeatFromLifecycleEvent(
      makeLifecycleEvent("fallback_cleared", {
        selectedProvider: "openai",
        selectedModel: "gpt-5.4",
      }),
      {
        resolveAgentId: () => "forge",
      },
    );

    expect(fallback).toMatchObject({
      entity: "agent",
      kind: "heartbeat",
      agent: {
        id: "forge",
        status: "running",
        summary: "Using fallback model deepinfra/moonshotai/Kimi-K2.5 (rate limit)",
      },
    });
    expect(cleared).toMatchObject({
      agent: {
        id: "forge",
        status: "running",
        summary: "Returned to selected model openai/gpt-5.4",
      },
    });
  });

  it("broadcasts accepted auto-ingested lifecycle updates on tasks.ledger", async () => {
    const broadcast = vi.fn();
    const publishedEvent: TaskLedgerAgentRecord = {
      schema: "openclaw.task-ledger.event.v1",
      id: "evt-1",
      ts: "2026-03-15T05:10:00.000Z",
      entity: "agent",
      kind: "heartbeat",
      agentId: "forge",
      status: "running",
      sessionKey: "agent:forge:main",
      summary: "Run started",
      metadata: { runId: "run-1" },
    };
    const publish = vi.fn().mockResolvedValue({
      accepted: 1,
      events: [publishedEvent],
      snapshot: {} as never,
    });

    const listener = createTaskLedgerAgentActivityListener({
      broadcast,
      publish,
      resolveAgentId: () => "forge",
    });

    listener(makeLifecycleEvent("start"));
    await Promise.resolve();
    await Promise.resolve();

    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({
        events: [
          expect.objectContaining({
            entity: "agent",
            kind: "heartbeat",
            agent: expect.objectContaining({
              id: "forge",
              status: "running",
              summary: "Run started",
            }),
          }),
        ],
      }),
    );
    expect(broadcast).toHaveBeenCalledWith("tasks.ledger", publishedEvent, { dropIfSlow: true });
  });

  it("ignores unsupported lifecycle phases instead of marking agents blocked", async () => {
    const broadcast = vi.fn();
    const publish = vi.fn().mockResolvedValue({ accepted: 0, events: [], snapshot: {} as never });
    const listener = createTaskLedgerAgentActivityListener({
      broadcast,
      publish,
      resolveAgentId: () => "forge",
    });

    listener(makeLifecycleEvent("catalog"));
    await Promise.resolve();

    expect(publish).not.toHaveBeenCalled();
    expect(broadcast).not.toHaveBeenCalled();
  });
});

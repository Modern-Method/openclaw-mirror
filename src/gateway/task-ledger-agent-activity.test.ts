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
  runContext?: AgentEventPayload["runContext"],
): AgentEventPayload {
  return {
    runId: "run-1",
    seq: 7,
    stream: "lifecycle",
    ts: Date.parse("2026-03-15T05:10:00.000Z"),
    sessionKey,
    ...(runContext ? { runContext } : {}),
    data: { phase, ...data },
  };
}

describe("task-ledger agent activity", () => {
  it("maps lifecycle events to the explicit heartbeat contract", () => {
    const fallback = buildTaskLedgerAgentHeartbeatFromLifecycleEvent(
      makeLifecycleEvent("fallback", {
        activeProvider: "deepinfra",
        activeModel: "moonshotai/Kimi-K2.5",
        reasonSummary: "rate limit",
      }),
      {
        resolveAgentId: () => "forge",
        resolveRunContext: () => ({
          lane: "pinned",
          currentTaskId: "task-42",
          worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
          branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
        }),
      },
    );
    const cleared = buildTaskLedgerAgentHeartbeatFromLifecycleEvent(
      makeLifecycleEvent("fallback_cleared", {
        selectedProvider: "openai",
        selectedModel: "gpt-5.4",
      }),
      {
        resolveAgentId: () => "forge",
        resolveRunContext: () => ({
          lane: "pinned",
          currentTaskId: "task-42",
          worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
          branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
        }),
      },
    );

    expect(fallback).toMatchObject({
      entity: "agent",
      kind: "heartbeat",
      agent: {
        id: "forge",
        status: "running",
        lane: "pinned",
        currentTaskId: "task-42",
        sessionKey: "agent:forge:main",
        worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
        branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
        summary: "Using fallback model deepinfra/moonshotai/Kimi-K2.5 (rate limit)",
      },
    });
    expect(cleared).toMatchObject({
      agent: {
        id: "forge",
        status: "running",
        lane: "pinned",
        currentTaskId: "task-42",
        sessionKey: "agent:forge:main",
        worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
        branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
        summary: "Returned to selected model openai/gpt-5.4",
      },
    });
  });

  it("emits explicit clears for heartbeat contract fields when run context drops them", () => {
    const heartbeat = buildTaskLedgerAgentHeartbeatFromLifecycleEvent(makeLifecycleEvent("end"), {
      resolveAgentId: () => "forge",
      resolveRunContext: () => ({
        lane: undefined,
        currentTaskId: undefined,
        worktree: undefined,
        branch: undefined,
      }),
    });

    expect(heartbeat).toMatchObject({
      entity: "agent",
      kind: "heartbeat",
      agent: {
        id: "forge",
        status: "idle",
        lane: undefined,
        currentTaskId: undefined,
        sessionKey: "agent:forge:main",
        worktree: undefined,
        branch: undefined,
      },
    });
    expect(Object.keys(heartbeat?.agent ?? {})).toEqual(
      expect.arrayContaining(["lane", "currentTaskId", "worktree", "branch"]),
    );
  });

  it("prefers terminal event runContext snapshots so end heartbeats can clear stale task context", () => {
    const heartbeat = buildTaskLedgerAgentHeartbeatFromLifecycleEvent(
      makeLifecycleEvent("end", undefined, "agent:forge:main", {
        sessionKey: "agent:forge:main",
        lane: undefined,
        currentTaskId: undefined,
        worktree: undefined,
        branch: undefined,
      }),
      {
        resolveAgentId: () => "forge",
        resolveRunContext: () => ({
          lane: "stale-lane",
          currentTaskId: "task-stale",
          worktree: "/tmp/stale-worktree",
          branch: "stale-branch",
        }),
      },
    );

    expect(heartbeat).toMatchObject({
      agent: {
        id: "forge",
        status: "idle",
        lane: undefined,
        currentTaskId: undefined,
        sessionKey: "agent:forge:main",
        worktree: undefined,
        branch: undefined,
      },
    });
  });

  it("uses run context sessionKey when event sessionKey is omitted", () => {
    const heartbeat = buildTaskLedgerAgentHeartbeatFromLifecycleEvent(
      {
        runId: "run-1",
        seq: 3,
        stream: "lifecycle",
        ts: Date.parse("2026-03-15T05:10:00.000Z"),
        data: { phase: "start" },
        runContext: {
          sessionKey: "agent:forge:main",
          lane: "headless",
          currentTaskId: "task-99",
        },
      },
      {
        resolveAgentId: () => "forge",
        resolveRunContext: () => ({
          lane: "headless",
          currentTaskId: "task-99",
          sessionKey: "agent:forge:main",
        }),
      },
    );

    expect(heartbeat).toMatchObject({
      agent: {
        id: "forge",
        status: "running",
        lane: "headless",
        currentTaskId: "task-99",
        sessionKey: "agent:forge:main",
      },
    });
  });

  it("omits explicit clear fields when no run context source exists", () => {
    const heartbeat = buildTaskLedgerAgentHeartbeatFromLifecycleEvent(
      makeLifecycleEvent("fallback"),
      {
        resolveAgentId: () => "forge",
        resolveRunContext: () => undefined,
      },
    );

    expect(heartbeat).toMatchObject({
      agent: {
        id: "forge",
        status: "running",
        sessionKey: "agent:forge:main",
      },
    });
    expect(Object.keys(heartbeat?.agent ?? {})).not.toEqual(
      expect.arrayContaining(["lane", "currentTaskId", "worktree", "branch"]),
    );
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
      lane: "pinned",
      currentTaskId: "task-42",
      sessionKey: "agent:forge:main",
      worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
      branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
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
      resolveRunContext: () => ({
        lane: "pinned",
        currentTaskId: "task-42",
        worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
        branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
      }),
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
              lane: "pinned",
              currentTaskId: "task-42",
              sessionKey: "agent:forge:main",
              worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
              branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
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

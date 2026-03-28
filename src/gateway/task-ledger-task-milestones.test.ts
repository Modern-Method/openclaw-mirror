import { describe, expect, it, vi } from "vitest";
import type { AgentEventPayload } from "../infra/agent-events.js";
import type { TaskLedgerTaskRecord } from "../infra/task-ledger.js";
import {
  buildTaskLifecycleMilestoneUpdate,
  createTaskLedgerTaskMilestoneListener,
} from "./task-ledger-task-milestones.js";

function makeLifecycleEvent(
  phase: string,
  data?: Record<string, unknown>,
  runContext?: AgentEventPayload["runContext"],
): AgentEventPayload {
  return {
    runId: "run-1",
    seq: 7,
    stream: "lifecycle",
    ts: Date.parse("2026-03-28T03:15:00.000Z"),
    sessionKey: "agent:forge:main",
    ...(runContext ? { runContext } : {}),
    data: { phase, ...data },
  };
}

describe("task-ledger task milestones", () => {
  it("publishes a run-start milestone once per task/session context", () => {
    const update = buildTaskLifecycleMilestoneUpdate(
      makeLifecycleEvent("start", undefined, {
        sessionKey: "agent:forge:main",
        currentTaskId: "task-42",
        lane: "pinned",
        branch: "feat/milestone-update-rules",
      }),
    );

    expect(update).toMatchObject({
      action: "note",
      taskId: "task-42",
      summary:
        "Milestone update: active implementation work started in lane pinned on branch feat/milestone-update-rules.",
      actor: {
        type: "system",
        id: "task-milestone-updater",
      },
      idempotencyKey: "task-milestone:run-started:task-42:run-1:agent:forge:main",
    });
  });

  it("publishes fallback milestones with the fallback model summary", () => {
    const update = buildTaskLifecycleMilestoneUpdate(
      makeLifecycleEvent(
        "fallback",
        {
          activeProvider: "openai",
          activeModel: "gpt-5.4-mini",
          reasonSummary: "rate limit",
        },
        {
          sessionKey: "agent:forge:main",
          currentTaskId: "task-42",
        },
      ),
    );

    expect(update).toMatchObject({
      action: "note",
      taskId: "task-42",
      summary:
        "Milestone update: the active run switched to fallback model openai/gpt-5.4-mini (rate limit).",
      idempotencyKey: "task-milestone:fallback:task-42:run-1:openai/gpt-5.4-mini:rate limit",
    });
  });

  it("publishes waiting-for-input milestones for blocked terminal states", () => {
    const update = buildTaskLifecycleMilestoneUpdate(
      makeLifecycleEvent(
        "end",
        {
          terminalState: "blocked_by_input",
        },
        {
          sessionKey: "agent:forge:main",
          currentTaskId: "task-42",
        },
      ),
    );

    expect(update).toMatchObject({
      action: "note",
      taskId: "task-42",
      summary: "Milestone update: the active run is waiting for user input before it can continue.",
      idempotencyKey: "task-milestone:waiting-for-input:task-42:run-1",
    });
  });

  it("publishes repeated-failure milestones with the latest error", () => {
    const update = buildTaskLifecycleMilestoneUpdate(
      makeLifecycleEvent(
        "error",
        {
          terminalState: "repeated_failure",
          error: "provider connection reset",
        },
        {
          sessionKey: "agent:forge:main",
          currentTaskId: "task-42",
        },
      ),
    );

    expect(update).toMatchObject({
      action: "note",
      taskId: "task-42",
      summary:
        "Milestone update: the active run hit repeated failures and needs attention. Latest error: provider connection reset",
      idempotencyKey: "task-milestone:repeated-failure:task-42:run-1",
    });
  });

  it("skips routine done terminals and heartbeat runs", () => {
    expect(
      buildTaskLifecycleMilestoneUpdate(
        makeLifecycleEvent(
          "end",
          {
            terminalState: "done",
          },
          {
            sessionKey: "agent:forge:main",
            currentTaskId: "task-42",
          },
        ),
      ),
    ).toBeNull();

    expect(
      buildTaskLifecycleMilestoneUpdate(
        makeLifecycleEvent("start", undefined, {
          sessionKey: "agent:forge:main",
          currentTaskId: "task-42",
          isHeartbeat: true,
        }),
      ),
    ).toBeNull();
  });

  it("broadcasts accepted milestone notes on tasks.ledger", async () => {
    const broadcast = vi.fn();
    const publishedEvent: TaskLedgerTaskRecord = {
      schema: "openclaw.task-ledger.event.v1",
      id: "evt-1",
      ts: "2026-03-28T03:15:00.000Z",
      entity: "task",
      kind: "note",
      taskId: "task-42",
      summary: "Milestone update: the active run is waiting for user input before it can continue.",
      actor: {
        type: "system",
        id: "task-milestone-updater",
        name: "Task milestone updater",
      },
      idempotencyKey: "task-milestone:waiting-for-input:task-42:run-1",
    };
    const publish = vi.fn().mockResolvedValue({
      accepted: 1,
      events: [publishedEvent],
      snapshot: {} as never,
    });

    const listener = createTaskLedgerTaskMilestoneListener({
      broadcast,
      publish,
    });

    listener(
      makeLifecycleEvent(
        "end",
        { terminalState: "blocked_by_input" },
        {
          sessionKey: "agent:forge:main",
          currentTaskId: "task-42",
        },
      ),
    );
    await Promise.resolve();
    await Promise.resolve();

    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "note",
        taskId: "task-42",
      }),
    );
    expect(broadcast).toHaveBeenCalledWith("tasks.ledger", publishedEvent, {
      dropIfSlow: true,
    });
  });
});

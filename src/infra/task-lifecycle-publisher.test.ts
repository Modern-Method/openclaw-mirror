import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { readTaskLedgerEvents, readTaskLedgerSnapshot } from "./task-ledger.js";
import {
  buildTaskLifecyclePublishInput,
  publishTaskLifecycleEvent,
} from "./task-lifecycle-publisher.js";

const stateDirs: string[] = [];

async function createStateDir() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-task-lifecycle-"));
  stateDirs.push(dir);
  return dir;
}

afterEach(async () => {
  await Promise.all(
    stateDirs.splice(0).map(async (dir) => await fs.rm(dir, { recursive: true, force: true })),
  );
});

describe("task lifecycle publisher", () => {
  it("maps lifecycle actions onto canonical task-ledger publish inputs", () => {
    expect(
      buildTaskLifecyclePublishInput({
        action: "start",
        taskId: "task-1",
        summary: "Started work",
        idempotencyKey: "k-start",
      }),
    ).toMatchObject({
      entity: "task",
      kind: "transition",
      taskId: "task-1",
      state: "in_progress",
      summary: "Started work",
      idempotencyKey: "k-start",
    });

    expect(
      buildTaskLifecyclePublishInput({
        action: "block",
        taskId: "task-1",
        summary: "Waiting on review",
        blockedReason: "Need Neko review",
      }),
    ).toMatchObject({
      entity: "task",
      kind: "transition",
      taskId: "task-1",
      state: "blocked",
      task: expect.objectContaining({ blockedReason: "Need Neko review" }),
    });

    expect(
      buildTaskLifecyclePublishInput({
        action: "note",
        taskId: "task-1",
        summary: "Implementation note",
        proofCheckpoint: {
          files: ["src/infra/task-ledger.ts"],
          diffSummary: "Adds proof checkpoint metadata projection.",
          tests: ["pnpm test -- src/infra/task-ledger.test.ts"],
          reviewSignal: "Requested maintainer review",
        },
      }),
    ).toMatchObject({
      entity: "task",
      kind: "note",
      taskId: "task-1",
      summary: "Implementation note",
      proofCheckpoint: {
        files: ["src/infra/task-ledger.ts"],
        diffSummary: "Adds proof checkpoint metadata projection.",
        tests: ["pnpm test -- src/infra/task-ledger.test.ts"],
        reviewSignal: "Requested maintainer review",
      },
    });

    expect(
      buildTaskLifecyclePublishInput({
        action: "qa",
        taskId: "task-1",
        summary: "Ready for QA",
      }),
    ).toMatchObject({
      entity: "task",
      kind: "transition",
      taskId: "task-1",
      state: "qa",
    });

    expect(
      buildTaskLifecyclePublishInput({
        action: "done",
        taskId: "task-1",
        summary: "Shipped",
      }),
    ).toMatchObject({
      entity: "task",
      kind: "transition",
      taskId: "task-1",
      state: "done",
    });
  });

  it("publishes start, block, note, qa, and done through the canonical ledger path with provenance", async () => {
    const stateDir = await createStateDir();

    await publishTaskLifecycleEvent({
      stateDir,
      action: "start",
      taskId: "task-1",
      summary: "Started execution",
      idempotencyKey: "task-1:start:run-1",
      actor: { type: "agent", id: "forge", name: "Forge" },
      task: {
        title: "Ship lifecycle publisher",
        sessionKey: "agent:forge:main",
        worktree: "/tmp/openclaw-p0-1-lifecycle-publisher-20260325",
        assignedAgent: "forge",
        source: "openclaw-runtime",
        metadata: { runId: "run-1", publisher: "task-lifecycle-publisher" },
      },
    });

    await publishTaskLifecycleEvent({
      stateDir,
      action: "note",
      taskId: "task-1",
      summary: "Publisher seam wired",
      idempotencyKey: "task-1:note:run-1",
      actor: { type: "agent", id: "forge" },
    });

    await publishTaskLifecycleEvent({
      stateDir,
      action: "block",
      taskId: "task-1",
      summary: "Blocked on review",
      blockedReason: "Need reviewer sign-off",
      idempotencyKey: "task-1:block:run-1",
      actor: { type: "agent", id: "forge" },
    });

    await publishTaskLifecycleEvent({
      stateDir,
      action: "qa",
      taskId: "task-1",
      summary: "Ready for QA",
      idempotencyKey: "task-1:qa:run-1",
      actor: { type: "agent", id: "forge" },
    });

    const doneResult = await publishTaskLifecycleEvent({
      stateDir,
      action: "done",
      taskId: "task-1",
      summary: "QA passed and shipped",
      idempotencyKey: "task-1:done:run-1",
      actor: { type: "agent", id: "forge" },
    });

    const snapshot = await readTaskLedgerSnapshot({ stateDir });
    const events = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });

    expect(doneResult.accepted).toBe(1);
    expect(snapshot.tasks[0]).toMatchObject({
      id: "task-1",
      state: "done",
      sessionKey: "agent:forge:main",
      worktree: "/tmp/openclaw-p0-1-lifecycle-publisher-20260325",
      assignedAgent: "forge",
      source: "openclaw-runtime",
      blockedReason: undefined,
    });
    expect(events.map((event) => event.kind)).toEqual([
      "state_changed",
      "note",
      "note",
      "blocked",
      "qa",
      "state_changed",
    ]);

    const reconcileNotes = events.filter(
      (event) =>
        event.kind == "note" &&
        typeof event.idempotencyKey == "string" &&
        event.idempotencyKey.startsWith("reconcile:in-progress-agent-missing:"),
    );

    expect(reconcileNotes).toHaveLength(1);
    expect(reconcileNotes.map((event) => event.summary)).toEqual([
      "Reconcile residue: task is still marked in progress for assigned agent forge, but no agent heartbeat is recorded. This usually means stale residue from earlier work; verify whether the task should remain in progress or be reassigned.",
    ]);

    expect(events.map((event) => event.idempotencyKey)).toEqual([
      "task-1:start:run-1",
      expect.stringMatching(/^reconcile:in-progress-agent-missing:/),
      "task-1:note:run-1",
      "task-1:block:run-1",
      "task-1:qa:run-1",
      "task-1:done:run-1",
    ]);
  });

  it("preserves idempotency on replayed lifecycle events", async () => {
    const stateDir = await createStateDir();

    await publishTaskLifecycleEvent({
      stateDir,
      action: "start",
      taskId: "task-1",
      summary: "Started execution",
      idempotencyKey: "task-1:start:run-2",
      task: { title: "Replay-safe lifecycle" },
    });

    const replay = await publishTaskLifecycleEvent({
      stateDir,
      action: "start",
      taskId: "task-1",
      summary: "Started execution",
      idempotencyKey: "task-1:start:run-2",
    });

    const events = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });
    expect(replay.accepted).toBe(0);
    expect(events.map((event) => event.idempotencyKey)).toEqual(["task-1:start:run-2"]);
  });
});

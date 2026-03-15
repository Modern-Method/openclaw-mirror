import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  TASK_LEDGER_SCHEMA,
  publishTaskLedgerEvents,
  readTaskLedgerEvents,
  readTaskLedgerSnapshot,
  type TaskLedgerRecord,
} from "./task-ledger.js";

const stateDirs: string[] = [];

async function createStateDir() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-task-ledger-"));
  stateDirs.push(dir);
  return dir;
}

async function readSnapshotFile(stateDir: string) {
  return JSON.parse(
    await fs.readFile(path.join(stateDir, "shared", "task-ledger", "snapshot.json"), "utf8"),
  ) as {
    tasks: Array<{ id: string; state: string }>;
    agents: Array<{ id: string; status: string; summary: string }>;
    lastEventId?: string;
  };
}

async function appendRawEvents(stateDir: string, events: TaskLedgerRecord[]) {
  const ledgerDir = path.join(stateDir, "shared", "task-ledger");
  await fs.mkdir(ledgerDir, { recursive: true });
  await fs.appendFile(
    path.join(ledgerDir, "events.jsonl"),
    `${events.map((event) => JSON.stringify(event)).join("\n")}\n`,
    "utf8",
  );
}

afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(
    stateDirs.splice(0).map(async (dir) => await fs.rm(dir, { recursive: true, force: true })),
  );
});

describe("task ledger", () => {
  it("materializes task and agent state from append-only events", async () => {
    const stateDir = await createStateDir();

    const created = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: {
            id: "task-1",
            title: "Ship ledger MVP",
            description: "Replace demo data with shared substrate",
            state: "todo",
            assignedAgent: "sebastian",
            externalRef: "mc-123",
          },
          summary: "Imported task from Mission Control",
          actor: { type: "operator", id: "mission-control" },
        },
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "sebastian",
            name: "Sebastian",
            status: "running",
            currentTaskId: "task-1",
            summary: "Implementing ledger substrate",
            metadata: { host: "ganymede" },
          },
        },
      ],
    });

    await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "transition",
          taskId: "task-1",
          state: "in_progress",
          summary: "Started implementation",
          actor: { type: "agent", id: "sebastian", name: "Sebastian" },
        },
        {
          entity: "task",
          kind: "note",
          taskId: "task-1",
          summary: "Gateway methods and snapshot path are wired",
          actor: { type: "agent", id: "sebastian" },
        },
      ],
    });

    const snapshot = await readTaskLedgerSnapshot({ stateDir });
    const events = await readTaskLedgerEvents({ stateDir });
    const persistedSnapshot = await readSnapshotFile(stateDir);

    expect(created.accepted).toBe(2);
    expect(snapshot.tasks).toHaveLength(1);
    expect(snapshot.tasks[0]).toMatchObject({
      id: "task-1",
      title: "Ship ledger MVP",
      state: "in_progress",
      assignedAgent: "sebastian",
      externalRef: "mc-123",
    });
    expect(snapshot.agents).toHaveLength(1);
    expect(snapshot.agents[0]).toMatchObject({
      id: "sebastian",
      name: "Sebastian",
      status: "running",
      currentTaskId: "task-1",
    });
    expect(snapshot.recentEvents.map((event) => event.kind)).toEqual([
      "created",
      "heartbeat",
      "state_changed",
      "note",
    ]);
    expect(events).toHaveLength(4);
    expect(persistedSnapshot.tasks[0]).toMatchObject({ id: "task-1", state: "in_progress" });
  });

  it("filters event reads by task id and agent id", async () => {
    const stateDir = await createStateDir();

    await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: { id: "task-a", title: "A", state: "backlog" },
        },
        {
          entity: "task",
          kind: "upsert",
          task: { id: "task-b", title: "B", state: "todo" },
        },
        {
          entity: "agent",
          kind: "heartbeat",
          agent: { id: "forge", status: "idle", summary: "Waiting" },
        },
      ],
    });

    const taskEvents = await readTaskLedgerEvents({ stateDir, taskId: "task-b" });
    const agentEvents = await readTaskLedgerEvents({ stateDir, agentId: "forge" });

    expect(taskEvents).toHaveLength(1);
    expect(taskEvents[0]).toMatchObject({ entity: "task", taskId: "task-b" });
    expect(agentEvents).toHaveLength(1);
    expect(agentEvents[0]).toMatchObject({ entity: "agent", agentId: "forge" });
  });

  it("rebuilds a stale snapshot from the canonical event log and repairs snapshot.json", async () => {
    const stateDir = await createStateDir();

    const initial = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: { id: "task-1", title: "Crash-safe snapshot", state: "todo" },
        },
      ],
    });

    await appendRawEvents(stateDir, [
      {
        schema: TASK_LEDGER_SCHEMA,
        id: "evt-manual-1",
        ts: "2026-03-15T05:00:00.000Z",
        entity: "task",
        kind: "state_changed",
        taskId: "task-1",
        summary: "Out-of-band append after snapshot write failed",
        actor: { type: "system" },
        fromState: "todo",
        toState: "in_progress",
      },
    ]);

    const snapshot = await readTaskLedgerSnapshot({ stateDir });
    const repairedSnapshot = await readSnapshotFile(stateDir);

    expect(initial.snapshot.tasks[0]?.state).toBe("todo");
    expect(snapshot.tasks[0]).toMatchObject({ id: "task-1", state: "in_progress" });
    expect(snapshot.lastEventId).toBe("evt-manual-1");
    expect(repairedSnapshot.tasks[0]).toMatchObject({ id: "task-1", state: "in_progress" });
    expect(repairedSnapshot.lastEventId).toBe("evt-manual-1");
  });

  it("publishes against the log, not a stale snapshot cache", async () => {
    const stateDir = await createStateDir();

    await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: { id: "task-1", title: "Append then crash", state: "todo" },
        },
      ],
    });

    await appendRawEvents(stateDir, [
      {
        schema: TASK_LEDGER_SCHEMA,
        id: "evt-manual-2",
        ts: "2026-03-15T05:01:00.000Z",
        entity: "task",
        kind: "state_changed",
        taskId: "task-1",
        summary: "Recovered in-progress state from log",
        actor: { type: "system" },
        fromState: "todo",
        toState: "in_progress",
      },
    ]);

    const result = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "note",
          taskId: "task-1",
          summary: "Continued after crash recovery",
        },
      ],
    });

    const events = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });

    expect(result.snapshot.tasks[0]).toMatchObject({ id: "task-1", state: "in_progress" });
    expect(events.map((event) => event.kind)).toEqual(["created", "state_changed", "note"]);
    expect(result.snapshot.lastEventId).toBe(events[2]?.id);
  });

  it("dedupes immediate retry publishes and explicit idempotency-key replays", async () => {
    const stateDir = await createStateDir();
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-15T05:02:00.000Z"));

    await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: { id: "task-1", title: "Deduped transition", state: "todo" },
        },
      ],
    });

    const firstTransition = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "transition",
          taskId: "task-1",
          state: "in_progress",
          summary: "Worker picked up the task",
        },
      ],
    });
    const immediateRetry = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "transition",
          taskId: "task-1",
          state: "in_progress",
          summary: "Worker picked up the task",
        },
      ],
    });

    vi.setSystemTime(new Date("2026-03-15T05:03:00.000Z"));
    const firstHeartbeat = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "agent",
          kind: "heartbeat",
          idempotencyKey: "run-1:start",
          agent: {
            id: "forge",
            status: "running",
            summary: "Run started",
            sessionKey: "main",
            metadata: { runId: "run-1", phase: "start" },
          },
        },
      ],
    });
    vi.setSystemTime(new Date("2026-03-15T05:05:00.000Z"));
    const replayedHeartbeat = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "agent",
          kind: "heartbeat",
          idempotencyKey: "run-1:start",
          agent: {
            id: "forge",
            status: "running",
            summary: "Run started",
            sessionKey: "main",
            metadata: { runId: "run-1", phase: "start" },
          },
        },
      ],
    });

    const taskEvents = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });
    const agentEvents = await readTaskLedgerEvents({ stateDir, agentId: "forge" });

    expect(firstTransition.accepted).toBe(1);
    expect(immediateRetry.accepted).toBe(0);
    expect(taskEvents.map((event) => event.kind)).toEqual(["created", "state_changed"]);
    expect(firstHeartbeat.accepted).toBe(1);
    expect(replayedHeartbeat.accepted).toBe(0);
    expect(agentEvents).toHaveLength(1);
  });

  it("rejects malformed task and agent ids", async () => {
    const stateDir = await createStateDir();

    await expect(
      publishTaskLedgerEvents({
        stateDir,
        events: [
          {
            entity: "task",
            kind: "upsert",
            task: { id: "bad id", title: "Should fail" },
          },
        ],
      }),
    ).rejects.toThrow(/task id must not contain whitespace/i);

    await expect(
      publishTaskLedgerEvents({
        stateDir,
        events: [
          {
            entity: "agent",
            kind: "heartbeat",
            agent: { id: "bad agent", status: "idle" },
          },
        ],
      }),
    ).rejects.toThrow(/agent id must not contain whitespace/i);
  });
});

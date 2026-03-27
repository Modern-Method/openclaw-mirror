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
            lane: "pinned",
            currentTaskId: "task-1",
            sessionKey: "agent:sebastian:main",
            worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
            branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
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

    expect(created.accepted).toBe(3);
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
      lane: "pinned",
      currentTaskId: "task-1",
      sessionKey: "agent:sebastian:main",
      worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
      branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
    });
    expect(events.find((event) => event.entity === "agent")).toMatchObject({
      entity: "agent",
      agentId: "sebastian",
      status: "running",
      lane: "pinned",
      currentTaskId: "task-1",
      sessionKey: "agent:sebastian:main",
      worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
      branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
      summary: "Implementing ledger substrate",
    });
    expect(snapshot.recentEvents.map((event) => event.kind)).toEqual([
      "created",
      "heartbeat",
      "note",
      "state_changed",
      "note",
    ]);
    expect(events).toHaveLength(5);
    expect(persistedSnapshot.tasks[0]).toMatchObject({ id: "task-1", state: "in_progress" });
  });

  it("clears explicit heartbeat contract fields when a later heartbeat omits them", async () => {
    const stateDir = await createStateDir();

    await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "running",
            lane: "build",
            currentTaskId: "task-1",
            sessionKey: "agent:forge:main",
            worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
            branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
            summary: "Working task",
          },
        },
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "idle",
            lane: undefined,
            currentTaskId: undefined,
            sessionKey: undefined,
            worktree: undefined,
            branch: undefined,
            summary: "Heartbeat",
          },
        },
      ],
    });

    const snapshot = await readTaskLedgerSnapshot({ stateDir });
    const agentEvents = await readTaskLedgerEvents({ stateDir, agentId: "forge" });

    expect(snapshot.agents[0]).toMatchObject({
      id: "forge",
      status: "idle",
      summary: "Heartbeat",
      lane: undefined,
      currentTaskId: undefined,
      sessionKey: undefined,
      worktree: undefined,
      branch: undefined,
    });
    expect(agentEvents[1]).toMatchObject({
      entity: "agent",
      kind: "heartbeat",
      lane: null,
      currentTaskId: null,
      sessionKey: null,
      worktree: null,
      branch: null,
    });
  });

  it("clears heartbeat contract fields even when the later heartbeat omits the keys entirely", async () => {
    const stateDir = await createStateDir();

    await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "running",
            lane: "build",
            currentTaskId: "task-1",
            sessionKey: "agent:forge:main",
            worktree: "/tmp/openclaw-p0-2-heartbeat-contract-20260325",
            branch: "feat/task-ledger-p0-2-heartbeat-contract-20260325",
            summary: "Working task",
          },
        },
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "idle",
            summary: "Run finished",
          },
        },
      ],
    });

    const snapshot = await readTaskLedgerSnapshot({ stateDir });
    const agentEvents = await readTaskLedgerEvents({ stateDir, agentId: "forge" });

    expect(snapshot.agents[0]).toMatchObject({
      id: "forge",
      status: "idle",
      summary: "Run finished",
      lane: undefined,
      currentTaskId: undefined,
      sessionKey: undefined,
      worktree: undefined,
      branch: undefined,
    });
    expect(agentEvents[1]).toMatchObject({
      entity: "agent",
      kind: "heartbeat",
      lane: null,
      currentTaskId: null,
      sessionKey: null,
      worktree: null,
      branch: null,
    });
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

  it("repairs snapshot content drift even when lastEventId matches", async () => {
    const stateDir = await createStateDir();

    const initial = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: { id: "task-1", title: "Drift-safe snapshot", state: "todo" },
        },
      ],
    });

    const snapshotPath = path.join(stateDir, "shared", "task-ledger", "snapshot.json");
    const staleSnapshot = JSON.parse(await fs.readFile(snapshotPath, "utf8")) as {
      tasks: Array<{ state: string }>;
      lastEventId?: string;
    };
    staleSnapshot.tasks[0].state = "blocked";
    await fs.writeFile(snapshotPath, `${JSON.stringify(staleSnapshot, null, 2)}\n`, "utf8");

    const repaired = await readTaskLedgerSnapshot({ stateDir });
    const repairedOnDisk = await readSnapshotFile(stateDir);

    expect(initial.snapshot.lastEventId).toBe(staleSnapshot.lastEventId);
    expect(repaired.lastEventId).toBe(staleSnapshot.lastEventId);
    expect(repaired.tasks[0]).toMatchObject({ id: "task-1", state: "todo" });
    expect(repairedOnDisk.tasks[0]).toMatchObject({ id: "task-1", state: "todo" });
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

  it("ignores delayed task replays with the same idempotency key after newer task events", async () => {
    const stateDir = await createStateDir();
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-15T05:10:00.000Z"));

    await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: { id: "task-1", title: "Replay-safe task", state: "todo" },
        },
      ],
    });

    const started = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "transition",
          taskId: "task-1",
          state: "in_progress",
          summary: "Started work",
          idempotencyKey: "task-1:start",
        },
      ],
    });

    vi.setSystemTime(new Date("2026-03-15T05:11:00.000Z"));
    const qa = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "transition",
          taskId: "task-1",
          state: "qa",
          summary: "Ready for QA",
          idempotencyKey: "task-1:qa",
        },
      ],
    });

    vi.setSystemTime(new Date("2026-03-15T05:12:00.000Z"));
    const replayedStart = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "transition",
          taskId: "task-1",
          state: "in_progress",
          summary: "Started work",
          idempotencyKey: "task-1:start",
        },
      ],
    });

    const snapshot = await readTaskLedgerSnapshot({ stateDir });
    const taskEvents = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });

    expect(started.accepted).toBe(1);
    expect(qa.accepted).toBe(1);
    expect(replayedStart.accepted).toBe(0);
    expect(taskEvents.map((event) => event.idempotencyKey)).toEqual([
      undefined,
      "task-1:start",
      "task-1:qa",
    ]);
    expect(snapshot.tasks[0]).toMatchObject({ id: "task-1", state: "qa" });
    expect(snapshot.recentEvents.map((event) => event.kind)).toEqual([
      "created",
      "state_changed",
      "qa",
    ]);
  });

  it("ignores non-consecutive agent idempotency-key replays when rebuilding from events.jsonl", async () => {
    const stateDir = await createStateDir();

    await publishTaskLedgerEvents({
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
          ts: "2026-03-15T05:20:00.000Z",
        },
        {
          entity: "agent",
          kind: "heartbeat",
          idempotencyKey: "run-1:end",
          agent: {
            id: "forge",
            status: "idle",
            summary: "Run finished",
            sessionKey: "main",
            metadata: { runId: "run-1", phase: "end" },
          },
          ts: "2026-03-15T05:21:00.000Z",
        },
      ],
    });

    await appendRawEvents(stateDir, [
      {
        schema: TASK_LEDGER_SCHEMA,
        id: "evt-agent-replay-1",
        ts: "2026-03-15T05:22:00.000Z",
        entity: "agent",
        kind: "heartbeat",
        agentId: "forge",
        status: "running",
        sessionKey: "main",
        summary: "Run started",
        metadata: { runId: "run-1", phase: "start" },
        idempotencyKey: "run-1:start",
      },
    ]);

    const snapshot = await readTaskLedgerSnapshot({ stateDir });
    const rawAgentEvents = await readTaskLedgerEvents({ stateDir, agentId: "forge" });

    expect(rawAgentEvents).toHaveLength(3);
    expect(snapshot.agents[0]).toMatchObject({
      id: "forge",
      status: "idle",
      summary: "Run finished",
    });
    expect(snapshot.recentEvents.filter((event) => event.entity === "agent")).toHaveLength(2);
    expect(
      snapshot.recentEvents
        .filter((event) => event.entity === "agent")
        .map((event) => event.idempotencyKey),
    ).toEqual(["run-1:start", "run-1:end"]);
  });

  it("emits reconcile evidence when an active agent points at a task that is still todo", async () => {
    const stateDir = await createStateDir();

    const result = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: { id: "task-1", title: "Start me", state: "todo", assignedAgent: "forge" },
          ts: "2026-03-15T06:00:00.000Z",
        },
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "running",
            currentTaskId: "task-1",
            summary: "Working task-1",
          },
          ts: "2026-03-15T06:01:00.000Z",
        },
      ],
    });

    const taskEvents = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });

    expect(result.accepted).toBe(3);
    expect(taskEvents.at(-1)).toMatchObject({
      entity: "task",
      kind: "note",
      taskId: "task-1",
      actor: { type: "system", id: "task-ledger-reconciler" },
    });
    expect(taskEvents.at(-1)?.summary).toMatch(/^Reconcile mismatch:/);
    expect(taskEvents.at(-1)?.summary).toMatch(/still todo/i);
  });

  it("emits reconcile residue when an in-progress task is assigned to an idle agent", async () => {
    const stateDir = await createStateDir();

    const result = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: {
            id: "task-1",
            title: "Investigate drift",
            state: "in_progress",
            assignedAgent: "forge",
          },
          ts: "2026-03-15T06:10:00.000Z",
        },
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "idle",
            currentTaskId: "task-1",
            summary: "Waiting",
          },
          ts: "2026-03-15T06:10:00.000Z",
        },
      ],
    });

    const taskEvents = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });

    expect(result.accepted).toBe(3);
    expect(taskEvents.at(-1)?.summary).toMatch(/^Reconcile residue:/);
    expect(taskEvents.at(-1)?.summary).toMatch(/latest heartbeat reports the agent idle/i);
  });

  it("does not repeat reconcile residue for unchanged idle drift", async () => {
    const stateDir = await createStateDir();

    await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: {
            id: "task-1",
            title: "Investigate drift",
            state: "in_progress",
            assignedAgent: "forge",
          },
          ts: "2026-03-15T06:10:00.000Z",
        },
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "idle",
            currentTaskId: "task-1",
            summary: "Waiting",
          },
          ts: "2026-03-15T06:10:00.000Z",
        },
      ],
    });

    const replay = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "idle",
            currentTaskId: "task-1",
            summary: "Still waiting",
          },
          ts: "2026-03-15T06:11:00.000Z",
        },
      ],
    });

    const taskEvents = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });
    const reconcileEvents = taskEvents.filter(
      (event) =>
        event.entity === "task" &&
        event.kind === "note" &&
        event.actor.id === "task-ledger-reconciler",
    );

    expect(replay.accepted).toBe(1);
    expect(reconcileEvents).toHaveLength(1);
    expect(reconcileEvents[0]?.summary).toMatch(/^Reconcile residue:/);
  });

  it("emits reconcile residue when an in-progress task is assigned to a stale agent heartbeat", async () => {
    const stateDir = await createStateDir();

    const result = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "running",
            currentTaskId: "task-1",
            summary: "Earlier run",
          },
          ts: "2026-03-15T06:20:00.000Z",
        },
        {
          entity: "task",
          kind: "upsert",
          task: {
            id: "task-1",
            title: "Resume task",
            state: "in_progress",
            assignedAgent: "forge",
          },
          ts: "2026-03-15T06:40:00.000Z",
        },
      ],
    });

    const taskEvents = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });

    expect(result.accepted).toBe(3);
    expect(taskEvents.at(-1)?.summary).toMatch(/^Reconcile residue:/);
    expect(taskEvents.at(-1)?.summary).toMatch(/latest heartbeat is stale/i);
    expect(taskEvents.at(-1)?.summary).toContain("2026-03-15T06:20:00.000Z");
  });

  it("emits reconcile evidence when older blocked work coexists with newer active work for the same agent", async () => {
    const stateDir = await createStateDir();

    const result = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: { id: "task-1", title: "Blocked work", state: "blocked", assignedAgent: "forge" },
          ts: "2026-03-15T06:50:00.000Z",
        },
        {
          entity: "task",
          kind: "upsert",
          task: {
            id: "task-2",
            title: "Active work",
            state: "in_progress",
            assignedAgent: "forge",
          },
          ts: "2026-03-15T07:00:00.000Z",
        },
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "running",
            currentTaskId: "task-2",
            summary: "Working task-2",
          },
          ts: "2026-03-15T07:00:00.000Z",
        },
      ],
    });

    const blockedTaskEvents = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });

    expect(result.accepted).toBe(4);
    expect(blockedTaskEvents.at(-1)?.summary).toMatch(/^Reconcile residue:/);
    expect(blockedTaskEvents.at(-1)?.summary).toMatch(/newer active work exists on task-2/i);
  });

  it("emits reconcile mismatch when heartbeat task context disagrees with task ownership", async () => {
    const stateDir = await createStateDir();

    const result = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "task",
          kind: "upsert",
          task: { id: "task-1", title: "Already closed", state: "done", assignedAgent: "atlas" },
          ts: "2026-03-15T07:10:00.000Z",
        },
        {
          entity: "agent",
          kind: "heartbeat",
          agent: {
            id: "forge",
            status: "running",
            currentTaskId: "task-1",
            summary: "Still on task-1",
          },
          ts: "2026-03-15T07:11:00.000Z",
        },
      ],
    });

    const taskEvents = await readTaskLedgerEvents({ stateDir, taskId: "task-1" });

    expect(result.accepted).toBe(3);
    expect(taskEvents.at(-1)?.summary).toMatch(/^Reconcile mismatch:/);
    expect(taskEvents.at(-1)?.summary).toMatch(/task is assigned to atlas/i);
  });

  it("accepts recall trace events without mutating task/agent projections", async () => {
    const stateDir = await createStateDir();

    const result = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "recall",
          kind: "trace",
          sessionKey: "agent:forge:main",
          agentId: "forge",
          ran: true,
          scope: { senderId: "8480568759", channelClass: "dm" },
          candidatesConsidered: 3,
          injectedCount: 1,
          injectedChars: 42,
          withheldCount: 2,
          dependencyStatus: "ok",
          idempotencyKey: "recall:trace:1",
          ts: "2026-03-15T07:20:00.000Z",
        },
      ],
    });

    expect(result.accepted).toBe(1);
    expect(result.events[0]).toMatchObject({
      entity: "recall",
      kind: "trace",
      sessionKey: "agent:forge:main",
      agentId: "forge",
      ran: true,
      dependencyStatus: "ok",
      candidatesConsidered: 3,
      injectedCount: 1,
      injectedChars: 42,
      withheldCount: 2,
    });

    const events = await readTaskLedgerEvents({ stateDir });
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ entity: "recall", kind: "trace" });

    const snapshot = await readTaskLedgerSnapshot({ stateDir });
    expect(snapshot.tasks).toEqual([]);
    expect(snapshot.agents).toEqual([]);
    expect(snapshot.recentEvents).toHaveLength(1);
    expect(snapshot.recentEvents[0]).toMatchObject({ entity: "recall", kind: "trace" });
  });

  it("accepts distinct recall trace events with identical payload within the dedupe window", async () => {
    const stateDir = await createStateDir();

    const first = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "recall",
          kind: "trace",
          sessionKey: "agent:forge:main",
          agentId: "forge",
          ran: true,
          scope: { senderId: "8480568759", channelClass: "dm" },
          candidatesConsidered: 3,
          injectedCount: 1,
          injectedChars: 42,
          withheldCount: 2,
          dependencyStatus: "ok",
          ts: "2026-03-15T07:21:00.000Z",
        },
      ],
    });

    const second = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "recall",
          kind: "trace",
          sessionKey: "agent:forge:main",
          agentId: "forge",
          ran: true,
          scope: { senderId: "8480568759", channelClass: "dm" },
          candidatesConsidered: 3,
          injectedCount: 1,
          injectedChars: 42,
          withheldCount: 2,
          dependencyStatus: "ok",
          ts: "2026-03-15T07:21:00.400Z",
        },
      ],
    });

    expect(first.accepted).toBe(1);
    expect(second.accepted).toBe(1);

    const events = await readTaskLedgerEvents({ stateDir });
    expect(events).toHaveLength(2);
  });

  it("normalizes recall scope to allowed string keys", async () => {
    const stateDir = await createStateDir();

    const result = await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "recall",
          kind: "trace",
          sessionKey: "agent:forge:main",
          agentId: "forge",
          ran: true,
          scope: {
            senderId: "8480568759",
            channelClass: "dm",
            threadId: "agent:main:thread",
            resourceId: "agent:main:resource",
            ignored: { nested: true },
            dropped: 11,
          } as unknown as Record<string, string>,
          candidatesConsidered: 1,
          injectedCount: 0,
          injectedChars: 0,
          withheldCount: 0,
          dependencyStatus: "ok",
          ts: "2026-03-15T07:22:00.000Z",
        },
      ],
    });

    expect(result.accepted).toBe(1);
    expect(result.events[0].scope).toMatchObject({
      senderId: "8480568759",
      channelClass: "dm",
      threadId: "agent:main:thread",
      resourceId: "agent:main:resource",
    });
    expect(result.events[0].scope?.ignored).toBeUndefined();
    expect(result.events[0].scope?.dropped).toBeUndefined();
  });

  it("includes recall traces in agent-scoped event reads", async () => {
    const stateDir = await createStateDir();

    await publishTaskLedgerEvents({
      stateDir,
      events: [
        {
          entity: "recall",
          kind: "trace",
          sessionKey: "agent:forge:main",
          agentId: "forge",
          ran: true,
          candidatesConsidered: 0,
          injectedCount: 0,
          injectedChars: 0,
          withheldCount: 0,
          dependencyStatus: "ok",
          ts: "2026-03-15T07:23:00.000Z",
        },
        {
          entity: "agent",
          kind: "heartbeat",
          agent: { id: "forge", status: "running" },
          ts: "2026-03-15T07:23:01.000Z",
        },
      ],
    });

    const events = await readTaskLedgerEvents({ stateDir, agentId: "forge" });

    expect(events.some((event) => event.entity === "recall")).toBe(true);
    expect(events.filter((event) => event.entity === "agent")).toHaveLength(1);
    expect(events).toHaveLength(2);
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

    await expect(
      publishTaskLedgerEvents({
        stateDir,
        events: [
          {
            entity: "recall",
            kind: "trace",
            sessionKey: "agent:forge:main",
            agentId: "forge",
            ran: true,
            dependencyStatus: "bogus-status" as never,
            ts: "2026-03-15T07:24:00.000Z",
          },
        ],
      }),
    ).rejects.toThrow(/invalid recall dependencyStatus/i);
  });
});

import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  publishTaskLedgerEvents,
  readTaskLedgerEvents,
  readTaskLedgerSnapshot,
} from "./task-ledger.js";

const stateDirs: string[] = [];

async function createStateDir() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-task-ledger-"));
  stateDirs.push(dir);
  return dir;
}

afterEach(async () => {
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
    const persistedSnapshot = JSON.parse(
      await fs.readFile(path.join(stateDir, "shared", "task-ledger", "snapshot.json"), "utf8"),
    ) as {
      tasks: Array<{ id: string; state: string }>;
    };

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
});

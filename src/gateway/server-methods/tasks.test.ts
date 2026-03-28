import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as taskLedger from "../../infra/task-ledger.js";
import { tasksHandlers } from "./tasks.js";
import type { GatewayRequestContext } from "./types.js";

let previousStateDir: string | undefined;
const stateDirs: string[] = [];

async function createStateDir() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-task-methods-"));
  stateDirs.push(dir);
  process.env.OPENCLAW_STATE_DIR = dir;
  return dir;
}

function makeContext(): GatewayRequestContext {
  return {
    broadcast: vi.fn(),
  } as unknown as GatewayRequestContext;
}

beforeEach(() => {
  previousStateDir = process.env.OPENCLAW_STATE_DIR;
});

afterEach(async () => {
  process.env.OPENCLAW_STATE_DIR = previousStateDir;
  await Promise.all(
    stateDirs.splice(0).map(async (dir) => await fs.rm(dir, { recursive: true, force: true })),
  );
});

describe("tasks gateway handlers", () => {
  it("uses tasks.publish as the write path and tasks.snapshot/tasks.events as the sync surfaces", async () => {
    await createStateDir();
    const publishRespond = vi.fn();
    const snapshotRespond = vi.fn();
    const eventsRespond = vi.fn();
    const context = makeContext();

    await tasksHandlers["tasks.publish"]({
      req: {} as never,
      params: {
        events: [
          {
            entity: "task",
            kind: "upsert",
            task: { id: "task-1", title: "Trace Mission Control ingest", state: "in_progress" },
          },
          {
            entity: "agent",
            kind: "heartbeat",
            agent: {
              id: "forge",
              status: "running",
              currentTaskId: "task-1",
              summary: "Syncing through the task ledger",
            },
          },
        ],
      },
      respond: publishRespond as never,
      context,
      client: null,
      isWebchatConnect: () => false,
    });

    await tasksHandlers["tasks.snapshot"]({
      req: {} as never,
      params: { recentEventLimit: 10 },
      respond: snapshotRespond as never,
      context,
      client: null,
      isWebchatConnect: () => false,
    });

    await tasksHandlers["tasks.events"]({
      req: {} as never,
      params: { taskId: "task-1", limit: 10 },
      respond: eventsRespond as never,
      context,
      client: null,
      isWebchatConnect: () => false,
    });

    expect(publishRespond).toHaveBeenCalledWith(
      true,
      expect.objectContaining({
        accepted: expect.any(Number),
        events: expect.arrayContaining([
          expect.objectContaining({ entity: "task", taskId: "task-1" }),
          expect.objectContaining({ entity: "agent", agentId: "forge" }),
        ]),
      }),
      undefined,
    );
    expect(snapshotRespond).toHaveBeenCalledWith(
      true,
      expect.objectContaining({
        tasks: [expect.objectContaining({ id: "task-1", state: "in_progress" })],
        agents: [
          expect.objectContaining({
            id: "forge",
            status: "running",
            currentTaskId: "task-1",
          }),
        ],
        recentEvents: expect.arrayContaining([
          expect.objectContaining({ entity: "task", taskId: "task-1" }),
          expect.objectContaining({ entity: "agent", agentId: "forge" }),
        ]),
      }),
      undefined,
    );
    expect(eventsRespond).toHaveBeenCalledWith(
      true,
      {
        events: expect.arrayContaining([
          expect.objectContaining({ entity: "task", taskId: "task-1" }),
        ]),
      },
      undefined,
    );
  });

  it("publishes events, broadcasts them, and returns a snapshot", async () => {
    await createStateDir();
    const respond = vi.fn();
    const context = makeContext();

    await tasksHandlers["tasks.publish"]({
      req: {} as never,
      params: {
        events: [
          {
            entity: "task",
            kind: "upsert",
            task: { id: "task-1", title: "Wire Mission Control", state: "todo" },
          },
          {
            entity: "agent",
            kind: "heartbeat",
            agent: { id: "sebastian", status: "running", summary: "Wiring dashboard" },
          },
        ],
      },
      respond: respond as never,
      context,
      client: null,
      isWebchatConnect: () => false,
    });

    expect(respond).toHaveBeenCalledWith(
      true,
      expect.objectContaining({
        accepted: 2,
        snapshot: expect.objectContaining({
          tasks: [expect.objectContaining({ id: "task-1", state: "todo" })],
          agents: [expect.objectContaining({ id: "sebastian", status: "running" })],
        }),
      }),
      undefined,
    );
    expect(vi.mocked(context.broadcast)).toHaveBeenCalledTimes(2);
    expect(vi.mocked(context.broadcast)).toHaveBeenNthCalledWith(
      1,
      "tasks.ledger",
      expect.objectContaining({ entity: "task", taskId: "task-1" }),
      { dropIfSlow: true },
    );
  });

  it("returns filtered event history", async () => {
    await createStateDir();
    const publishRespond = vi.fn();
    const snapshotRespond = vi.fn();
    const context = makeContext();

    await tasksHandlers["tasks.publish"]({
      req: {} as never,
      params: {
        events: [
          { entity: "task", kind: "upsert", task: { id: "task-a", title: "A" } },
          { entity: "task", kind: "upsert", task: { id: "task-b", title: "B" } },
        ],
      },
      respond: publishRespond as never,
      context,
      client: null,
      isWebchatConnect: () => false,
    });

    await tasksHandlers["tasks.events"]({
      req: {} as never,
      params: { taskId: "task-b", limit: 10 },
      respond: snapshotRespond as never,
      context,
      client: null,
      isWebchatConnect: () => false,
    });

    expect(snapshotRespond).toHaveBeenCalledWith(
      true,
      {
        events: [expect.objectContaining({ entity: "task", taskId: "task-b" })],
      },
      undefined,
    );
  });

  it("returns INVALID_REQUEST for malformed task ledger events", async () => {
    const publishRespond = vi.fn();
    const context = makeContext();
    const spy = vi.spyOn(taskLedger, "publishTaskLedgerEvents").mockRejectedValueOnce(
      new taskLedger.TaskLedgerPublishInputError("bad event"),
    );

    await tasksHandlers["tasks.publish"]({
      req: {} as never,
      params: { events: [{ entity: "task", kind: "upsert", task: { id: "task-1", title: "A" } }] },
      respond: publishRespond as never,
      context,
      client: null,
      isWebchatConnect: () => false,
    });

    expect(publishRespond).toHaveBeenCalledWith(
      false,
      undefined,
      expect.objectContaining({ code: "INVALID_REQUEST", message: "bad event" }),
    );
    spy.mockRestore();
  });

  it("returns UNAVAILABLE for internal publish failures", async () => {
    const publishRespond = vi.fn();
    const context = makeContext();
    const spy = vi.spyOn(taskLedger, "publishTaskLedgerEvents").mockRejectedValueOnce(
      new Error("disk full"),
    );

    await tasksHandlers["tasks.publish"]({
      req: {} as never,
      params: { events: [{ entity: "task", kind: "upsert", task: { id: "task-1", title: "A" } }] },
      respond: publishRespond as never,
      context,
      client: null,
      isWebchatConnect: () => false,
    });

    expect(publishRespond).toHaveBeenCalledWith(
      false,
      undefined,
      expect.objectContaining({ code: "UNAVAILABLE", message: "disk full" }),
    );
    spy.mockRestore();
  });
});

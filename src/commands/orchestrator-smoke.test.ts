import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TaskLedgerSnapshot } from "../infra/task-ledger.js";
import { jsonResponse, requestBodyText, requestUrl } from "../test-helpers/http.js";
import type { OrchestratorSmokeReport } from "./orchestrator-smoke.js";
import { orchestratorSmokeCommand } from "./orchestrator-smoke.js";

const callGatewayMock = vi.fn();
const readBestEffortConfigMock = vi.fn();

vi.mock("../gateway/call.js", () => ({
  callGateway: (...args: unknown[]) => callGatewayMock(...args),
}));

vi.mock("../config/config.js", () => ({
  readBestEffortConfig: (...args: unknown[]) => readBestEffortConfigMock(...args),
}));

vi.mock("../cli/progress.js", () => ({
  withProgress: async (_opts: unknown, run: () => Promise<unknown>) => await run(),
}));

function createRuntime() {
  return {
    log: vi.fn(),
    error: vi.fn(),
    exit: vi.fn(),
  };
}

function createSnapshot(params?: Partial<TaskLedgerSnapshot>): TaskLedgerSnapshot {
  const now = Date.now();
  return {
    schema: "openclaw.task-ledger.snapshot.v1",
    generatedAt: new Date(now).toISOString(),
    lastEventId: "event-1",
    paths: {
      rootDir: "/tmp/state/shared/task-ledger",
      eventsFile: "/tmp/state/shared/task-ledger/events.jsonl",
      snapshotFile: "/tmp/state/shared/task-ledger/snapshot.json",
    },
    tasks: [
      {
        id: "task-1",
        title: "Ship smoke check",
        state: "in_progress",
        priority: "high",
        source: "openclaw",
        busTopic: "shared.task.ledger",
        lastEventAt: new Date(now - 60_000).toISOString(),
        metadata: {},
      },
    ],
    agents: [
      {
        id: "main",
        name: "Main",
        status: "running",
        summary: "Running smoke",
        heartbeatAt: new Date(now - 30_000).toISOString(),
        lastSeenAt: new Date(now - 30_000).toISOString(),
        metadata: {},
      },
    ],
    recentEvents: [
      {
        schema: "openclaw.task-ledger.event.v1",
        id: "event-1",
        ts: new Date(now - 30_000).toISOString(),
        entity: "agent",
        kind: "heartbeat",
        agentId: "main",
        status: "running",
        summary: "Heartbeat",
        metadata: {},
      },
    ],
    ...params,
  };
}

describe("orchestratorSmokeCommand", () => {
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    vi.clearAllMocks();
    readBestEffortConfigMock.mockResolvedValue({
      hooks: {
        internal: {
          entries: {
            "ethos-context": {
              enabled: true,
              ethosUrl: "http://127.0.0.1:8766",
              apiKey: "secret-token",
            },
          },
        },
      },
    });
  });

  afterEach(() => {
    vi.stubGlobal("fetch", originalFetch);
  });

  it("prints a JSON report when all checks pass", async () => {
    const runtime = createRuntime();
    const snapshot = createSnapshot();

    callGatewayMock.mockImplementation(async (params: { method: string }) => {
      if (params.method === "health") {
        return { ok: true };
      }
      if (params.method === "tasks.snapshot") {
        return snapshot;
      }
      if (params.method === "tasks.events") {
        return { events: [snapshot.recentEvents[0]] };
      }
      throw new Error(`unexpected method ${params.method}`);
    });

    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      expect(requestUrl(input)).toBe("http://127.0.0.1:8766/search");
      expect(init?.method).toBe("POST");
      expect(init?.headers).toMatchObject({
        "content-type": "application/json",
        authorization: "Bearer secret-token",
      });
      expect(JSON.parse(requestBodyText(init?.body))).toMatchObject({
        query: "openclaw orchestrator smoke check",
        limit: 1,
        resourceId: "smoke-check",
        threadId: "smoke-check",
      });
      return jsonResponse({ results: [{ id: "memory-1", text: "hello" }] });
    });
    vi.stubGlobal("fetch", fetchMock as unknown as typeof fetch);

    await orchestratorSmokeCommand({ json: true, timeoutMs: 2000 }, runtime as never);

    expect(runtime.exit).not.toHaveBeenCalled();
    const report = JSON.parse(String(runtime.log.mock.calls[0]?.[0])) as OrchestratorSmokeReport;
    expect(report.ok).toBe(true);
    expect(report.gateway.status).toBe("ok");
    expect(report.taskLedger.status).toBe("ok");
    expect(report.missionControl.status).toBe("ok");
    expect(report.ethos.status).toBe("ok");
  });

  it("warns when ethos search is disabled but keeps the smoke check green", async () => {
    const runtime = createRuntime();
    const snapshot = createSnapshot();

    readBestEffortConfigMock.mockResolvedValue({
      hooks: {
        internal: {
          entries: {
            "ethos-context": {
              enabled: false,
            },
          },
        },
      },
    });

    callGatewayMock.mockImplementation(async (params: { method: string }) => {
      if (params.method === "health") {
        return { ok: true };
      }
      if (params.method === "tasks.snapshot") {
        return snapshot;
      }
      if (params.method === "tasks.events") {
        return { events: [snapshot.recentEvents[0]] };
      }
      throw new Error(`unexpected method ${params.method}`);
    });

    await orchestratorSmokeCommand({ json: false }, runtime as never);

    expect(runtime.exit).not.toHaveBeenCalled();
    expect(runtime.log.mock.calls.map(([line]) => String(line)).join("\n")).toContain(
      "[warn] Ethos: ethos-context disabled",
    );
  });

  it("fails when the Mission Control sync surfaces disagree on the latest event", async () => {
    const runtime = createRuntime();
    const snapshot = createSnapshot({ lastEventId: "event-2" });

    callGatewayMock.mockImplementation(async (params: { method: string }) => {
      if (params.method === "health") {
        return { ok: true };
      }
      if (params.method === "tasks.snapshot") {
        return snapshot;
      }
      if (params.method === "tasks.events") {
        return {
          events: [
            {
              ...snapshot.recentEvents[0],
              id: "event-1",
            },
          ],
        };
      }
      throw new Error(`unexpected method ${params.method}`);
    });

    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ results: [] })) as unknown as typeof fetch,
    );

    await orchestratorSmokeCommand({ json: true }, runtime as never);

    expect(runtime.exit).toHaveBeenCalledWith(1);
    const report = JSON.parse(String(runtime.log.mock.calls[0]?.[0])) as OrchestratorSmokeReport;
    expect(report.ok).toBe(false);
    expect(report.missionControl.status).toBe("fail");
    expect(report.missionControl.summary).toContain("mismatch");
  });
});

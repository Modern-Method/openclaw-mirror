import { resolveDefaultAgentId } from "../agents/agent-scope.js";
import { withProgress } from "../cli/progress.js";
import { readBestEffortConfig } from "../config/config.js";
import type { OpenClawConfig } from "../config/config.js";
import { callGateway } from "../gateway/call.js";
import { resolveHookConfig } from "../hooks/config.js";
import { formatErrorMessage } from "../infra/errors.js";
import {
  formatDurationCompact,
  formatDurationPrecise,
} from "../infra/format-time/format-duration.js";
import type { TaskLedgerRecord, TaskLedgerSnapshot } from "../infra/task-ledger.js";
import { writeRuntimeJson, type RuntimeEnv } from "../runtime.js";

const DEFAULT_TIMEOUT_MS = 5_000;
const DEFAULT_LEDGER_STALE_AFTER_MS = 15 * 60_000;
const TASK_LEDGER_RECENT_LIMIT = 50;

type SmokeStatus = "ok" | "warn" | "fail";

type GatewayCheck = {
  status: SmokeStatus;
  summary: string;
  reachable: boolean;
  durationMs?: number;
  error?: string;
};

type TaskLedgerCheck = {
  status: SmokeStatus;
  summary: string;
  taskCount: number;
  agentCount: number;
  recentEventCount: number;
  lastEventId?: string | null;
  lastEventAt?: string | null;
  lastEventAgeMs?: number | null;
  staleAfterMs: number;
  error?: string;
};

type MissionControlCheck = {
  status: SmokeStatus;
  summary: string;
  snapshotLastEventId?: string | null;
  eventStreamLastEventId?: string | null;
  error?: string;
};

type EthosCheck = {
  status: SmokeStatus;
  summary: string;
  configured: boolean;
  enabled: boolean;
  searchUrl?: string | null;
  durationMs?: number;
  resultCount?: number;
  error?: string;
};

export type OrchestratorSmokeReport = {
  ok: boolean;
  checkedAt: string;
  durationMs: number;
  gateway: GatewayCheck;
  taskLedger: TaskLedgerCheck;
  missionControl: MissionControlCheck;
  ethos: EthosCheck;
};

type TimedSuccess<T> = {
  ok: true;
  value: T;
  durationMs: number;
};

type TimedFailure = {
  ok: false;
  error: string;
  durationMs: number;
};

type TimedResult<T> = TimedSuccess<T> | TimedFailure;

type TasksEventsResponse = {
  events?: TaskLedgerRecord[];
};

type EthosContextHookConfig = {
  enabled?: boolean;
  ethosUrl?: string;
  apiKey?: string;
  timeoutMs?: number;
};

function resolveOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function resolvePositiveInt(value: unknown, fallback: number): number {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    return Math.floor(value);
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed) && parsed > 0) {
      return Math.floor(parsed);
    }
  }
  return fallback;
}

async function runTimed<T>(work: () => Promise<T>): Promise<TimedResult<T>> {
  const startedAt = Date.now();
  try {
    return {
      ok: true,
      value: await work(),
      durationMs: Date.now() - startedAt,
    };
  } catch (error) {
    return {
      ok: false,
      error: formatErrorMessage(error),
      durationMs: Date.now() - startedAt,
    };
  }
}

function resolveLatestLedgerTimestamp(snapshot: TaskLedgerSnapshot): string | null {
  let latestMs = Number.NEGATIVE_INFINITY;

  for (const task of snapshot.tasks) {
    const parsed = Date.parse(task.lastEventAt);
    if (Number.isFinite(parsed)) {
      latestMs = Math.max(latestMs, parsed);
    }
  }

  for (const agent of snapshot.agents) {
    const parsed = Date.parse(agent.lastSeenAt);
    if (Number.isFinite(parsed)) {
      latestMs = Math.max(latestMs, parsed);
    }
  }

  for (const event of snapshot.recentEvents) {
    const parsed = Date.parse(event.ts);
    if (Number.isFinite(parsed)) {
      latestMs = Math.max(latestMs, parsed);
    }
  }

  if (!Number.isFinite(latestMs)) {
    return null;
  }
  return new Date(latestMs).toISOString();
}

function countEthosResults(payload: unknown): number {
  if (!payload || typeof payload !== "object") {
    return 0;
  }
  const record = payload as Record<string, unknown>;
  if (Array.isArray(record.results)) {
    return record.results.length;
  }
  if (Array.isArray(record.items)) {
    return record.items.length;
  }
  return 0;
}

async function probeEthosSearch(params: {
  cfg: OpenClawConfig;
  timeoutMs?: number;
}): Promise<EthosCheck> {
  const hookConfig = resolveHookConfig(params.cfg, "ethos-context") as
    | EthosContextHookConfig
    | undefined;
  const enabled = hookConfig?.enabled === true;
  const baseUrl = resolveOptionalString(hookConfig?.ethosUrl);

  if (!enabled) {
    return {
      status: "warn",
      summary: "ethos-context disabled",
      configured: false,
      enabled: false,
      searchUrl: null,
    };
  }

  if (!baseUrl) {
    return {
      status: "fail",
      summary: "ethos-context enabled but ethosUrl missing",
      configured: false,
      enabled: true,
      searchUrl: null,
      error: "ethos-context.enabled=true without hooks.internal.entries.ethos-context.ethosUrl",
    };
  }

  let searchUrl: string;
  try {
    searchUrl = new URL("/search", baseUrl).toString();
  } catch {
    return {
      status: "fail",
      summary: "ethos-context URL invalid",
      configured: true,
      enabled: true,
      searchUrl: null,
      error: `Invalid Ethos base URL: ${baseUrl}`,
    };
  }

  const agentId = resolveDefaultAgentId(params.cfg);
  const timeoutMs =
    params.timeoutMs ?? resolvePositiveInt(hookConfig?.timeoutMs, DEFAULT_TIMEOUT_MS);
  const startedAt = Date.now();
  const abortController = new AbortController();
  const timeout = setTimeout(() => {
    abortController.abort();
  }, timeoutMs);

  try {
    const response = await fetch(searchUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(resolveOptionalString(hookConfig?.apiKey)
          ? { authorization: `Bearer ${resolveOptionalString(hookConfig?.apiKey)}` }
          : {}),
      },
      body: JSON.stringify({
        query: "openclaw orchestrator smoke check",
        limit: 1,
        agentId,
        resourceId: "smoke-check",
        threadId: "smoke-check",
      }),
      signal: abortController.signal,
    });
    const durationMs = Date.now() - startedAt;
    if (!response.ok) {
      return {
        status: "fail",
        summary: `search probe failed (${response.status})`,
        configured: true,
        enabled: true,
        searchUrl,
        durationMs,
        error: `HTTP ${response.status}`,
      };
    }

    const payload = await response.json().catch(() => null);
    if (!payload) {
      return {
        status: "fail",
        summary: "search probe returned invalid JSON",
        configured: true,
        enabled: true,
        searchUrl,
        durationMs,
        error: "Ethos /search returned an empty or invalid JSON body",
      };
    }

    const resultCount = countEthosResults(payload);
    return {
      status: "ok",
      summary: `search ok (${formatDurationPrecise(durationMs)}; ${resultCount} result${resultCount === 1 ? "" : "s"})`,
      configured: true,
      enabled: true,
      searchUrl,
      durationMs,
      resultCount,
    };
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    const isTimeout = error instanceof Error && error.name === "AbortError";
    return {
      status: "fail",
      summary: isTimeout ? "search probe timed out" : "search probe failed",
      configured: true,
      enabled: true,
      searchUrl,
      durationMs,
      error: formatErrorMessage(error),
    };
  } finally {
    clearTimeout(timeout);
  }
}

function evaluateGatewayCheck(result: TimedResult<unknown>): GatewayCheck {
  if (!result.ok) {
    return {
      status: "fail",
      summary: "gateway unreachable",
      reachable: false,
      durationMs: result.durationMs,
      error: result.error,
    };
  }

  return {
    status: "ok",
    summary: `gateway reachable (${formatDurationPrecise(result.durationMs)})`,
    reachable: true,
    durationMs: result.durationMs,
  };
}

function evaluateTaskLedgerCheck(
  result: TimedResult<TaskLedgerSnapshot>,
  staleAfterMs: number,
): TaskLedgerCheck {
  if (!result.ok) {
    return {
      status: "fail",
      summary: "task ledger unavailable",
      taskCount: 0,
      agentCount: 0,
      recentEventCount: 0,
      staleAfterMs,
      error: result.error,
    };
  }

  const snapshot = result.value;
  const lastEventAt = resolveLatestLedgerTimestamp(snapshot);
  if (!lastEventAt) {
    return {
      status: "warn",
      summary: "ledger reachable but empty",
      taskCount: snapshot.tasks.length,
      agentCount: snapshot.agents.length,
      recentEventCount: snapshot.recentEvents.length,
      lastEventId: snapshot.lastEventId ?? null,
      lastEventAt: null,
      lastEventAgeMs: null,
      staleAfterMs,
    };
  }

  const ageMs = Math.max(0, Date.now() - Date.parse(lastEventAt));
  const stale = ageMs > staleAfterMs;
  return {
    status: stale ? "fail" : "ok",
    summary: stale
      ? `last ledger activity ${formatDurationCompact(ageMs, { spaced: true }) ?? "n/a"} ago`
      : `last ledger activity ${formatDurationCompact(ageMs, { spaced: true }) ?? "0s"} ago`,
    taskCount: snapshot.tasks.length,
    agentCount: snapshot.agents.length,
    recentEventCount: snapshot.recentEvents.length,
    lastEventId: snapshot.lastEventId ?? null,
    lastEventAt,
    lastEventAgeMs: ageMs,
    staleAfterMs,
  };
}

function evaluateMissionControlCheck(params: {
  snapshotResult: TimedResult<TaskLedgerSnapshot>;
  eventsResult: TimedResult<TasksEventsResponse>;
}): MissionControlCheck {
  if (!params.snapshotResult.ok) {
    return {
      status: "fail",
      summary: "snapshot sync surface unavailable",
      error: params.snapshotResult.error,
    };
  }

  if (!params.eventsResult.ok) {
    return {
      status: "fail",
      summary: "event sync surface unavailable",
      snapshotLastEventId: params.snapshotResult.value.lastEventId ?? null,
      error: params.eventsResult.error,
    };
  }

  const snapshotLastEventId = params.snapshotResult.value.lastEventId ?? null;
  const latestEvent = Array.isArray(params.eventsResult.value.events)
    ? params.eventsResult.value.events.at(-1)
    : undefined;
  const eventStreamLastEventId = latestEvent?.id ?? null;

  // Mission Control projects the ledger via snapshot + event reads, so verify
  // those two read surfaces agree on the latest ledger record.
  if (!snapshotLastEventId && !eventStreamLastEventId) {
    return {
      status: "warn",
      summary: "snapshot and event stream reachable but ledger is empty",
      snapshotLastEventId,
      eventStreamLastEventId,
    };
  }

  if (snapshotLastEventId && snapshotLastEventId === eventStreamLastEventId) {
    return {
      status: "ok",
      summary: "snapshot and event stream agree on the latest ledger event",
      snapshotLastEventId,
      eventStreamLastEventId,
    };
  }

  return {
    status: "fail",
    summary: "snapshot/event sync mismatch",
    snapshotLastEventId,
    eventStreamLastEventId,
    error:
      snapshotLastEventId && eventStreamLastEventId
        ? `snapshot=${snapshotLastEventId} events=${eventStreamLastEventId}`
        : "One sync surface reported a latest event id while the other did not",
  };
}

function statusPrefix(status: SmokeStatus): string {
  if (status === "ok") {
    return "[ok]";
  }
  if (status === "warn") {
    return "[warn]";
  }
  return "[fail]";
}

function writeTextReport(runtime: RuntimeEnv, report: OrchestratorSmokeReport) {
  runtime.log("OpenClaw orchestrator smoke check");
  runtime.log(`${statusPrefix(report.gateway.status)} Gateway: ${report.gateway.summary}`);
  if (report.gateway.error) {
    runtime.log(`  ${report.gateway.error}`);
  }

  runtime.log(
    `${statusPrefix(report.taskLedger.status)} Task ledger: ${report.taskLedger.summary}`,
  );
  runtime.log(
    `  tasks=${report.taskLedger.taskCount} agents=${report.taskLedger.agentCount} recentEvents=${report.taskLedger.recentEventCount}`,
  );
  if (report.taskLedger.lastEventAt) {
    runtime.log(`  lastEventAt=${report.taskLedger.lastEventAt}`);
  }
  if (report.taskLedger.error) {
    runtime.log(`  ${report.taskLedger.error}`);
  }

  runtime.log(
    `${statusPrefix(report.missionControl.status)} Mission Control sync: ${report.missionControl.summary}`,
  );
  if (report.missionControl.snapshotLastEventId || report.missionControl.eventStreamLastEventId) {
    runtime.log(
      `  snapshotLastEventId=${report.missionControl.snapshotLastEventId ?? "n/a"} eventStreamLastEventId=${report.missionControl.eventStreamLastEventId ?? "n/a"}`,
    );
  }
  if (report.missionControl.error) {
    runtime.log(`  ${report.missionControl.error}`);
  }

  runtime.log(`${statusPrefix(report.ethos.status)} Ethos: ${report.ethos.summary}`);
  if (report.ethos.searchUrl) {
    runtime.log(`  searchUrl=${report.ethos.searchUrl}`);
  }
  if (report.ethos.error) {
    runtime.log(`  ${report.ethos.error}`);
  }

  runtime.log(
    `${report.ok ? "[ok]" : "[fail]"} Overall: ${report.ok ? "stack looks ready" : "stack needs attention"} (${formatDurationPrecise(report.durationMs)})`,
  );
}

export async function orchestratorSmokeCommand(
  opts: {
    json?: boolean;
    timeoutMs?: number;
    ledgerStaleAfterMs?: number;
  },
  runtime: RuntimeEnv,
): Promise<void> {
  const cfg = await readBestEffortConfig();
  const startedAt = Date.now();
  const gatewayTimeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const ledgerStaleAfterMs = opts.ledgerStaleAfterMs ?? DEFAULT_LEDGER_STALE_AFTER_MS;

  const [gatewayResult, snapshotResult, eventsResult, ethos] = await withProgress(
    {
      label: "Running orchestrator smoke check…",
      indeterminate: true,
      enabled: opts.json !== true,
    },
    async () =>
      await Promise.all([
        runTimed(
          async () =>
            await callGateway({
              method: "health",
              timeoutMs: gatewayTimeoutMs,
              config: cfg,
            }),
        ),
        runTimed(
          async () =>
            await callGateway<TaskLedgerSnapshot>({
              method: "tasks.snapshot",
              params: { recentEventLimit: TASK_LEDGER_RECENT_LIMIT },
              timeoutMs: gatewayTimeoutMs,
              config: cfg,
            }),
        ),
        runTimed(
          async () =>
            await callGateway<TasksEventsResponse>({
              method: "tasks.events",
              params: { limit: 1 },
              timeoutMs: gatewayTimeoutMs,
              config: cfg,
            }),
        ),
        probeEthosSearch({ cfg, timeoutMs: opts.timeoutMs }),
      ]),
  );

  const report: OrchestratorSmokeReport = {
    ok: true,
    checkedAt: new Date().toISOString(),
    durationMs: Date.now() - startedAt,
    gateway: evaluateGatewayCheck(gatewayResult),
    taskLedger: evaluateTaskLedgerCheck(snapshotResult, ledgerStaleAfterMs),
    missionControl: evaluateMissionControlCheck({
      snapshotResult,
      eventsResult,
    }),
    ethos,
  };

  report.ok =
    report.gateway.status !== "fail" &&
    report.taskLedger.status !== "fail" &&
    report.missionControl.status !== "fail" &&
    report.ethos.status !== "fail";

  if (opts.json) {
    writeRuntimeJson(runtime, report);
  } else {
    writeTextReport(runtime, report);
  }

  if (!report.ok) {
    runtime.exit(1);
  }
}

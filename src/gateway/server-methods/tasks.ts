import {
  type ReadTaskLedgerEventsOptions,
  readTaskLedgerEvents,
  readTaskLedgerSnapshot,
  publishTaskLedgerEvents,
} from "../../infra/task-ledger.js";
import { ErrorCodes, errorShape } from "../protocol/index.js";
import type { GatewayRequestHandlers } from "./types.js";

function parsePositiveInt(value: unknown): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return undefined;
  }
  const normalized = Math.floor(value);
  return normalized > 0 ? normalized : undefined;
}

function trimToUndefined(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

export const tasksHandlers: GatewayRequestHandlers = {
  "tasks.snapshot": async ({ params, respond }) => {
    const recentEventLimit =
      params.recentEventLimit === undefined ? undefined : parsePositiveInt(params.recentEventLimit);
    if (params.recentEventLimit !== undefined && recentEventLimit === undefined) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          "invalid tasks.snapshot params: recentEventLimit must be a positive integer",
        ),
      );
      return;
    }
    const snapshot = await readTaskLedgerSnapshot({ recentEventLimit });
    respond(true, snapshot, undefined);
  },
  "tasks.events": async ({ params, respond }) => {
    const limit = params.limit === undefined ? undefined : parsePositiveInt(params.limit);
    if (params.limit !== undefined && limit === undefined) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          "invalid tasks.events params: limit must be a positive integer",
        ),
      );
      return;
    }
    const taskId = trimToUndefined(params.taskId);
    const agentId = trimToUndefined(params.agentId);
    const options: ReadTaskLedgerEventsOptions = {
      ...(limit ? { limit } : {}),
      ...(taskId ? { taskId } : {}),
      ...(agentId ? { agentId } : {}),
    };
    const events = await readTaskLedgerEvents(options);
    respond(true, { events }, undefined);
  },
  "tasks.publish": async ({ params, respond, context }) => {
    if (!Array.isArray(params.events) || params.events.length === 0) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          "invalid tasks.publish params: events must be a non-empty array",
        ),
      );
      return;
    }
    const recentEventLimit =
      params.recentEventLimit === undefined ? undefined : parsePositiveInt(params.recentEventLimit);
    if (params.recentEventLimit !== undefined && recentEventLimit === undefined) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          "invalid tasks.publish params: recentEventLimit must be a positive integer",
        ),
      );
      return;
    }
    try {
      const result = await publishTaskLedgerEvents({
        events: params.events as never,
        recentEventLimit,
      });
      for (const event of result.events) {
        context.broadcast("tasks.ledger", event, { dropIfSlow: true });
      }
      respond(true, result, undefined);
    } catch (error) {
      respond(
        false,
        undefined,
        errorShape(
          ErrorCodes.INVALID_REQUEST,
          error instanceof Error ? error.message : String(error),
        ),
      );
    }
  },
};

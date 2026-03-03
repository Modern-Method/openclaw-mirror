import type { OpenClawConfig } from "../../../config/config.js";
import { createSubsystemLogger } from "../../../logging/subsystem.js";
import {
  resolveAgentIdFromSessionKey,
  resolveCanonicalResourceId,
} from "../../../routing/session-key.js";
import { resolveHookConfig } from "../../config.js";
import {
  isMessageReceivedEvent,
  isMessageSentEvent,
  type HookHandler,
  type MessageReceivedHookContext,
  type MessageSentHookContext,
} from "../../hooks.js";

const HOOK_KEY = "ethos-ingest";
const DEFAULT_TIMEOUT_MS = 1_500;
const log = createSubsystemLogger("hooks/ethos-ingest");

type EthosIngestConfig = {
  enabled?: boolean;
  ethosUrl?: string;
  apiKey?: string;
  timeoutMs?: number;
  canaryAgents?: string[];
};

function resolveOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : undefined;
}

function resolvePositiveInt(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? Math.floor(value)
    : fallback;
}

function resolveStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((entry) => (typeof entry === "string" ? entry.trim().toLowerCase() : ""))
    .filter(Boolean);
}

function normalizeAgentId(value?: string): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed.toLowerCase() : undefined;
}

function isCanaryAllowed(canaryAgents: string[], agentId?: string): boolean {
  if (canaryAgents.length === 0) {
    return false;
  }
  const normalizedAgentId = normalizeAgentId(agentId);
  if (!normalizedAgentId) {
    return false;
  }
  return canaryAgents.includes(normalizedAgentId);
}

function resolveEthosIngestConfig(cfg?: OpenClawConfig): EthosIngestConfig {
  const hookConfig = resolveHookConfig(cfg, HOOK_KEY) as EthosIngestConfig | undefined;
  return hookConfig ?? {};
}

function resolveIngestUrl(baseUrl: string): string | null {
  try {
    return new URL("/ingest", baseUrl).toString();
  } catch {
    return null;
  }
}

function compactRecord(input: Record<string, unknown>): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(input)) {
    if (value === undefined) {
      continue;
    }
    output[key] = value;
  }
  return output;
}

function resolveSenderId(
  context: MessageReceivedHookContext | MessageSentHookContext,
  isInbound: boolean,
): string | undefined {
  const explicitSenderId = resolveOptionalString(context.senderId);
  if (explicitSenderId) {
    return explicitSenderId;
  }
  if (isInbound) {
    return resolveOptionalString((context as MessageReceivedHookContext).from);
  }
  return resolveOptionalString((context as MessageSentHookContext).to);
}

async function postWithTimeout(params: {
  url: string;
  body: Record<string, unknown>;
  timeoutMs: number;
  apiKey?: string;
}): Promise<void> {
  const abortController = new AbortController();
  const timeout = setTimeout(() => {
    abortController.abort();
  }, params.timeoutMs);
  try {
    const response = await fetch(params.url, {
      method: "POST",
      headers: compactRecord({
        "content-type": "application/json",
        authorization: params.apiKey ? `Bearer ${params.apiKey}` : undefined,
      }),
      body: JSON.stringify(params.body),
      signal: abortController.signal,
    });
    if (!response.ok) {
      log.debug("Ethos ingest request returned non-OK status", {
        status: response.status,
      });
    }
  } finally {
    clearTimeout(timeout);
  }
}

const ethosIngestHook: HookHandler = async (event) => {
  const isInbound = isMessageReceivedEvent(event);
  const isOutbound = isMessageSentEvent(event);
  if (!isInbound && !isOutbound) {
    return;
  }

  const context = event.context;
  const cfg = context.cfg;
  const hookConfig = resolveEthosIngestConfig(cfg);
  if (hookConfig.enabled !== true) {
    return;
  }

  const ethosUrl = resolveOptionalString(hookConfig.ethosUrl);
  if (!ethosUrl) {
    return;
  }
  const ingestUrl = resolveIngestUrl(ethosUrl);
  if (!ingestUrl) {
    log.warn("Ethos ingest URL is invalid");
    return;
  }

  const agentId =
    resolveOptionalString(context.agentId) ?? resolveAgentIdFromSessionKey(event.sessionKey);
  const canaryAgents = resolveStringArray(hookConfig.canaryAgents);
  if (!isCanaryAllowed(canaryAgents, agentId)) {
    return;
  }

  const senderId = resolveSenderId(context, isInbound);
  const fallbackSenderId = senderId ?? "unknown";
  const resourceId = resolveCanonicalResourceId({
    identityLinks: cfg?.session?.identityLinks,
    channelId: context.channelId,
    senderId: fallbackSenderId,
  });

  const metadata = compactRecord({
    agentId,
    sessionKey: event.sessionKey,
    threadId: event.sessionKey,
    resourceId,
    channelId: context.channelId,
    accountId: context.accountId,
    conversationId: context.conversationId,
    messageId: context.messageId,
    senderId,
    from: isInbound
      ? resolveOptionalString((context as MessageReceivedHookContext).from)
      : resolveOptionalString((context as MessageSentHookContext).from),
    to: isInbound
      ? resolveOptionalString((context as MessageReceivedHookContext).to)
      : resolveOptionalString((context as MessageSentHookContext).to),
    eventTimestamp: event.timestamp.toISOString(),
    messageTimestamp:
      typeof context.timestamp === "number" && Number.isFinite(context.timestamp)
        ? context.timestamp
        : undefined,
    ingestTimestamp: Date.now(),
  });
  const timeoutMs = resolvePositiveInt(hookConfig.timeoutMs, DEFAULT_TIMEOUT_MS);

  try {
    await postWithTimeout({
      url: ingestUrl,
      timeoutMs,
      apiKey: resolveOptionalString(hookConfig.apiKey),
      body: {
        content: context.content,
        source: isInbound ? "user" : "assistant",
        metadata,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    log.debug("Ethos ingest request failed", { message });
  }
};

export default ethosIngestHook;

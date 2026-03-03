import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { OpenClawConfig } from "../../../config/config.js";
import type { HookHandler } from "../../hooks.js";
import { createHookEvent } from "../../hooks.js";

const logWarn = vi.fn();
const logDebug = vi.fn();

vi.mock("../../../logging/subsystem.js", () => ({
  createSubsystemLogger: () => ({
    warn: logWarn,
    debug: logDebug,
    info: vi.fn(),
    error: vi.fn(),
  }),
}));

let handler: HookHandler;
let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(async () => {
  ({ default: handler } = await import("./handler.js"));
  fetchMock = vi.fn(async () => ({ ok: true, status: 200 }));
  vi.stubGlobal("fetch", fetchMock);
  logWarn.mockClear();
  logDebug.mockClear();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function createConfig(overrides?: Record<string, unknown>): OpenClawConfig {
  return {
    session: {
      identityLinks: {
        michael: ["telegram:8480568759"],
      },
    },
    hooks: {
      internal: {
        entries: {
          "ethos-ingest": {
            enabled: true,
            ethosUrl: "http://127.0.0.1:8766",
            timeoutMs: 500,
            ...overrides,
          },
        },
      },
    },
  };
}

describe("ethos-ingest hook", () => {
  it("skips ingest when hook entry is disabled", async () => {
    const cfg = createConfig({ enabled: false });
    const event = createHookEvent("message", "received", "agent:main:main", {
      from: "8480568759",
      content: "hello",
      channelId: "telegram",
      cfg,
      agentId: "main",
    });

    await handler(event);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("posts inbound payload with canonical resourceId and metadata", async () => {
    const cfg = createConfig({ canaryAgents: ["main"], apiKey: "secret-token" });
    const event = createHookEvent("message", "received", "agent:main:main", {
      from: "8480568759",
      senderId: "8480568759",
      to: "telegram:bot",
      content: "where are you",
      channelId: "telegram",
      accountId: "acc-1",
      conversationId: "telegram:8480568759",
      messageId: "msg-1",
      timestamp: 1710000000000,
      cfg,
      agentId: "main",
    });

    await handler(event);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, request] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("http://127.0.0.1:8766/ingest");
    expect((request.headers as Record<string, string>).authorization).toBe("Bearer secret-token");

    expect(typeof request.body).toBe("string");
    const body = JSON.parse(request.body as string) as Record<string, unknown>;
    expect(body.content).toBe("where are you");
    expect(body.source).toBe("user");
    expect(body.metadata).toEqual(
      expect.objectContaining({
        agentId: "main",
        sessionKey: "agent:main:main",
        threadId: "agent:main:main",
        resourceId: "michael",
        channelId: "telegram",
        accountId: "acc-1",
        conversationId: "telegram:8480568759",
        messageId: "msg-1",
        senderId: "8480568759",
        from: "8480568759",
        to: "telegram:bot",
        messageTimestamp: 1710000000000,
      }),
    );
  });

  it("falls back to channel-prefixed resourceId when identity mapping is missing", async () => {
    const cfg = createConfig({ canaryAgents: ["main"] });
    const event = createHookEvent("message", "received", "agent:main:main", {
      from: "9999",
      senderId: "9999",
      content: "new sender",
      channelId: "telegram",
      cfg,
      agentId: "main",
    });

    await handler(event);

    const [, request] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(typeof request.body).toBe("string");
    const body = JSON.parse(request.body as string) as { metadata: Record<string, unknown> };
    expect(body.metadata.resourceId).toBe("telegram:9999");
  });

  it("denies ingest when canaryAgents is empty", async () => {
    const cfg = createConfig({ canaryAgents: [] });
    const event = createHookEvent("message", "sent", "agent:main:main", {
      to: "telegram:8480568759",
      content: "pong",
      success: true,
      channelId: "telegram",
      cfg,
      agentId: "main",
    });

    await handler(event);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("denies ingest when canaryAgents is not configured", async () => {
    const cfg = createConfig();
    const event = createHookEvent("message", "sent", "agent:main:main", {
      to: "telegram:8480568759",
      content: "pong",
      success: true,
      channelId: "telegram",
      cfg,
      agentId: "main",
    });

    await handler(event);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("skips non-canary agents", async () => {
    const cfg = createConfig({ canaryAgents: ["ops"] });
    const event = createHookEvent("message", "sent", "agent:main:main", {
      to: "telegram:8480568759",
      content: "pong",
      success: true,
      channelId: "telegram",
      cfg,
      agentId: "main",
    });

    await handler(event);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("fails open when the Ethos request throws", async () => {
    fetchMock.mockRejectedValueOnce(new Error("connection refused"));
    const cfg = createConfig({ canaryAgents: ["main"] });
    const event = createHookEvent("message", "sent", "agent:main:main", {
      to: "telegram:8480568759",
      content: "pong",
      success: true,
      channelId: "telegram",
      cfg,
      agentId: "main",
    });

    await expect(handler(event)).resolves.toBeUndefined();
    expect(logDebug).toHaveBeenCalledWith("Ethos ingest request failed", {
      message: "connection refused",
    });
  });
});

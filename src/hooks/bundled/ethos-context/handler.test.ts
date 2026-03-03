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
  fetchMock = vi.fn(async () => ({
    ok: true,
    status: 200,
    json: async () => ({
      results: [
        { id: "r1", content: "User prefers concise status updates", timestamp: 1710000000000 },
        { id: "r2", content: "Prefers UTC timestamps in summaries", timestamp: 1710000001000 },
        { id: "r3", content: "Uses telegram for urgent follow-ups", timestamp: 1710000002000 },
      ],
    }),
  }));
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
          "ethos-context": {
            enabled: true,
            ethosUrl: "http://127.0.0.1:8766",
            timeoutMs: 500,
            limit: 5,
            maxChars: 2000,
            ...overrides,
          },
        },
      },
    },
  };
}

describe("ethos-context hook", () => {
  it("skips when hook is disabled", async () => {
    const cfg = createConfig({ enabled: false });
    const event = createHookEvent("agent", "before_prompt_build", "agent:main:main", {
      prompt: "where did we leave off?",
      messages: [],
      cfg,
      agentId: "main",
    });

    await handler(event);
    expect(fetchMock).not.toHaveBeenCalled();
    expect((event.context as { prependContext?: unknown }).prependContext).toBeUndefined();
  });

  it("requests Ethos search and sets prependContext as untrusted block", async () => {
    const cfg = createConfig({ canaryAgents: ["main"], apiKey: "token-1" });
    const event = createHookEvent("agent", "before_prompt_build", "agent:main:main", {
      prompt: "prepare a status summary for michael",
      messages: [{ role: "user", content: "previous" }],
      cfg,
      agentId: "main",
      channelId: "telegram",
      senderId: "8480568759",
    });

    await handler(event);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, request] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("http://127.0.0.1:8766/search");
    expect((request.headers as Record<string, string>).authorization).toBe("Bearer token-1");

    expect(typeof request.body).toBe("string");
    const requestBody = JSON.parse(request.body as string) as Record<string, unknown>;
    expect(requestBody).toEqual(
      expect.objectContaining({
        query: "prepare a status summary for michael",
        limit: 5,
        threadId: "agent:main:main",
        resourceId: "michael",
      }),
    );

    const prependContext = (event.context as { prependContext?: unknown }).prependContext;
    expect(typeof prependContext).toBe("string");
    expect(String(prependContext)).toContain("Untrusted");
    expect(String(prependContext)).toContain("provenance:");
  });

  it("obeys maxChars budget when building prependContext", async () => {
    const cfg = createConfig({ maxChars: 220, canaryAgents: ["main"] });
    const event = createHookEvent("agent", "before_prompt_build", "agent:main:main", {
      prompt: "memory query",
      messages: [],
      cfg,
      agentId: "main",
    });

    await handler(event);

    const prependContext = String((event.context as { prependContext?: unknown }).prependContext);
    expect(prependContext.length).toBeLessThanOrEqual(220);
    expect(prependContext).toContain("Top memories:");
  });

  it("skips non-canary agents", async () => {
    const cfg = createConfig({ canaryAgents: ["ops"] });
    const event = createHookEvent("agent", "before_prompt_build", "agent:main:main", {
      prompt: "memory query",
      messages: [],
      cfg,
      agentId: "main",
    });

    await handler(event);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("fails open when search request throws", async () => {
    fetchMock.mockRejectedValueOnce(new Error("search unavailable"));
    const cfg = createConfig({ canaryAgents: ["main"] });
    const event = createHookEvent("agent", "before_prompt_build", "agent:main:main", {
      prompt: "memory query",
      messages: [],
      cfg,
      agentId: "main",
    });

    await expect(handler(event)).resolves.toBeUndefined();
    expect((event.context as { prependContext?: unknown }).prependContext).toBeUndefined();
    expect(logDebug).toHaveBeenCalledWith("Ethos context request failed", {
      message: "search unavailable",
    });
  });
});

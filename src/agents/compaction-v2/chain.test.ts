import { describe, expect, it } from "vitest";
import {
  appendNodeWithLimits,
  parseCompactionV2State,
  renderCompactionV2Summary,
  resolveDeltaMessages,
} from "./chain.js";

describe("compaction-v2 chain", () => {
  it("roundtrips parse/render", () => {
    const state = appendNodeWithLimits({
      prior: null,
      node: {
        fromEntryId: "a",
        toEntryId: "b",
        createdAt: new Date(0).toISOString(),
        summary: "hello",
        artifactIndex: { files: [], commands: [], refs: [] },
        ethosHints: [],
      },
      limits: {
        maxSummaryNodes: 6,
        maxNodeTokens: 900,
        maxChainTokens: 4000,
        mergePolicy: "mergeOldest",
      },
    });
    const rendered = renderCompactionV2Summary(state);
    const parsed = parseCompactionV2State(rendered);
    expect(parsed?.version).toBe("v2");
    expect(parsed?.chain).toHaveLength(1);
  });

  it("merges oldest when over maxSummaryNodes", () => {
    let state = null;
    for (let i = 0; i < 4; i++) {
      state = appendNodeWithLimits({
        prior: state,
        node: {
          fromEntryId: `${i}`,
          toEntryId: `${i}`,
          createdAt: new Date().toISOString(),
          summary: `summary-${i}`,
          artifactIndex: { files: [], commands: [], refs: [] },
          ethosHints: [],
        },
        limits: {
          maxSummaryNodes: 2,
          maxNodeTokens: 900,
          maxChainTokens: 4000,
          mergePolicy: "mergeOldest",
        },
      });
    }
    expect(state?.chain.length).toBeLessThanOrEqual(2);
    expect(state?.chain[0]?.anchorId).toContain("+");
  });

  it("supports mergePairs policy", () => {
    let state = null;
    for (let i = 0; i < 5; i++) {
      state = appendNodeWithLimits({
        prior: state,
        node: {
          fromEntryId: `${i}`,
          toEntryId: `${i}`,
          createdAt: new Date().toISOString(),
          summary: `node-${i}`,
          artifactIndex: { files: [], commands: [], refs: [] },
          ethosHints: [],
        },
        limits: {
          maxSummaryNodes: 2,
          maxNodeTokens: 900,
          maxChainTokens: 4000,
          mergePolicy: "mergePairs",
        },
      });
    }
    expect(state?.chain.length).toBeLessThanOrEqual(2);
  });

  it("truncates node summaries to fit maxNodeTokens (best effort)", () => {
    const long = "x".repeat(20_000);
    const state = appendNodeWithLimits({
      prior: null,
      node: {
        fromEntryId: "a",
        toEntryId: "b",
        createdAt: new Date(0).toISOString(),
        summary: long,
        artifactIndex: { files: [], commands: [], refs: [] },
        ethosHints: [],
      },
      limits: {
        maxSummaryNodes: 6,
        maxNodeTokens: 10,
        maxChainTokens: 4000,
        mergePolicy: "mergeOldest",
      },
    });
    expect(state.chain[0]?.summary).toContain("[truncated to fit node budget]");
  });

  it("resolveDeltaMessages produces stable entry ids based on timestamps", () => {
    const messages = [
      { role: "user", timestamp: 111 },
      { role: "assistant", timestamp: 222 },
      { role: "user", timestamp: 222 },
    ];
    const full = resolveDeltaMessages(messages);
    expect(full?.fromEntryId).toBe("111:0");
    expect(full?.toEntryId).toBe("222:2");

    const delta = resolveDeltaMessages(messages, "222:1");
    expect(delta?.fromEntryId).toBe("222:2");
    expect(delta?.toEntryId).toBe("222:2");
  });
});

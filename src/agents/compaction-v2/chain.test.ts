import { describe, expect, it } from "vitest";
import { appendNodeWithLimits, parseCompactionV2State, renderCompactionV2Summary } from "./chain.js";

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
      limits: { maxSummaryNodes: 6, maxNodeTokens: 900, maxChainTokens: 4000, mergePolicy: "mergeOldest" },
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
        limits: { maxSummaryNodes: 2, maxNodeTokens: 900, maxChainTokens: 4000, mergePolicy: "mergeOldest" },
      });
    }
    expect(state?.chain.length).toBeLessThanOrEqual(2);
  });
});

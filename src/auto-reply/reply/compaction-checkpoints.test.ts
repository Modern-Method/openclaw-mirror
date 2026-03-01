import fs from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it } from "vitest";
import { appendCompactionCheckpoint } from "./compaction-checkpoints.js";

describe("compaction checkpoints", () => {
  it("is idempotent by key", async () => {
    const dir = await fs.mkdtemp(path.join(tmpdir(), "checkpoint-"));
    const cfg = { agents: { defaults: { compaction: { v2: { enabled: true } } } } } as const;
    await appendCompactionCheckpoint({
      workspaceDir: dir,
      cfg,
      idempotencyKey: "s:1:compaction",
      sessionId: "s",
      kind: "compaction",
      payload: "payload",
      nowMs: Date.parse("2026-03-02T00:00:00Z"),
    });
    await appendCompactionCheckpoint({
      workspaceDir: dir,
      cfg,
      idempotencyKey: "s:1:compaction",
      sessionId: "s",
      kind: "compaction",
      payload: "payload",
      nowMs: Date.parse("2026-03-02T00:01:00Z"),
    });
    const file = path.join(dir, "memory/checkpoints/2026-03-02.md");
    const text = await fs.readFile(file, "utf8");
    expect(text.match(/idempotency-key: s:1:compaction/g)?.length ?? 0).toBe(1);
  });
});

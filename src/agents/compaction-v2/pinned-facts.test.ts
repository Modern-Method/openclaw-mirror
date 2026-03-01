import fs from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it } from "vitest";
import { loadPinnedFacts } from "./pinned-facts.js";

describe("pinned-facts", () => {
  it("returns null when missing", async () => {
    const got = await loadPinnedFacts({ workspaceDir: tmpdir(), pinnedFactsPath: "nope.md" });
    expect(got).toBeNull();
  });

  it("loads file and stable hash", async () => {
    const dir = await fs.mkdtemp(path.join(tmpdir(), "pinned-"));
    await fs.mkdir(path.join(dir, "memory"), { recursive: true });
    await fs.writeFile(path.join(dir, "memory/pinned.md"), "abc\n");
    const a = await loadPinnedFacts({ workspaceDir: dir, pinnedFactsPath: "memory/pinned.md" });
    const b = await loadPinnedFacts({ workspaceDir: dir, pinnedFactsPath: "memory/pinned.md" });
    expect(a?.hash).toBe(b?.hash);
    expect(a?.text).toContain("abc");
  });
});

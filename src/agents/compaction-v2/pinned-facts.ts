import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

export type LoadedPinnedFacts = {
  path: string;
  hash: string;
  text: string;
  truncated: boolean;
};

const MAX_PINNED_FACTS_BYTES = 32 * 1024;
const MAX_PINNED_FACTS_CHARS = 12_000;

export async function loadPinnedFacts(params: {
  workspaceDir: string;
  pinnedFactsPath?: string;
}): Promise<LoadedPinnedFacts | null> {
  const rel = params.pinnedFactsPath?.trim() || "memory/pinned.md";
  const resolved = path.isAbsolute(rel) ? rel : path.resolve(params.workspaceDir, rel);

  let raw: Buffer;
  try {
    raw = await fs.readFile(resolved);
  } catch {
    return null;
  }

  const clipped = raw.length > MAX_PINNED_FACTS_BYTES ? raw.subarray(0, MAX_PINNED_FACTS_BYTES) : raw;
  let text = clipped.toString("utf8");
  let truncated = raw.length > MAX_PINNED_FACTS_BYTES;
  if (text.length > MAX_PINNED_FACTS_CHARS) {
    text = text.slice(0, MAX_PINNED_FACTS_CHARS);
    truncated = true;
  }

  const hash = `sha256:${crypto.createHash("sha256").update(raw).digest("hex")}`;
  return { path: rel, hash, text: text.trim(), truncated };
}

export function formatPinnedFactsBlock(pinned: LoadedPinnedFacts | null): string {
  if (!pinned || !pinned.text) {
    return "";
  }
  return `Pinned facts (verbatim, do not paraphrase):\n<${pinned.path}>\n${pinned.text}\n</${pinned.path}>`;
}

import fs from "node:fs/promises";
import path from "node:path";
import type { OpenClawConfig } from "../../config/config.js";

function formatDate(nowMs: number): string {
  return new Date(nowMs).toISOString().slice(0, 10);
}

function resolveLedgerPath(params: { workspaceDir: string; cfg?: OpenClawConfig; nowMs?: number }): string {
  const nowMs = params.nowMs ?? Date.now();
  const p =
    params.cfg?.agents?.defaults?.compaction?.v2?.checkpointLedgerPath ??
    "memory/checkpoints/YYYY-MM-DD.md";
  const dated = p.replaceAll("YYYY-MM-DD", formatDate(nowMs));
  return path.isAbsolute(dated) ? dated : path.resolve(params.workspaceDir, dated);
}

export async function appendCompactionCheckpoint(params: {
  workspaceDir: string;
  cfg?: OpenClawConfig;
  idempotencyKey: string;
  sessionId: string;
  kind: "memory-flush" | "compaction";
  payload: string;
  ethosHints?: string[];
  nowMs?: number;
}): Promise<{ wrote: boolean; path: string }> {
  if (params.cfg?.agents?.defaults?.compaction?.v2?.checkpointLedgerEnabled === false) {
    return { wrote: false, path: resolveLedgerPath(params) };
  }
  const filePath = resolveLedgerPath(params);
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  let existing = "";
  try {
    existing = await fs.readFile(filePath, "utf8");
  } catch {}
  if (existing.includes(`idempotency-key: ${params.idempotencyKey}`)) {
    return { wrote: false, path: filePath };
  }
  const ts = new Date(params.nowMs ?? Date.now()).toISOString();
  const hints = params.ethosHints?.length ? params.ethosHints.join(", ") : "";
  const block = [
    `\n## ${params.kind} checkpoint (${ts})`,
    `- idempotency-key: ${params.idempotencyKey}`,
    `- session-id: ${params.sessionId}`,
    hints ? `- ethos-hints: ${hints}` : "",
    "",
    params.payload.trim(),
    "",
  ]
    .filter(Boolean)
    .join("\n");
  await fs.appendFile(filePath, block, "utf8");
  return { wrote: true, path: filePath };
}

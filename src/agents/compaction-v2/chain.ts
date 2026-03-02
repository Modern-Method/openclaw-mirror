import { estimateMessagesTokens } from "../compaction.js";
import { extractCompactionV2Json, renderCompactionV2Envelope } from "./template.js";

export type CompactionV2Node = {
  anchorId: string;
  fromEntryId: string;
  toEntryId: string;
  createdAt: string;
  phase?: string;
  summary: string;
  artifactIndex: { files: string[]; commands: string[]; refs: string[] };
  ethosHints: string[];
};

export type CompactionV2State = {
  version: "v2";
  pinned?: { path: string; hash: string };
  chain: CompactionV2Node[];
  firstKeptEntryId?: string;
  stats?: { tokensBefore?: number; tokensAfter?: number };
};

export type CompactionV2Limits = {
  maxSummaryNodes: number;
  maxNodeTokens: number;
  maxChainTokens: number;
  mergePolicy: "mergeOldest" | "mergePairs";
};

export function parseCompactionV2State(summary?: string): CompactionV2State | null {
  const json = extractCompactionV2Json(summary);
  if (!json) {
    return null;
  }
  try {
    const parsed = JSON.parse(json) as CompactionV2State;
    if (parsed?.version !== "v2" || !Array.isArray(parsed.chain)) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function nodeTokenEstimate(node: CompactionV2Node): number {
  return estimateMessagesTokens([{ role: "user", content: node.summary, timestamp: Date.now() }]);
}

function trimSummaryByTokens(text: string, maxNodeTokens: number): string {
  const est = estimateMessagesTokens([{ role: "user", content: text, timestamp: Date.now() }]);
  if (est <= maxNodeTokens) {
    return text;
  }
  const ratio = Math.max(0.1, maxNodeTokens / Math.max(1, est));
  const keep = Math.max(200, Math.floor(text.length * ratio));
  return `${text.slice(0, keep)}\n\n[truncated to fit node budget]`;
}

function mergeNodePair(a: CompactionV2Node, b: CompactionV2Node): CompactionV2Node {
  return {
    anchorId: `${a.anchorId}+${b.anchorId}`,
    fromEntryId: a.fromEntryId,
    toEntryId: b.toEntryId,
    createdAt: b.createdAt,
    phase: b.phase,
    summary: `${a.summary}\n\n---\n\n${b.summary}`,
    artifactIndex: {
      files: [...new Set([...(a.artifactIndex.files ?? []), ...(b.artifactIndex.files ?? [])])],
      commands: [
        ...new Set([...(a.artifactIndex.commands ?? []), ...(b.artifactIndex.commands ?? [])]),
      ],
      refs: [...new Set([...(a.artifactIndex.refs ?? []), ...(b.artifactIndex.refs ?? [])])],
    },
    ethosHints: [...new Set([...(a.ethosHints ?? []), ...(b.ethosHints ?? [])])],
  };
}

function mergeOldest(nodes: CompactionV2Node[]): CompactionV2Node[] {
  if (nodes.length < 2) {
    return nodes;
  }
  const [a, b, ...rest] = nodes;
  return [mergeNodePair(a, b), ...rest];
}

function mergePairs(nodes: CompactionV2Node[]): CompactionV2Node[] {
  if (nodes.length < 2) {
    return nodes;
  }
  const merged: CompactionV2Node[] = [];
  for (let i = 0; i < nodes.length; i += 2) {
    const a = nodes[i];
    const b = nodes[i + 1];
    merged.push(b ? mergeNodePair(a, b) : a);
  }
  return merged;
}

function totalChainTokens(chain: CompactionV2Node[]): number {
  return chain.reduce((sum, node) => sum + nodeTokenEstimate(node), 0);
}

export function appendNodeWithLimits(params: {
  prior: CompactionV2State | null;
  node: Omit<CompactionV2Node, "anchorId" | "summary"> & { summary: string };
  limits: CompactionV2Limits;
  pinned?: { path: string; hash: string };
  firstKeptEntryId?: string;
  tokensBefore?: number;
  tokensAfter?: number;
}): CompactionV2State {
  const priorChain = params.prior?.chain ?? [];
  const anchorId = `a-${String(priorChain.length + 1).padStart(4, "0")}`;
  let chain = [
    ...priorChain,
    {
      ...params.node,
      anchorId,
      summary: trimSummaryByTokens(params.node.summary, params.limits.maxNodeTokens),
    },
  ];

  while (
    chain.length > params.limits.maxSummaryNodes ||
    totalChainTokens(chain) > params.limits.maxChainTokens
  ) {
    chain = params.limits.mergePolicy === "mergePairs" ? mergePairs(chain) : mergeOldest(chain);
  }

  return {
    version: "v2",
    pinned: params.pinned ?? params.prior?.pinned,
    chain,
    firstKeptEntryId: params.firstKeptEntryId,
    stats: { tokensBefore: params.tokensBefore, tokensAfter: params.tokensAfter },
  };
}

export function renderCompactionV2Summary(state: CompactionV2State): string {
  const human = [
    "Compaction v2 anchored chain",
    `- Nodes: ${state.chain.length}`,
    ...(state.chain.length > 0
      ? [
          "",
          ...state.chain.map(
            (node, idx) =>
              `### ${idx + 1}. ${node.anchorId} (${node.fromEntryId} → ${node.toEntryId})\n${node.summary}`,
          ),
        ]
      : []),
  ].join("\n");
  return renderCompactionV2Envelope({ stateJson: JSON.stringify(state), humanSummary: human });
}

export function resolveDeltaMessages<T extends { timestamp?: number }>(
  messages: T[],
  priorToEntryId?: string,
): { delta: T[]; fromEntryId: string; toEntryId: string } | null {
  if (!messages.length) {
    return null;
  }
  const ids = messages.map((m, i) => `${m.timestamp ?? 0}:${i}`);
  let start = 0;
  if (priorToEntryId) {
    const idx = ids.lastIndexOf(priorToEntryId);
    if (idx >= 0 && idx + 1 < messages.length) {
      start = idx + 1;
    }
  }
  const delta = messages.slice(start);
  if (!delta.length) {
    return null;
  }
  return { delta, fromEntryId: ids[start], toEntryId: ids[ids.length - 1] };
}

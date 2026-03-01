export const COMPACTION_V2_MARKER_START = "<!-- OPENCLAW_COMPACTION_V2";
export const COMPACTION_V2_MARKER_END = "-->";

export function renderCompactionV2Envelope(params: { stateJson: string; humanSummary: string }): string {
  return `${COMPACTION_V2_MARKER_START}\n${params.stateJson}\n${COMPACTION_V2_MARKER_END}\n\n${params.humanSummary}`.trim();
}

export function extractCompactionV2Json(summary: string | undefined): string | null {
  if (!summary) return null;
  const start = summary.indexOf(COMPACTION_V2_MARKER_START);
  if (start < 0) return null;
  const jsonStart = start + COMPACTION_V2_MARKER_START.length;
  const end = summary.indexOf(COMPACTION_V2_MARKER_END, jsonStart);
  if (end < 0) return null;
  return summary.slice(jsonStart, end).trim() || null;
}

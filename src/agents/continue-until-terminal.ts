export type ContinueUntilTerminalState =
  | "done"
  | "blocked_by_input"
  | "unsafe_to_proceed"
  | "repeated_failure";

export function resolveContinueUntilTerminalState(
  params: {
    errorKind?: string;
    stopReason?: string;
    didSendDeterministicApprovalPrompt?: boolean;
    unsafeToProceed?: boolean;
  } = {},
): ContinueUntilTerminalState | undefined {
  if (params.unsafeToProceed) {
    return "unsafe_to_proceed";
  }
  if (params.stopReason === "tool_calls") {
    return undefined;
  }
  if (params.didSendDeterministicApprovalPrompt) {
    return "blocked_by_input";
  }
  switch (params.errorKind) {
    case "image_size":
      return "blocked_by_input";
    case "context_overflow":
    case "compaction_failure":
    case "role_ordering":
    case "retry_limit":
      return "repeated_failure";
    default:
      return "done";
  }
}

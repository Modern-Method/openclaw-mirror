import { describe, expect, it } from "vitest";
import { resolveContinueUntilTerminalState } from "./continue-until-terminal.js";

describe("resolveContinueUntilTerminalState", () => {
  it("returns done for a normal completion", () => {
    expect(resolveContinueUntilTerminalState({})).toBe("done");
  });

  it("treats approval-pending runs as blocked by input", () => {
    expect(
      resolveContinueUntilTerminalState({
        didSendDeterministicApprovalPrompt: true,
      }),
    ).toBe("blocked_by_input");
  });

  it("treats tool-call checkpoints as non-terminal", () => {
    expect(
      resolveContinueUntilTerminalState({
        stopReason: "tool_calls",
      }),
    ).toBeUndefined();
  });

  it("keeps tool-call checkpoints non-terminal even when approval prompting was used", () => {
    expect(
      resolveContinueUntilTerminalState({
        stopReason: "tool_calls",
        didSendDeterministicApprovalPrompt: true,
      }),
    ).toBeUndefined();
  });

  it("treats image-size errors as blocked by input", () => {
    expect(
      resolveContinueUntilTerminalState({
        errorKind: "image_size",
      }),
    ).toBe("blocked_by_input");
  });

  it("treats retry-limit failures as repeated failures", () => {
    expect(
      resolveContinueUntilTerminalState({
        errorKind: "retry_limit",
      }),
    ).toBe("repeated_failure");
  });

  it("supports explicit unsafe terminal states", () => {
    expect(
      resolveContinueUntilTerminalState({
        unsafeToProceed: true,
      }),
    ).toBe("unsafe_to_proceed");
  });
});

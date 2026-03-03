---
name: ethos-context
description: "Inject scoped, untrusted Ethos recall JSON before prompt build"
metadata:
  {
    "openclaw":
      {
        "emoji": "📚",
        "events": ["agent:before_prompt_build"],
        "install": [{ "id": "bundled", "kind": "bundled", "label": "Bundled with OpenClaw" }],
      },
  }
---

# Ethos Context Hook

Queries Ethos `/search` and prepends a bounded **hard-delimited JSON block** with untrusted recall data before prompt build.

## Default Behavior

This hook is **disabled by default**. Enable it explicitly with `hooks.internal.entries.ethos-context.enabled`.

## Configuration

```json
{
  "hooks": {
    "internal": {
      "entries": {
        "ethos-context": {
          "enabled": true,
          "ethosUrl": "http://127.0.0.1:8766",
          "apiKey": "YOUR_ETHOS_API_KEY",
          "timeoutMs": 1500,
          "maxChars": 2500,
          "limit": 5,
          "canaryAgents": ["main"]
        }
      }
    }
  }
}
```

### Options

- `enabled`: master switch for this hook.
- `ethosUrl`: Ethos base URL (the hook posts to `/search`).
- `apiKey`: optional bearer token for Ethos API auth.
- `timeoutMs`: strict request timeout in milliseconds.
- `maxChars`: max prepend context size.
- `limit`: max number of Ethos memories to request/use.
- `canaryAgents`: **required allowlist** of agent IDs allowed to inject context.
  - Empty/missing `canaryAgents` means **no agents are allowed** (safe default).

## Search Scoping

The hook sends scoped search filters in the request body:

- `query`
- `limit`
- `agentId`
- `threadId` (session key)
- `resourceId` (canonical identity resolved from `session.identityLinks`, `channelId`, `senderId`)

Ethos response parsing expects:

- recall entries under `results[]` (with compatibility fallbacks)
- stored ingest metadata under `results[].metadata`
- retrieval signals under `results[].retrieval` and `results[].metadata_scores`

## Prompt-Injection Hardening

Injected recall is rendered as:

- a hard start delimiter + hard end delimiter
- a single JSON payload (data-only)
- explicit instruction that recalled memories are untrusted quoted data
- `memories` encoded as a JSON array (no markdown headings/lists)
- delimiter collision escaping inside string fields

## Circuit Breaker

To avoid repeated failing calls, the hook uses a simple fail-open breaker:

- if search fails **3 times within 30 seconds**,
- skip context injection for **60 seconds**,
- then retry automatically.

## Failure Mode

Fail-open: if Ethos is unavailable, times out, or the circuit breaker is open, prompt build proceeds unchanged.

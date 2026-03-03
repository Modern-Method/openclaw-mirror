---
name: ethos-ingest
description: "Ingest inbound and outbound messages into Ethos for memory indexing"
metadata:
  {
    "openclaw":
      {
        "emoji": "🧠",
        "events": ["message:received", "message:sent"],
        "install": [{ "id": "bundled", "kind": "bundled", "label": "Bundled with OpenClaw" }],
      },
  }
---

# Ethos Ingest Hook

Posts inbound and outbound message events to an Ethos ingestion endpoint.

## Default Behavior

This hook is **disabled by default**. Enable it explicitly with `hooks.internal.entries.ethos-ingest.enabled`.

## Configuration

```json
{
  "hooks": {
    "internal": {
      "entries": {
        "ethos-ingest": {
          "enabled": true,
          "ethosUrl": "http://127.0.0.1:8766",
          "apiKey": "YOUR_ETHOS_API_KEY",
          "timeoutMs": 1500,
          "canaryAgents": ["main"]
        }
      }
    }
  }
}
```

### Options

- `enabled`: master switch for this hook.
- `ethosUrl`: Ethos base URL (the hook posts to `/ingest`).
- `apiKey`: optional bearer token for Ethos API auth.
- `timeoutMs`: strict request timeout in milliseconds.
- `canaryAgents`: required allowlist of agent IDs allowed to emit ingest calls.
  - Empty/missing `canaryAgents` means no agents are allowed (safe default).

## Failure Mode

Fail-open: if Ethos is unavailable or times out, OpenClaw messaging flow continues.

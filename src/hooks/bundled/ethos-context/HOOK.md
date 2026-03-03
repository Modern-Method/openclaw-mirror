---
name: ethos-context
description: "Inject untrusted Ethos memory recall before prompt build"
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

Queries Ethos recall and prepends a bounded, untrusted memory block before prompt build.

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
- `canaryAgents`: optional list of agent IDs allowed to inject context.

## Failure Mode

Fail-open: if Ethos is unavailable or times out, prompt build proceeds unchanged.

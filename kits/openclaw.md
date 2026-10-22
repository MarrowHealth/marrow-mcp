# OpenClaw

OpenClaw speaks Streamable HTTP natively, so the mirror plugs in with no
bridge. Config file: `~/.openclaw/openclaw.json` (JSON5). Add or merge:

```json
{
  "mcp": {
    "servers": {
      "marrow": {
        "url": "http://<server-ip>:8800/mcp",
        "transport": "streamable-http",
        "headers": { "Authorization": "Bearer ${MARROW_TOKEN}" },
        "connectionTimeoutMs": 5000,
        "requestTimeoutMs": 30000
      }
    }
  }
}
```

Steps:

1. Export `MARROW_TOKEN` in the gateway's environment, or paste the literal
   token in place of `${MARROW_TOKEN}` (headers are redacted in logs).
2. Or write the same entry from the CLI:
   `openclaw mcp set marrow '{"url":"http://<server-ip>:8800/mcp","transport":"streamable-http","headers":{"Authorization":"Bearer <token>"}}'`
3. Check with `openclaw mcp list` and `openclaw mcp show marrow`.
4. With Gateway hot reload on there is nothing to restart. Otherwise restart
   the gateway.

Always write `transport` explicitly. Then ask the agent "How am I doing
today?" and it calls `daily_brief`.

Verified against docs.openclaw.ai (gateway configuration reference, config
extensions, tools/mcp, cli/mcp) on 2026-10-22. `openclaw mcp add` also
accepts `--header`, but check `--help` for its argument form.

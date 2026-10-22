# OpenClaw

OpenClaw speaks Streamable HTTP natively, with one rule: plain `http://`
is accepted only for loopback hosts (localhost, 127.0.0.1). A LAN address
like `http://192.168.1.20:21212/mcp` is refused. So:

- The mirror behind HTTPS (`tailscale serve`, Caddy, Cloudflare Tunnel):
  point OpenClaw at it directly.
- The phone's Local MCP, or a mirror on plain http: go through the
  `mcp-remote` bridge, which runs on the OpenClaw machine and carries the
  header.

Config file: `~/.openclaw/openclaw.json` (JSON5).

## Direct (HTTPS)

```json
{
  "mcp": {
    "servers": {
      "marrow": {
        "url": "https://<your-mirror>/mcp",
        "transport": "streamable-http",
        "headers": { "Authorization": "Bearer ${MARROW_TOKEN}" },
        "connectionTimeoutMs": 5000,
        "requestTimeoutMs": 30000
      }
    }
  }
}
```

Or from the CLI:
`openclaw mcp set marrow '{"url":"https://<your-mirror>/mcp","transport":"streamable-http","headers":{"Authorization":"Bearer <token>"}}'`

## Through the bridge (the phone, or plain http)

Needs Node on the OpenClaw machine. The bridge is a stdio server that
forwards to the phone:

```json
{
  "mcp": {
    "servers": {
      "marrow": {
        "command": "npx",
        "args": ["-y", "mcp-remote", "http://<phone-ip>:21212/mcp", "--allow-http",
                 "--header", "Authorization: Bearer <token>"]
      }
    }
  }
}
```

Then `openclaw mcp list` and `openclaw mcp show marrow` to check. With
Gateway hot reload on there is nothing to restart. Ask the agent "How am I
doing today?" and it calls `daily_brief`.

Verified against docs.openclaw.ai (cli/mcp/transports, gateway
configuration reference, config extensions) and the mcp-remote README on
2026-10-22. `openclaw mcp add` also accepts `--header`, but check `--help`
for its argument form.

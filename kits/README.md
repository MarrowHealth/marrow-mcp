# Kits: point your agent at Marrow

One page per runtime, copy and paste. Every kit talks to the same server:
the self-hosted mirror at `http://<server-ip>:8800/mcp` with the bearer
token it printed on first run (`.mcp-token` in its data directory), or the
phone's own server at `http://<phone-ip>:21212/mcp` with the token shown
under Export, Local MCP. Only the URL and token differ.

| Runtime | Kit |
|---|---|
| Claude Code | [claude-code.md](claude-code.md) |
| Claude Desktop | [claude-desktop.md](claude-desktop.md) (one-click bundle, or mcp-remote) |
| Cursor | [cursor.md](cursor.md) |
| OpenClaw | [openclaw.md](openclaw.md) |
| Hermes Agent | [hermes.md](hermes.md) |
| Anything with an `mcp.json` | the block in [cursor.md](cursor.md) |

For ChatGPT, Claude.ai and Gemini, which run in their makers' clouds and
cannot reach a server on your LAN, use Marrow Connect:
<https://marrowhealthapp.com/connect>. Claude users can pick Marrow Health
from the connectors directory.

Once connected, start with `daily_brief`. It scores today against your own
trailing normal with the same math the app's Home page uses, so the agent
begins from a read that is already true for you. The four prompts
(`how_am_i_doing`, `weekly_review`, `doctor_prep`, `log_from_text`) are
there for clients that show prompts.

Reaching the server from outside your LAN: `tailscale serve --bg
--https=8800 http://127.0.0.1:8800`, then use the tailnet URL. Never
port-forward it raw.

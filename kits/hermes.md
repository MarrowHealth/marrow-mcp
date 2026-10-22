# Hermes Agent

Hermes Agent (Nous Research) takes a remote HTTP MCP server with a header
in `~/.hermes/config.yaml`. Add:

```yaml
mcp_servers:
  marrow:
    url: "http://<server-ip>:8800/mcp"
    headers:
      Authorization: "Bearer ${MARROW_TOKEN}"
    enabled: true
    timeout: 120
    connect_timeout: 30
```

Steps:

1. Put `MARROW_TOKEN=...` in `~/.hermes/.env`, or write the literal
   `Bearer <token>` in the file.
2. Or from the CLI, which writes the same block and prompts for the token:
   `hermes mcp add marrow --url http://<server-ip>:8800/mcp --auth header`
3. `hermes mcp test marrow`, then `hermes mcp list`.
4. In a running chat type `/reload-mcp`. Tools appear as
   `mcp__marrow__<tool>`, so `mcp__marrow__daily_brief` is the first one
   to try.

Do not set `transport`: omitted means Streamable HTTP, and `sse` is only
for legacy servers. No bridge needed.

Verified against hermes-agent.nousresearch.com (features/mcp and the MCP
config reference) on 2026-10-22. The `--auth header` CLI form comes from a
third-party guide, so confirm with `hermes mcp add --help`.

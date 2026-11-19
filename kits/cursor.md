# Cursor (and anything that reads an mcp.json)

Cursor, Settings, MCP, Add new global MCP server, or write the file
yourself: `~/.cursor/mcp.json` for every project, `.cursor/mcp.json` inside
one project.

```json
{
  "mcpServers": {
    "marrow": {
      "url": "http://<server-ip>:8800/mcp",
      "headers": { "Authorization": "Bearer <mcp token>" }
    }
  }
}
```

Cursor shows a green dot and the tool list once it has connected. Ask in
the agent pane: "Call daily_brief and tell me what stands out today."

The same block works for Windsurf, Cline, Continue, Zed and most other
clients that take a Streamable HTTP server with headers. Some older clients
want `"type": "http"` or `"transport": "streamable-http"` beside the URL.
Add it if the client ignores the entry.

## As a Cursor plugin

This repo is also a Cursor plugin (`.cursor-plugin/marketplace.json`, plugin in
`plugins/marrow-health`). It adds the server plus four commands (how am I doing,
weekly review, doctor prep, log) and a skill that knows which tool answers what.
Set two environment variables before you start Cursor:

```sh
export MARROW_MCP_URL="http://<server-ip>:8800/mcp"
export MARROW_MCP_TOKEN="<mcp token>"
```

The token is the one `marrow-server` prints on first run.

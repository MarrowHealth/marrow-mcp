# Claude Desktop

## One click

Download
[marrow.mcpb](https://github.com/skeletonarmytech/marrow-mcp/releases/latest/download/marrow.mcpb)
and double-click it. Claude Desktop installs the extension and asks for the
URL and token. Paste `http://<server-ip>:8800/mcp` and the mirror's token
(or the phone's URL and token from Export, Local MCP).

## By hand (mcp-remote)

Claude Desktop's config file takes local servers only, so a remote one goes
through the `mcp-remote` bridge. Settings, Developer, Edit Config, then add:

```json
{
  "mcpServers": {
    "marrow": {
      "command": "npx",
      "args": [
        "-y", "mcp-remote",
        "http://<server-ip>:8800/mcp",
        "--header", "Authorization: Bearer <mcp token>"
      ]
    }
  }
}
```

Restart Claude Desktop. The tools, prompts and resources appear under the
tools button in a new chat.

## The hosted copy instead (Marrow Connect, Pro)

Claude Desktop shares Claude.ai's connectors. In Claude Desktop or at
claude.ai: Settings, Connectors, pick Marrow Health from the directory (or
Add custom connector with `https://connect.marrowhealthapp.com/mcp`), then
sign in and approve from the phone. Details:
<https://marrowhealthapp.com/connect#claude>

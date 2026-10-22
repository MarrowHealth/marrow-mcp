# Claude Code

One command, user scope (every project):

```sh
claude mcp add --transport http --scope user marrow \
  http://<server-ip>:8800/mcp \
  --header "Authorization: Bearer <mcp token>"
```

Or a project file, `.mcp.json` at the repo root, with the token in an
environment variable so it never lands in git:

```json
{
  "mcpServers": {
    "marrow": {
      "type": "http",
      "url": "http://<server-ip>:8800/mcp",
      "headers": { "Authorization": "Bearer ${MARROW_TOKEN}" }
    }
  }
}
```

Check it: `/mcp` lists marrow with its tools, prompts and resources. Then:

```
> How am I doing today?
```

Claude calls `daily_brief` and answers in the app's own phrases (normal for
you, above your normal). The prompts appear as `/mcp__marrow__how_am_i_doing`,
`/mcp__marrow__weekly_review`, `/mcp__marrow__doctor_prep` and
`/mcp__marrow__log_from_text`.

## The hosted copy instead (Marrow Connect, Pro)

No token. Claude Code signs in through OAuth and you approve it from the
phone:

```sh
claude mcp add --transport http --scope user marrow https://connect.marrowhealthapp.com/mcp
```

Then `/mcp`, pick marrow, Authenticate. The page asks which Marrow this is:
paste the link from Export, Connect your AI, Copy your link, then type the
code it shows into Marrow and approve. The same server can also log for
you once Allow AI write is on in the app.

## The plugin

`/plugin marketplace add skeletonarmytech/marrow-mcp` then
`/plugin install marrow-health@marrow` gives you the Connect server plus
four slash commands (`/marrow-health:how-am-i-doing`, `weekly-review`,
`doctor-prep`, `log`) and a skill that knows which tool answers what.

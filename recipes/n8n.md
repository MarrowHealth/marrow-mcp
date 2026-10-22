# n8n

A Webhook node that receives Marrow's POST and an Edit Fields node that
pulls the day, the steps and a timestamp out of it. Chain whatever you like
after it (a database node, Google Sheets, a message).

Import this workflow (three-dot menu, Import from File, or paste it):

```json
{
  "name": "Marrow Apple Health in",
  "nodes": [
    {
      "parameters": {
        "httpMethod": "POST",
        "path": "marrow-health",
        "authentication": "none",
        "responseMode": "onReceived",
        "options": {}
      },
      "name": "Webhook",
      "type": "n8n-nodes-base.webhook",
      "typeVersion": 2,
      "position": [0, 0],
      "webhookId": "3f0b6f0e-5c0a-4d4e-9d7a-0a1b2c3d4e5f"
    },
    {
      "parameters": {
        "assignments": {
          "assignments": [
            { "id": "a1", "name": "day", "type": "string",
              "value": "={{ $json.body.data.metrics[0].data.at(-1).date }}" },
            { "id": "a2", "name": "steps", "type": "number",
              "value": "={{ $json.body.data.metrics.find(m => m.name === 'step_count')?.data.at(-1)?.qty ?? null }}" },
            { "id": "a3", "name": "received_at", "type": "string",
              "value": "={{ $now.toISO() }}" }
          ]
        },
        "includeOtherFields": false,
        "options": {}
      },
      "name": "Edit Fields",
      "type": "n8n-nodes-base.set",
      "typeVersion": 3.4,
      "position": [240, 0]
    }
  ],
  "connections": {
    "Webhook": { "main": [[ { "node": "Edit Fields", "type": "main", "index": 0 } ]] }
  },
  "settings": { "executionOrder": "v1" }
}
```

Steps:

1. Open the Webhook node, set Authentication to Header Auth and create a
   Header Auth credential (name `Authorization`, value `Bearer <secret>`).
   Credentials never travel in workflow JSON, so this step is by hand. Add
   the same header in the Marrow automation.
2. Listen for test event, then Run now in Marrow within two minutes.
3. Publish the workflow and paste the Production URL
   (`https://<n8n-host>/webhook/marrow-health`, not `/webhook-test/`) into
   Marrow. Production runs show under Executions, not in the editor.
4. For one item per metric, insert a Split Out node on `body.data.metrics`
   after the Webhook.

The payload sits at `$json.body`. Sources: the n8n Webhook node docs and
the Webhook and Set node sources (Webhook up to 2.2, Set up to 3.5),
verified 2026-10-22.

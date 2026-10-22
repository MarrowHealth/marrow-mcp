# Google Sheets through Apps Script

A web app that appends one row per metric per day to a sheet named Marrow.
Open the spreadsheet, Extensions, Apps Script, replace `Code.gs` with:

```javascript
const SHEET_NAME = 'Marrow'
const SECRET = 'change-me-to-a-long-random-string'

function doPost(e) {
  if (SECRET && (e.parameter.key || '') !== SECRET) {
    return ContentService.createTextOutput('forbidden')
  }
  const ss = SpreadsheetApp.getActiveSpreadsheet()
  const sheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME)
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(['received_at', 'metric', 'units', 'date', 'qty', 'extra'])
  }
  const body = JSON.parse(e.postData.contents)
  const now = new Date()
  const rows = []
  for (const m of (body.data && body.data.metrics) || []) {
    for (const d of m.data || []) {
      const { date, qty, ...extra } = d
      rows.push([now, m.name, m.units, date, qty === undefined ? '' : qty,
                 Object.keys(extra).length ? JSON.stringify(extra) : ''])
    }
  }
  if (rows.length) {
    sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows)
  }
  return ContentService.createTextOutput(JSON.stringify({ ok: true, rows: rows.length }))
    .setMimeType(ContentService.MimeType.JSON)
}
```

Deploy:

1. Save. Deploy, New deployment, the gear next to Select type, Web app.
2. Execute as: Me. Who has access: Anyone. Deploy, approve the permission
   prompt once. Anyone can only call `doPost`, never read the sheet.
3. Copy the Web app URL (it ends in `/exec`). Paste it into Marrow as
   `https://script.google.com/macros/s/<id>/exec?key=<SECRET>`.
4. After every code edit: Deploy, Manage deployments, the pencil, Version:
   New version, Deploy. `/exec` keeps serving the old version until you do.

Notes: `doPost` never sees request headers, so Marrow's `Authorization`
header is invisible here, hence the `?key=` query secret. `/exec` answers a
POST with a redirect that Marrow follows, so the run logs as delivered. Rows
are appended raw on purpose: dedupe on read (a pivot on metric and date,
largest received_at wins).

Source: the Apps Script web apps guide, verified 2026-10-22.

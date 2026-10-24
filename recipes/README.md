# Recipes: receive Marrow's Automations

Marrow's Automations (Export, Automated) POST a file of your data to any
URL on a schedule, from the phone, with the app closed. One automation is
one kind of data (daily metrics, workouts, strength sets, the food diary,
symptoms), one format (JSON or CSV), one date range, one URL, one cadence,
and any headers you type (a bearer token, a webhook secret).

What arrives for daily metrics in JSON:

```json
{"data": {"metrics": [
  {"name": "step_count", "units": "count", "data": [{"date": "2026-10-21", "qty": 8746}]},
  {"name": "heart_rate", "units": "bpm", "data": [{"date": "2026-10-21", "Avg": 71, "Min": 52, "Max": 148}]},
  {"name": "sleep_analysis", "units": "h", "data": [{"date": "2026-10-21", "totalSleep": 7.4, "deep": 1.1, "rem": 1.6, "core": 4.7, "awake": 0.3, "sleepStart": "2026-10-20 23:10", "sleepEnd": "2026-10-21 06:50"}]}
]}}
```

The shape follows Health Auto Export's metrics export, so a receiver written
for that app needs small changes only: dates are plain days and the values
are reconciled across phone and watch before they leave the phone. "Since
last run" re-sends the last two days on purpose, so key your store by
(metric, date) and let the latest write win. Any 2xx counts as delivered.
Full reference: <https://marrowhealthapp.com/automations>.

| Receiver | Recipe |
|---|---|
| Home Assistant | [home-assistant.md](home-assistant.md): webhook trigger, template sensors |
| n8n | [n8n.md](n8n.md): importable workflow, Webhook then Edit Fields |
| Google Sheets | [apps-script.md](apps-script.md): an Apps Script web app that appends rows |
| InfluxDB 2 or 3 | [influxdb.md](influxdb.md): a 30-line bridge to line protocol, or Telegraf |

Three rules keep any receiver robust: answer 2xx fast and do the work after,
archive the raw body before parsing it, and treat every delivery as
latest-wins.

For a finished project on top of the InfluxDB recipe (every automation kind,
Grafana dashboard, Docker Compose, demo data), see
[Body Ops](https://github.com/MarrowHealth/built-with-marrow/tree/main/projects/body-ops)
in Built with Marrow.

# InfluxDB 2 or 3

InfluxDB's write endpoints accept line protocol only, so a small bridge
turns Marrow's JSON into lines. Both ways below produce points like:

```
health,metric=step_count,units=count qty=8746 1761091200
health,metric=heart_rate,units=bpm Avg=71,Min=52,Max=148 1761091200
```

written to `POST http://<influx-host>:8086/api/v2/write?org=home&bucket=health&precision=s`
with `Authorization: Token <influx-api-token>` (InfluxDB 2), or to
`http://<influx-host>:8181/api/v3/write_lp?db=health&precision=second` with
`Authorization: Bearer <database-token>` (InfluxDB 3 Core or Enterprise).

## Bridge 1: Python, standard library, about 30 lines

Marrow posts to `https://<bridge>/` with the header
`Authorization: Bearer <MARROW_SECRET>`.

```python
import json, os, urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

INFLUX = os.environ.get("INFLUX_URL",
    "http://localhost:8086/api/v2/write?org=home&bucket=health&precision=s")
INFLUX_AUTH = os.environ.get("INFLUX_AUTH", "Token YOUR_API_TOKEN")  # v3: "Bearer <token>"
SECRET = os.environ.get("MARROW_SECRET", "")

def esc(s):
    return str(s).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")

def lines(body):
    for m in body.get("data", {}).get("metrics", []):
        for d in m.get("data", []):
            day = datetime.strptime(str(d.get("date", ""))[:10], "%Y-%m-%d")
            ts = int(day.replace(tzinfo=timezone.utc).timestamp())
            fields = ",".join(f"{esc(k)}={float(v)}" for k, v in d.items()
                              if isinstance(v, (int, float)) and not isinstance(v, bool))
            if fields:
                yield f"health,metric={esc(m['name'])},units={esc(m.get('units', ''))} {fields} {ts}"

class H(BaseHTTPRequestHandler):
    def do_POST(self):
        if SECRET and self.headers.get("Authorization") != f"Bearer {SECRET}":
            self.send_response(401)
            return self.end_headers()
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        lp = "\n".join(lines(body)).encode()
        req = urllib.request.Request(INFLUX, data=lp, method="POST",
            headers={"Authorization": INFLUX_AUTH, "Content-Type": "text/plain; charset=utf-8"})
        try:
            urllib.request.urlopen(req, timeout=30).close()
            self.send_response(204)
        except Exception as e:
            self.send_response(502, str(e)[:80])
        self.end_headers()

HTTPServer(("0.0.0.0", 8798), H).serve_forever()
```

Run `MARROW_SECRET=... INFLUX_AUTH="Token ..." python3 bridge.py` behind
HTTPS (Cloudflare Tunnel, Tailscale Funnel, Caddy). Fields are always
floats so a field never flips type. The timestamp is the day at 00:00 UTC,
so a re-sent day overwrites the same point. Daily totals only. Raw samples
would need `start` parsed instead of `date`.

## Bridge 2: Telegraf, no code

Paired with your usual `[[outputs.influxdb_v2]]`:

```toml
[[inputs.http_listener_v2]]
  service_address = "tcp://:8798"
  paths = ["/marrow"]
  methods = ["POST"]
  data_format = "json_v2"
  basic_username = "marrow"
  basic_password = "change-me"

  [[inputs.http_listener_v2.json_v2]]
    measurement_name = "health"
    [[inputs.http_listener_v2.json_v2.object]]
      path = "data.metrics"
      tags = ["name", "units"]
      disable_prepend_keys = true
      timestamp_key = "date"
      timestamp_format = "2006-01-02"
      excluded_keys = ["sleepStart", "sleepEnd"]
```

Marrow side: URL `https://<bridge>/marrow`, header
`Authorization: Basic <base64 of marrow:change-me>` (the plugin documents
Basic auth only). The object path expands the nested `data` array into one
line per day with `name` and `units` as tags, and `disable_prepend_keys`
keeps field names `qty`, `Avg`, `totalSleep`. This block follows the plugin
docs and was not run here: confirm with Run now in Marrow.

Sources: InfluxDB v2 write API, InfluxDB 3 write_lp, Telegraf
http_listener_v2 and json_v2 parser docs, verified 2026-10-22.

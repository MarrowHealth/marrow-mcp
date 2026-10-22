# Home Assistant

Trigger-based template sensors fed straight from a webhook. No helpers, no
custom component, and the values survive a restart.

Marrow side: an automation with kind Health metrics, format JSON, URL
`https://<ha-host>/api/webhook/<webhook_id>`, no extra header (the id is the
secret, make it long and random). Pick the cadence you like and tap Run now
once to test.

Home Assistant side, in `configuration.yaml` (or a package):

```yaml
template:
  - triggers:
      - trigger: webhook
        webhook_id: "marrow-REPLACE-WITH-32-RANDOM-CHARS"
        allowed_methods:
          - POST
        local_only: false
    sensor:
      - name: "Marrow steps"
        unique_id: marrow_steps
        unit_of_measurement: "steps"
        state_class: measurement
        state: >-
          {% set m = trigger.json.data.metrics | selectattr('name', 'eq', 'step_count') | list %}
          {{ (m[0].data | last).qty if m else states('sensor.marrow_steps') }}
      - name: "Marrow sleep"
        unique_id: marrow_sleep
        unit_of_measurement: "h"
        state_class: measurement
        state: >-
          {% set m = trigger.json.data.metrics | selectattr('name', 'eq', 'sleep_analysis') | list %}
          {{ (m[0].data | last).totalSleep if m else states('sensor.marrow_sleep') }}
      - name: "Marrow weight"
        unique_id: marrow_weight
        unit_of_measurement: "kg"
        device_class: weight
        state_class: measurement
        state: >-
          {% set m = trigger.json.data.metrics | selectattr('name', 'eq', 'body_mass') | list %}
          {{ (m[0].data | last).qty if m else states('sensor.marrow_weight') }}
```

Notes:

- `data | last` is the newest day in the window (latest wins). The
  `if m else states(...)` keeps the old value when a payload lacks that
  metric.
- `local_only: false` is needed when the phone posts over the internet to
  your own address. Drop it on the LAN or with a Nabu Casa cloud webhook.
- Copy a block and change the metric name for anything else Marrow sends
  (`resting_heart_rate`, `active_energy`, `dietary_water` and so on).
- Reload under Developer tools, YAML, Template entities. Then in Marrow tap
  Run now and watch the sensors fill.

Test from a terminal:

```sh
curl -X POST -H "Content-Type: application/json" \
  -d '{"data":{"metrics":[{"name":"step_count","units":"count","data":[{"date":"2026-10-22","qty":8746}]}]}}' \
  https://<ha-host>/api/webhook/<webhook_id>
```

An `input_number.set_value` automation variant with the same trigger is on
<https://marrowhealthapp.com/apple-health-webhook>.

Sources: Home Assistant docs for the webhook trigger and trigger-based
template entities, verified 2026-10-22.

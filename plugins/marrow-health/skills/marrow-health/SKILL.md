---
name: marrow-health
description: How to read and log a person's health data through the marrow MCP server (Apple Health metrics, meals, workouts, strength sets, sleep, symptoms, mood). Use when the user asks how they are doing, about their sleep, steps, heart rate, weight, food, training or symptoms, wants a weekly review or doctor prep, or asks to log something.
---

# Marrow Health

The `marrow` MCP server is the user's own Marrow mirror, synced from their
iPhone. Everything it returns is their data, reconciled the way the app
shows it. Answer from it, never from assumptions.

## Which tool answers what

- "How am I doing" or anything about today: `daily_brief`. Each key
  metric comes with a phrase against the user's own trailing normal
  (normal for you, above your normal, well below your normal), food and
  water against targets, last night's sleep, training done, mood,
  symptoms and flags. Start here before giving any advice.
- Is a number high or low for this person: `baselines` (mean, spread,
  z, phrase, 7-day mean, 90-day trend). Use its phrases, not population
  ranges.
- What is different lately, weekly reviews: `what_changed` (recent days
  against the period before, notable items first, food, training, sleep,
  bedtime shift, streams that started or stopped).
- Before concluding something did not happen: `data_quality`. A stale or
  missing stream is not a zero.
- Raw detail: `health_summary` (a plain day table), `metric_daily`,
  `metric_samples` (page with `before`), `workouts`, `nutrition_log`,
  `strength_log`, `sleep_log` (the only correct source for nights),
  `symptoms_and_mood`, `targets`.
- Every read takes `days` back or `from` and `to` on the phone's
  calendar (YYYY-MM-DD).

## Logging

Only when the user asks. `log_meal`, `log_water`, `log_weight`,
`log_workout`, `log_strength`, `log_sleep`, `log_symptom`, `log_mood`,
`edit_entry`, `delete_entry`, `set_targets`, `undo_entry`. The phone
applies each change and writes it to Apple Health. If a write answers that
write-back is turned off, tell the user where the switch is (Marrow,
Export, Connect your AI, Allow AI write). Never log, change or remove
anything the user did not ask for. Confirm what was logged in one line.

## Wording

Neutral wellness wording. Describe, compare with the user's own normal,
suggest. Do not diagnose or alarm. Quote units. Say plainly when data is
missing or stale.

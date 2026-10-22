#!/usr/bin/env python3
"""Marrow self-hosted server: pair once, own your mirror, serve your agents.

One file, Python stdlib only. Run it on anything that stays on (Raspberry
Pi, NAS, home server, old laptop):

    python3 marrow_server.py                # prints the pairing QR page URL
    python3 marrow_server.py --port 8800 --data ./data

Then in Marrow on your phone: Export → Server MCP → Pair with a server → scan.

What it does:
  - receives Marrow's background pushes (phone app can stay closed),
  - keeps a complete SQLite mirror of your verified health data,
  - serves it to AI agents over MCP (Claude, Cursor, any MCP client),
    24/7. This is the always-on companion to the app's on-device
    Live mode.
  - answers in the app's own terms: daily_brief, baselines, what_changed
    and data_quality score each day against your own trailing normal with
    the same math the app's Home page uses, so an AI starts from a read
    that is already true instead of re-deriving one from raw samples.

Your data flows only from your phone to this machine. No cloud, no vendor,
no telemetry. Put it behind HTTPS (tailscale serve, Caddy) if you want to
reach it from outside your LAN. Never port-forward it raw.

Endpoints (app token via X-HealthBridge-Token. Agents via Authorization:
Bearer <mcp token>. Two separate tokens, both auto-generated in --data):
    GET  /pair     pairing page (QR). Open on a second screen, scan
    POST /ping     pairing test
    POST /ingest   sample batches from the app (idempotent, uuid upsert)
    GET  /status   mirror summary
    POST /device   the phone says whether an AI may add entries (write-back)
    GET  /inbox    entries an AI queued for the phone to apply
    POST /inbox/ack  the phone reports what it applied
    POST /mcp      MCP Streamable HTTP (JSON-RPC 2.0)
    GET  /health   liveness
"""

import argparse
import hashlib
import hmac
import json
import re
import secrets
import socket
import sqlite3
import statistics
import threading
import time
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

__version__ = "0.4.0"

PROTOCOL_VERSION = "2025-06-18"
MAX_BODY = 32 * 1024 * 1024

# App stream key -> Apple-native type. `scale` converts wire value back to
# Apple-native storage (the one that matters: SpO2 travels as 97, stores as
# the 0.97 fraction Apple uses).
KEYMAP = {
    "step_count":               ("HKQuantityTypeIdentifierStepCount", 1),
    "walking_running_distance": ("HKQuantityTypeIdentifierDistanceWalkingRunning", 1),
    "flights_climbed":          ("HKQuantityTypeIdentifierFlightsClimbed", 1),
    "active_energy":            ("HKQuantityTypeIdentifierActiveEnergyBurned", 1),
    "basal_energy_burned":      ("HKQuantityTypeIdentifierBasalEnergyBurned", 1),
    "apple_exercise_time":      ("HKQuantityTypeIdentifierAppleExerciseTime", 1),
    "apple_stand_time":         ("HKQuantityTypeIdentifierAppleStandTime", 1),
    "heart_rate":               ("HKQuantityTypeIdentifierHeartRate", 1),
    "resting_heart_rate":       ("HKQuantityTypeIdentifierRestingHeartRate", 1),
    "heart_rate_variability":   ("HKQuantityTypeIdentifierHeartRateVariabilitySDNN", 1),
    "respiratory_rate":         ("HKQuantityTypeIdentifierRespiratoryRate", 1),
    "blood_oxygen":             ("HKQuantityTypeIdentifierOxygenSaturation", 0.01),
    "body_mass":                ("HKQuantityTypeIdentifierBodyMass", 1),
}

STAGEMAP = {
    "core":    "HKCategoryValueSleepAnalysisAsleepCore",
    "deep":    "HKCategoryValueSleepAnalysisAsleepDeep",
    "rem":     "HKCategoryValueSleepAnalysisAsleepREM",
    "asleep":  "HKCategoryValueSleepAnalysisAsleepUnspecified",
    "awake":   "HKCategoryValueSleepAnalysisAwake",
    "in_bed":  "HKCategoryValueSleepAnalysisInBed",
}

# App stream key -> Apple category type, for phones that send the stream
# without its "hk_type" (every app before 1.4 did, for these streams): the
# symptoms and mood tools read by Apple type, so without this the rows were
# counted as unknown and dropped. Generated from HealthTypes.categoryExtras.
CATEGORY_TYPES = {
    "symptom_headache": "HKCategoryTypeIdentifierHeadache",
    "symptom_fatigue": "HKCategoryTypeIdentifierFatigue",
    "symptom_nausea": "HKCategoryTypeIdentifierNausea",
    "symptom_dizziness": "HKCategoryTypeIdentifierDizziness",
    "symptom_fever": "HKCategoryTypeIdentifierFever",
    "symptom_coughing": "HKCategoryTypeIdentifierCoughing",
    "symptom_shortness_of_breath": "HKCategoryTypeIdentifierShortnessOfBreath",
    "symptom_abdominal_cramps": "HKCategoryTypeIdentifierAbdominalCramps",
    "symptom_appetite_changes": "HKCategoryTypeIdentifierAppetiteChanges",
    "symptom_acne": "HKCategoryTypeIdentifierAcne",
    "symptom_bladder_incontinence": "HKCategoryTypeIdentifierBladderIncontinence",
    "symptom_bloating": "HKCategoryTypeIdentifierBloating",
    "symptom_breast_pain": "HKCategoryTypeIdentifierBreastPain",
    "symptom_chest_tightness_or_pain": "HKCategoryTypeIdentifierChestTightnessOrPain",
    "symptom_chills": "HKCategoryTypeIdentifierChills",
    "symptom_constipation": "HKCategoryTypeIdentifierConstipation",
    "symptom_diarrhea": "HKCategoryTypeIdentifierDiarrhea",
    "symptom_dry_skin": "HKCategoryTypeIdentifierDrySkin",
    "symptom_fainting": "HKCategoryTypeIdentifierFainting",
    "symptom_body_ache": "HKCategoryTypeIdentifierGeneralizedBodyAche",
    "symptom_hair_loss": "HKCategoryTypeIdentifierHairLoss",
    "symptom_heartburn": "HKCategoryTypeIdentifierHeartburn",
    "symptom_hot_flashes": "HKCategoryTypeIdentifierHotFlashes",
    "symptom_loss_of_smell": "HKCategoryTypeIdentifierLossOfSmell",
    "symptom_loss_of_taste": "HKCategoryTypeIdentifierLossOfTaste",
    "symptom_lower_back_pain": "HKCategoryTypeIdentifierLowerBackPain",
    "symptom_memory_lapse": "HKCategoryTypeIdentifierMemoryLapse",
    "symptom_mood_changes": "HKCategoryTypeIdentifierMoodChanges",
    "symptom_night_sweats": "HKCategoryTypeIdentifierNightSweats",
    "symptom_pelvic_pain": "HKCategoryTypeIdentifierPelvicPain",
    "symptom_pounding_heartbeat": "HKCategoryTypeIdentifierRapidPoundingOrFlutteringHeartbeat",
    "symptom_runny_nose": "HKCategoryTypeIdentifierRunnyNose",
    "symptom_sinus_congestion": "HKCategoryTypeIdentifierSinusCongestion",
    "symptom_skipped_heartbeat": "HKCategoryTypeIdentifierSkippedHeartbeat",
    "symptom_sleep_changes": "HKCategoryTypeIdentifierSleepChanges",
    "symptom_sore_throat": "HKCategoryTypeIdentifierSoreThroat",
    "symptom_vaginal_dryness": "HKCategoryTypeIdentifierVaginalDryness",
    "symptom_vomiting": "HKCategoryTypeIdentifierVomiting",
    "symptom_wheezing": "HKCategoryTypeIdentifierWheezing",
    "menstrual_flow": "HKCategoryTypeIdentifierMenstrualFlow",
    "intermenstrual_bleeding": "HKCategoryTypeIdentifierIntermenstrualBleeding",
    "cervical_mucus_quality": "HKCategoryTypeIdentifierCervicalMucusQuality",
    "ovulation_test_result": "HKCategoryTypeIdentifierOvulationTestResult",
    "pregnancy_test_result": "HKCategoryTypeIdentifierPregnancyTestResult",
    "progesterone_test_result": "HKCategoryTypeIdentifierProgesteroneTestResult",
    "sexual_activity": "HKCategoryTypeIdentifierSexualActivity",
    "contraceptive": "HKCategoryTypeIdentifierContraceptive",
    "lactation": "HKCategoryTypeIdentifierLactation",
    "pregnancy": "HKCategoryTypeIdentifierPregnancy",
    "persistent_intermenstrual_bleeding": "HKCategoryTypeIdentifierPersistentIntermenstrualBleeding",
    "prolonged_menstrual_periods": "HKCategoryTypeIdentifierProlongedMenstrualPeriods",
    "irregular_menstrual_cycles": "HKCategoryTypeIdentifierIrregularMenstrualCycles",
    "infrequent_menstrual_cycles": "HKCategoryTypeIdentifierInfrequentMenstrualCycles",
    "low_heart_rate_event": "HKCategoryTypeIdentifierLowHeartRateEvent",
    "high_heart_rate_event": "HKCategoryTypeIdentifierHighHeartRateEvent",
    "irregular_rhythm_event": "HKCategoryTypeIdentifierIrregularHeartRhythmEvent",
    "low_cardio_fitness_event": "HKCategoryTypeIdentifierLowCardioFitnessEvent",
    "environmental_audio_exposure_event": "HKCategoryTypeIdentifierEnvironmentalAudioExposureEvent",
    "headphone_audio_exposure_event": "HKCategoryTypeIdentifierHeadphoneAudioExposureEvent",
    "handwashing": "HKCategoryTypeIdentifierHandwashingEvent",
    "toothbrushing": "HKCategoryTypeIdentifierToothbrushingEvent",
    "apple_stand_hour": "HKCategoryTypeIdentifierAppleStandHour",
}
# The other streams the app sends without a quantity metric behind them.
STREAM_TYPES = {
    "mindful_minutes": "HKCategoryTypeIdentifierMindfulSession",
    "ecg": "HKDataTypeIdentifierElectrocardiogram",
    "state_of_mind": "HKStateOfMindTypeIdentifier",
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    uuid           TEXT UNIQUE,
    type           TEXT NOT NULL,
    source_name    TEXT,
    source_version TEXT,
    device         TEXT,
    unit           TEXT,
    creation_date  TEXT,
    start_date     TEXT,
    end_date       TEXT,
    value          TEXT,
    num            REAL,
    day            TEXT
);
CREATE INDEX IF NOT EXISTS idx_records_type_day ON records (type, day);
CREATE INDEX IF NOT EXISTS idx_records_day ON records (day);
CREATE TABLE IF NOT EXISTS workouts (
    id             INTEGER PRIMARY KEY,
    uuid           TEXT UNIQUE,
    activity_type  TEXT,
    duration       REAL,
    duration_unit  TEXT,
    source_name    TEXT,
    device         TEXT,
    creation_date  TEXT,
    start_date     TEXT,
    end_date       TEXT,
    day            TEXT
);
CREATE TABLE IF NOT EXISTS strength_sets (
    uuid         TEXT UNIQUE,
    session_id   TEXT NOT NULL,
    session_name TEXT,
    exercise     TEXT NOT NULL,
    idx          INTEGER,
    weight       REAL,
    unit         TEXT,
    reps         INTEGER,
    done_at      TEXT,
    day          TEXT,
    muscles      TEXT,
    kind         TEXT,
    rir          REAL,
    program_id   TEXT,
    program_day  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_strength_session ON strength_sets (session_id);
CREATE INDEX IF NOT EXISTS idx_strength_day ON strength_sets (day);
CREATE TABLE IF NOT EXISTS diary (
    entry_id  TEXT UNIQUE,
    day       TEXT NOT NULL,
    meal      TEXT,
    name      TEXT,
    grams     REAL,
    kcal      REAL,
    protein   REAL,
    carbs     REAL,
    fat       REAL,
    logged_at REAL,
    extra     TEXT
);
CREATE INDEX IF NOT EXISTS idx_diary_day ON diary (day);
CREATE TABLE IF NOT EXISTS ingest_log (
    ts TEXT, seq INTEGER, stream TEXT, added INTEGER, deleted INTEGER
);
"""

SUM_HINTS = ("StepCount", "Distance", "FlightsClimbed", "EnergyBurned",
             "ExerciseTime", "StandTime", "MoveTime", "Dietary", "PushCount",
             "SwimmingStrokeCount", "TimeInDaylight", "NumberOf",
             "InhalerUsage", "InsulinDelivery")


# ---- write-back: an AI queues an entry, the phone applies it ----
#
# The phone stays the source of truth. A write tool only adds a row to
# `inbox`. Marrow on the phone picks it up, logs it the way a manual entry
# is logged (its own diary first, then Apple Health) and acknowledges.
# Nothing here ever writes the `diary` table: the phone replaces a whole
# diary day on every push, so a row written there would be wiped.
WB_SCHEMA = """
CREATE TABLE IF NOT EXISTS inbox (
    id      TEXT PRIMARY KEY,
    kind    TEXT NOT NULL,
    payload TEXT NOT NULL,
    client  TEXT,
    created REAL NOT NULL,
    state   TEXT NOT NULL,
    idem    TEXT,
    done_at REAL,
    result  TEXT
);
CREATE INDEX IF NOT EXISTS idx_inbox_state ON inbox (state, created);
CREATE INDEX IF NOT EXISTS idx_inbox_idem ON inbox (idem);
CREATE TABLE IF NOT EXISTS wb_state (
    k TEXT PRIMARY KEY,
    v TEXT
);
"""

WRITE_KINDS = ("meal", "water", "weight", "undo", "edit", "delete", "workout",
               "strength", "sleep", "symptom", "mood", "targets")
# Not inbox kinds: abilities a newer app adds to an existing kind.
WRITE_CAPS = ("bodyfat", "setkind")
# What the phone cannot take back once applied (no id comes back from the
# app's own water and weight writers).
NO_UNDO_KINDS = ("water", "weight", "undo")
WRITE_TOOL_NAMES = frozenset((
    "log_meal", "log_water", "log_weight", "undo_entry", "edit_entry",
    "delete_entry", "log_workout", "log_strength", "log_sleep",
    "log_symptom", "log_mood", "set_targets"))
MEALS = ("breakfast", "lunch", "dinner", "snack")
# Workout activities the app can write (Apple Health activity types).
ACTIVITIES = ("running", "walking", "cycling", "swimming", "rowing",
              "elliptical", "hiking", "hiit", "yoga", "pilates", "stairs",
              "core", "strength", "dance", "basketball", "soccer", "tennis",
              "golf", "other")
# Apple Health symptoms that take a severity (the three that use their own
# scales, appetite, mood and sleep changes, are read-only here).
SYMPTOMS = (
    "headache", "fatigue", "nausea", "dizziness", "fever", "coughing",
    "shortness_of_breath", "abdominal_cramps", "acne",
    "bladder_incontinence", "bloating", "breast_pain",
    "chest_tightness_or_pain", "chills", "constipation", "diarrhea",
    "dry_skin", "fainting", "body_ache", "hair_loss", "heartburn",
    "hot_flashes", "loss_of_smell", "loss_of_taste", "lower_back_pain",
    "memory_lapse", "night_sweats", "pelvic_pain", "pounding_heartbeat",
    "runny_nose", "sinus_congestion", "skipped_heartbeat", "sore_throat",
    "vaginal_dryness", "vomiting", "wheezing")
SEVERITIES = ("present", "mild", "moderate", "severe")
# Apple's State of Mind vocabulary (iOS 18), as the app names it.
MOOD_FEELINGS = {"very unpleasant": -1.0, "unpleasant": -0.67,
                 "slightly unpleasant": -0.33, "neutral": 0.0,
                 "slightly pleasant": 0.33, "pleasant": 0.67,
                 "very pleasant": 1.0}
MOOD_LABELS = (
    "amazed", "amused", "angry", "anxious", "ashamed", "brave", "calm",
    "content", "disappointed", "discouraged", "disgusted", "embarrassed",
    "drained", "excited", "frustrated", "grateful", "guilty", "happy",
    "hopeless", "indifferent", "irritated", "jealous", "joyful", "lonely",
    "passionate", "peaceful", "proud", "relieved", "sad", "scared",
    "stressed", "surprised", "worried")
MOOD_ABOUT = (
    "community", "current events", "dating", "education", "family",
    "fitness", "friends", "health", "hobbies", "identity", "money",
    "partner", "self care", "spirituality", "tasks", "travel", "work",
    "weather")
# Targets an AI may set: key -> (lowest, highest).
TARGET_BOUNDS = {
    "kcal": (800, 10000), "protein_g": (0, 600), "carbs_g": (0, 1500),
    "fat_g": (0, 600), "water_ml": (250, 10000), "steps": (1000, 100000),
    "sleep_hours": (3, 14), "exercise_minutes": (5, 600)}
WB_MAX_ITEMS = 20          # foods in one meal
WB_MAX_QUEUED = 60         # entries waiting for the phone, per mirror
WB_DAYS_BACK = 30          # how far in the past an entry may be filed
WB_DEDUP_S = 120           # an identical call inside this window is one entry
WB_EXPIRE_S = 7 * 86400    # a queued entry the phone never fetched
WB_UNMIRRORED_S = 86400    # applied, the day's diary push never came: stop showing it
WB_PUSH_SLACK_S = 5        # a diary push this soon after the ack may predate the apply
WB_INBOX_PAGE = 50
WB_DISABLED = ("Adding entries is turned off. In Marrow, open Export, "
               "Connect your AI, and turn on Allow AI write. "
               "Needs Marrow 1.3.4 or later.")
WB_UPDATE = ("The version of Marrow on the phone cannot do that yet. Update "
             "Marrow, open it once, and try again.")
PUSH_TOKEN_RE = re.compile(r"^[0-9a-fA-F]{32,200}$")
NUTRIENT_KEY_RE = re.compile(r"^dietary_[a-z0-9_]{1,48}$")


def short_key(apple_type):
    t = re.sub(r"^HK(Quantity|Category)TypeIdentifier", "", apple_type)
    return re.sub(r"(?<!^)(?=[A-Z])", "_", t).lower()


def load_token(path):
    if path.exists():
        return path.read_text().strip()
    tok = secrets.token_urlsafe(24)
    path.write_text(tok)
    path.chmod(0o600)
    return tok


def lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def local_strings(iso_utc, offset_s):
    """UTC instant -> device-local strings. The day must come from the
    DEVICE's timezone. Server-side days are the classic off-by-one."""
    dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone(timedelta(seconds=offset_s)))
    return dt.strftime("%Y-%m-%d %H:%M:%S %z"), dt.strftime("%Y-%m-%d")


# A set's kind, as the app stores it (1.5): warm-ups are listed but never
# progress, volume or muscle credit, the same rule the phone applies.
SET_KINDS = ("work", "warmup", "failure", "drop")


class Mirror:
    def __init__(self, data_dir):
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = data_dir / "mirror.sqlite"
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.executescript(SCHEMA)
        self.conn.executescript(WB_SCHEMA)
        self._add_columns("strength_sets", {"kind": "TEXT", "rir": "REAL",
                                            "program_id": "TEXT",
                                            "program_day": "INTEGER"})
        self.staging = data_dir / "staging"
        self.staging.mkdir(exist_ok=True)

    STAGING_KEEP_DAYS = 30

    def _prune_staging(self):
        """Raw pushes older than a month go. The tables hold the data and
        a day's file is only a replay aid, not an archive that grows forever."""
        floor = (datetime.now() - timedelta(days=self.STAGING_KEEP_DAYS)).strftime("%Y-%m-%d")
        for f in self.staging.glob("*.jsonl"):
            if f.stem < floor:
                try:
                    f.unlink()
                except OSError:
                    pass

    # ---- ingest (Marrow push protocol. Idempotent on uuid) ----


    def _add_columns(self, table, columns):
        """Additive migration: columns a newer app sends to a mirror created
        by an older one (the app's own store migrates the same way)."""
        have = {r[1] for r in self.conn.execute(f"PRAGMA table_info({table})")}
        for name, typ in columns.items():
            if name not in have:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
        self.conn.commit()
    def ingest(self, payload):
        stream = str(payload.get("stream") or "")
        hk_type = str(payload.get("hk_type") or "")
        if not hk_type.startswith("HK"):
            hk_type = STREAM_TYPES.get(stream) or CATEGORY_TYPES.get(stream) or hk_type
        try:
            native_scale = float(payload.get("native_scale") or 1)
        except (TypeError, ValueError):
            native_scale = 1.0
        try:
            offset = int(payload.get("tz_offset_seconds") or 0)
        except (TypeError, ValueError):
            offset = 0
        added = payload.get("added") or []
        deleted = payload.get("deleted") or []
        if stream not in ("strength_session", "diary_day") and (
                not isinstance(added, list) or not isinstance(deleted, list)
                or not all(isinstance(x, dict) for x in added)):
            raise ValueError("added/deleted must be lists (of objects)")

        # Raw staging first. Everything downstream is rebuildable.
        stamp = datetime.now().strftime("%Y-%m-%d")
        if stamp != getattr(self, "_staging_day", None):
            self._staging_day = stamp
            self._prune_staging()
        with open(self.staging / f"{stamp}.jsonl", "a") as fh:
            fh.write(json.dumps(payload, separators=(",", ":")) + "\n")

        # Diary days arrive whole and replace whole: any edit or delete on
        # the phone re-sends the day's final state, so latest-wins keeps
        # the mirror exact.
        if stream == "diary_day":
            day = str(payload.get("day") or "")
            entries = payload.get("entries") or []
            if not day or not isinstance(entries, list):
                raise ValueError("diary_day needs day and entries")
            cur = self.conn.cursor()
            cur.execute("DELETE FROM diary WHERE day=?", (day,))
            n = 0
            for e in entries:
                if not isinstance(e, dict):
                    continue
                cur.execute(
                    "INSERT INTO diary(entry_id,day,meal,name,grams,kcal,"
                    "protein,carbs,fat,logged_at,extra) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(entry_id) DO UPDATE SET meal=excluded.meal,"
                    "name=excluded.name, grams=excluded.grams,"
                    "kcal=excluded.kcal, extra=excluded.extra",
                    (str(e.get("id") or f"{day}:{n}"), day,
                     str(e.get("meal") or ""), str(e.get("name") or ""),
                     e.get("grams"), e.get("kcal"), e.get("protein"),
                     e.get("carbs"), e.get("fat"), e.get("created"),
                     json.dumps(e.get("extra") or {}, separators=(",", ":"))))
                n += 1
            cur.execute("INSERT INTO ingest_log VALUES(?,?,?,?,?)",
                        (datetime.now().isoformat(timespec="seconds"),
                         payload.get("seq"), stream, n, 0))
            # When this day's diary last arrived: from then on the diary
            # table, not the inbox, says what the day holds.
            self._set_state("diary_day:" + day, time.time())
            self.conn.commit()
            return {"ok": True, "stream": stream, "added": n, "deleted": 0,
                    "ignored": max(len(entries) - n, 0)}

        # Strength sessions arrive whole and replace whole: the app's own
        # save semantics are latest-wins per session, so an edited workout
        # can never leave stale sets here.
        if stream == "strength_session":
            session = payload.get("session") or {}
            sets = payload.get("sets") or []
            sid = str(session.get("id") or "")
            if not sid or not isinstance(sets, list):
                raise ValueError("strength_session needs session.id and sets")
            unit = str(payload.get("unit") or "")
            name = str(session.get("name") or "Workout")
            # 1.5: the program this session completed, when it did.
            program_id = session.get("program_id")
            program_id = str(program_id) if program_id else None
            program_day = session.get("program_day")
            program_day = (int(program_day)
                           if isinstance(program_day, (int, float)) else None)
            cur = self.conn.cursor()
            cur.execute("DELETE FROM strength_sets WHERE session_id=?", (sid,))
            n = 0
            for s in sets:
                if not isinstance(s, dict):
                    continue
                done = str(s.get("done_at") or "")
                try:
                    done_s, day = local_strings(done, offset)
                except ValueError:
                    continue
                # 1.5: the set's kind (the app sends it only when it is not
                # a working set) and reps in reserve, when stored.
                kind = str(s.get("kind") or "work")
                if kind not in SET_KINDS:
                    kind = "work"
                rir = s.get("rir")
                rir = float(rir) if isinstance(rir, (int, float)) else None
                cur.execute(
                    "INSERT INTO strength_sets(uuid,session_id,session_name,"
                    "exercise,idx,weight,unit,reps,done_at,day,muscles,"
                    "kind,rir,program_id,program_day) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(uuid) DO UPDATE SET weight=excluded.weight,"
                    "exercise=excluded.exercise,"
                    "session_name=excluded.session_name, idx=excluded.idx,"
                    "unit=excluded.unit,"
                    "reps=excluded.reps, done_at=excluded.done_at,"
                    "day=excluded.day, muscles=excluded.muscles,"
                    "kind=excluded.kind, rir=excluded.rir,"
                    "program_id=excluded.program_id,"
                    "program_day=excluded.program_day",
                    (str(s.get("id") or f"{sid}:{s.get('idx')}"), sid, name,
                     str(s.get("exercise") or ""), s.get("idx"),
                     s.get("weight"), unit, s.get("reps"), done_s, day,
                     json.dumps(s.get("muscles") or [],
                                separators=(",", ":")),
                     kind, rir, program_id, program_day))
                n += 1
            cur.execute("INSERT INTO ingest_log VALUES(?,?,?,?,?)",
                        (datetime.now().isoformat(timespec="seconds"),
                         payload.get("seq"), stream, n, 0))
            self.conn.commit()
            return {"ok": True, "stream": stream, "added": n, "deleted": 0,
                    "ignored": max(len(sets) - n, 0)}

        n_add = n_del = n_ignored = 0
        cur = self.conn.cursor()
        for s in added:
            uuid = str(s.get("uuid") or "")
            start = str(s.get("start") or "")
            end = str(s.get("end") or "")
            if not uuid or not start or not end:
                n_ignored += 1
                continue
            try:
                start_s, day = local_strings(start, offset)
                end_s, _ = local_strings(end, offset)
            except ValueError:
                n_ignored += 1
                continue
            source = str(s.get("source") or "")
            device = str(s.get("device") or "")
            unit = str(s.get("unit") or "")
            value = s.get("value")

            if stream == "workout":
                cur.execute("""
                    INSERT INTO workouts(uuid,activity_type,duration,duration_unit,
                        source_name,device,start_date,end_date,day)
                    VALUES(?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(uuid) DO UPDATE SET duration=excluded.duration,
                        start_date=excluded.start_date, end_date=excluded.end_date,
                        day=excluded.day
                    """, (uuid,
                          "HKWorkoutActivityType" + str(s.get("stage") or "Workout"),
                          value, unit, source, device, start_s, end_s, day))
                n_add += 1
                continue

            if stream == "sleep":
                row_type = "HKCategoryTypeIdentifierSleepAnalysis"
                text_value = STAGEMAP.get(str(s.get("stage") or ""),
                                          STAGEMAP["asleep"])
                num = None
            elif stream.startswith("symptom_") and hk_type.startswith("HK"):
                # Symptoms: the phone sends Apple's category value (severity
                # scale) in `stage`. Keep it as the number, not the duration.
                row_type = hk_type
                try:
                    num = float(s.get("stage"))
                except (TypeError, ValueError):
                    num = None
                text_value = str(s.get("stage") or "")
            elif stream == "state_of_mind" and hk_type.startswith("HK"):
                # State of Mind: valence is the number. Kind|labels|associations
                # travel in the text so the mood tool can say them in words.
                row_type = hk_type
                try:
                    num = float(value)
                except (TypeError, ValueError):
                    num = None
                text_value = str(s.get("stage") or "")
            elif stream in KEYMAP:
                row_type, scale = KEYMAP[stream]
                try:
                    num = float(value) * scale
                except (TypeError, ValueError):
                    n_ignored += 1
                    continue
                text_value = repr(num)
            elif hk_type.startswith("HK"):
                # Generic path: payload carries its own Apple-native type +
                # scale, so app streams added later land with zero server
                # updates.
                row_type = hk_type
                try:
                    num = float(value) * native_scale
                except (TypeError, ValueError):
                    n_ignored += 1
                    continue
                text_value = repr(num)
            else:
                n_ignored += 1   # unknown, kept in staging. Never 4xx
                continue

            cur.execute("""
                INSERT INTO records(uuid,type,source_name,device,unit,
                    start_date,end_date,value,num,day)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(uuid) DO UPDATE SET num=excluded.num,
                    value=excluded.value, start_date=excluded.start_date,
                    end_date=excluded.end_date, day=excluded.day
                """, (uuid, row_type, source, device, unit,
                      start_s, end_s, text_value, num, day))
            n_add += 1

        for u in deleted:
            cur.execute("DELETE FROM records WHERE uuid=?", (str(u),))
            cur.execute("DELETE FROM workouts WHERE uuid=?", (str(u),))
            n_del += cur.rowcount

        cur.execute("INSERT INTO ingest_log VALUES(?,?,?,?,?)",
                    (datetime.now().isoformat(timespec="seconds"),
                     payload.get("seq"), stream, n_add, n_del))
        self.conn.commit()
        return {"status": "ok", "added": n_add, "deleted": n_del,
                "ignored": n_ignored}

    def status(self):
        c = self.conn
        total, days, lo, hi = c.execute(
            "SELECT COUNT(*), COUNT(DISTINCT day), MIN(day), MAX(day) "
            "FROM records").fetchone()
        last = c.execute("SELECT MAX(ts) FROM ingest_log").fetchone()[0]
        return {"records": total, "days": days, "first_day": lo,
                "last_day": hi, "last_ingest": last}

    # ---- write-back (see WB_SCHEMA) ----

    def device(self):
        row = self.conn.execute("SELECT v FROM wb_state WHERE k='device'").fetchone()
        if not row:
            return {}
        try:
            d = json.loads(row[0])
        except ValueError:
            return {}
        return d if isinstance(d, dict) else {}

    def _save_device(self, d):
        self._set_state("device", json.dumps(d, separators=(",", ":")))
        self.conn.commit()

    def _set_state(self, key, value):
        self.conn.execute(
            "INSERT INTO wb_state(k, v) VALUES(?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, str(value)))

    def set_device(self, body):
        """The phone's own word on write-back: whether an AI may add
        entries, which kinds this app version can apply, where a push goes
        and which timezone "today" is in."""
        kinds = body.get("kinds")
        token = body.get("push_token")
        try:
            offset = int(body.get("tz_offset_seconds") or 0)
        except (TypeError, ValueError):
            offset = 0
        targets = {}
        if isinstance(body.get("targets"), dict):
            for k, v in body["targets"].items():
                if (k in TARGET_BOUNDS and isinstance(v, (int, float))
                        and not isinstance(v, bool) and v == v and 0 <= v <= 1e6):
                    targets[k] = round(float(v), 1)
            if body["targets"].get("mode") in ("recommended", "custom"):
                targets["mode"] = body["targets"]["mode"]
        d = {
            "write_enabled": body.get("write_enabled") is True,
            "kinds": ([k for k in kinds if k in WRITE_KINDS or k in WRITE_CAPS]
                      if isinstance(kinds, list) else []),
            "targets": targets,
            "push_token": (token.lower()
                           if isinstance(token, str) and PUSH_TOKEN_RE.match(token)
                           else None),
            "push_env": ("sandbox" if body.get("push_env") == "sandbox"
                         else "production"),
            "app_version": str(body.get("app_version") or "")[:20],
            "tz_offset_seconds": max(-50400, min(offset, 50400)),
            "updated": time.time(),
        }
        self._save_device(d)
        return d

    def clear_push_token(self):
        d = self.device()
        if d.get("push_token"):
            d["push_token"] = None
            self._save_device(d)

    def local_now(self):
        """Now on the phone's clock (naive), from the offset it reported."""
        off = self.device().get("tz_offset_seconds") or 0
        return (datetime.now(timezone.utc)
                + timedelta(seconds=off)).replace(tzinfo=None)

    def _expire_inbox(self, now):
        self.conn.execute(
            "UPDATE inbox SET state='expired', done_at=? "
            "WHERE state='queued' AND created<?", (now, now - WB_EXPIRE_S))

    def queue_write(self, kind, payload, client, idem_key=None):
        """Queue one entry for the phone. Returns {id, state, duplicate}."""
        dev = self.device()
        if not dev.get("write_enabled"):
            raise ValueError(WB_DISABLED)
        if kind not in (dev.get("kinds") or []):
            raise ValueError(WB_UPDATE)
        now = time.time()
        self._expire_inbox(now)
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if idem_key not in (None, ""):
            idem = "k:" + str(idem_key)[:100]
            row = self.conn.execute(
                "SELECT id, state FROM inbox WHERE idem=? "
                "ORDER BY created DESC LIMIT 1", (idem,)).fetchone()
        else:
            idem = "h:" + hashlib.sha256(f"{kind}|{body}".encode()).hexdigest()
            row = self.conn.execute(
                "SELECT id, state FROM inbox WHERE idem=? AND created>? "
                "AND state!='cancelled' ORDER BY created DESC LIMIT 1",
                (idem, now - WB_DEDUP_S)).fetchone()
        if row:
            self.conn.commit()
            return {"id": row[0], "state": row[1], "duplicate": True}
        waiting = self.conn.execute(
            "SELECT COUNT(*) FROM inbox WHERE state='queued'").fetchone()[0]
        if waiting >= WB_MAX_QUEUED:
            self.conn.commit()
            raise ValueError("Too many entries are waiting for the phone. "
                             "Open Marrow so it can catch up, then try again.")
        wid = "wb_" + secrets.token_hex(8)
        self.conn.execute(
            "INSERT INTO inbox(id, kind, payload, client, created, state, idem) "
            "VALUES(?,?,?,?,?,'queued',?)",
            (wid, kind, body, str(client or "Your AI")[:60], now, idem))
        self.conn.commit()
        return {"id": wid, "state": "queued", "duplicate": False}

    def inbox_items(self, limit=WB_INBOX_PAGE):
        """What the phone still has to apply, oldest first."""
        self._expire_inbox(time.time())
        self.conn.commit()
        rows = self.conn.execute(
            "SELECT id, kind, payload, client, created FROM inbox "
            "WHERE state='queued' ORDER BY created, rowid LIMIT ?",
            (limit + 1,)).fetchall()
        items = [{"id": wid, "kind": kind, "payload": json.loads(payload),
                  "client": client or "Your AI", "created": created}
                 for wid, kind, payload, client, created in rows[:limit]]
        return {"items": items, "more": len(rows) > limit}

    def inbox_ack(self, results):
        """The phone's report. Idempotent: a second report for the same id
        changes nothing."""
        now = time.time()
        n = 0
        for r in (results if isinstance(results, list) else [])[:200]:
            if not isinstance(r, dict):
                continue
            wid = str(r.get("id") or "")
            row = self.conn.execute(
                "SELECT kind, payload, state, client FROM inbox WHERE id=?",
                (wid,)).fetchone()
            if not row:
                continue
            n += 1
            kind, payload, state, client = row
            if state not in ("queued", "cancelled"):
                continue
            ok = r.get("ok") is True
            ids = []
            if ok:
                ids = [str(x)[:80] for x in (r.get("entry_ids") or [])
                       if isinstance(x, (str, int))][:WB_MAX_ITEMS]
                result = {"entry_ids": ids}
            else:
                result = {"error": str(r.get("error") or "failed")[:200]}
            if state == "cancelled" and not ok:
                continue
            self.conn.execute(
                "UPDATE inbox SET state=?, done_at=?, result=? WHERE id=?",
                ("applied" if ok else "failed", now,
                 json.dumps(result, separators=(",", ":")), wid))
            if ok and kind == "undo":
                try:
                    target = str(json.loads(payload).get("target") or "")
                except ValueError:
                    target = ""
                self.conn.execute(
                    "UPDATE inbox SET state='undone', done_at=? "
                    "WHERE id=? AND state='applied'", (now, target))
            if ok and state == "cancelled" and kind not in NO_UNDO_KINDS:
                # Cancelled after the phone had already fetched it: the
                # entry exists on the phone, so the cancel becomes an undo.
                self.conn.execute(
                    "INSERT INTO inbox(id, kind, payload, client, created, "
                    "state, idem) VALUES(?,?,?,?,?,'queued',?)",
                    ("wb_" + secrets.token_hex(8), "undo",
                     json.dumps({"target": wid, "entry_ids": ids},
                                sort_keys=True, separators=(",", ":")),
                     client, now, "k:undo:" + wid))
        self.conn.commit()
        return {"acked": n}

    def undo_write(self, wid, client):
        row = self.conn.execute(
            "SELECT kind, state, result FROM inbox WHERE id=?",
            (str(wid or ""),)).fetchone()
        if not row:
            raise ValueError("No entry with that id. pending_entries lists "
                             "the recent ones.")
        kind, state, result = row
        if kind == "undo":
            raise ValueError("That id is an undo, not an entry.")
        if state == "queued":
            self.conn.execute(
                "UPDATE inbox SET state='cancelled', done_at=? WHERE id=?",
                (time.time(), wid))
            self.conn.commit()
            return {"status": "cancelled", "id": wid,
                    "note": "It had not reached the phone yet, so nothing "
                            "was logged."}
        if state == "failed":
            return {"status": "failed", "id": wid,
                    "note": "The phone could not apply it, so there is "
                            "nothing to remove."}
        if state != "applied":
            return {"status": state, "id": wid}
        if kind in NO_UNDO_KINDS:
            raise ValueError("That entry is already in Marrow and Apple "
                             "Health. Water and weight entries can only be "
                             "removed in the app.")
        try:
            ids = json.loads(result or "{}").get("entry_ids") or []
        except ValueError:
            ids = []
        q = self.queue_write("undo", {"target": wid, "entry_ids": ids},
                             client, idem_key="undo:" + wid)
        return {"status": "queued", "id": q["id"], "removes": wid,
                "new": not q["duplicate"]}

    def pending_entries(self, hours=48):
        rows = self.conn.execute(
            "SELECT id, kind, payload, created, state, result FROM inbox "
            "WHERE created>? ORDER BY created DESC LIMIT 100",
            (time.time() - hours * 3600,)).fetchall()
        out = []
        for wid, kind, payload, created, state, result in rows:
            try:
                p = json.loads(payload)
            except ValueError:
                p = {}
            e = {"id": wid, "kind": kind, "state": state,
                 "queued_at": datetime.fromtimestamp(created, timezone.utc)
                                      .strftime("%Y-%m-%dT%H:%M:%SZ")}
            if kind == "meal":
                items = p.get("items") or []
                e.update(day=p.get("day"), meal=p.get("meal"),
                         items=[i.get("name") for i in items],
                         kcal=round(sum(i.get("kcal") or 0 for i in items), 1))
            elif kind == "water":
                e.update(day=p.get("day"), ml=p.get("ml"))
            elif kind == "weight":
                e.update(day=p.get("day"), kg=p.get("kg"))
            elif kind == "undo":
                e.update(removes=p.get("target"))
            elif kind in ("edit", "delete"):
                e.update(entry_id=p.get("entry_id"), day=p.get("day"))
            elif kind == "workout":
                e.update(day=p.get("day"), activity=p.get("activity"),
                         minutes=p.get("minutes"))
            elif kind == "strength":
                ex = p.get("exercises") or []
                e.update(day=p.get("day"), name=p.get("name"),
                         exercises=[x.get("name") for x in ex],
                         sets=sum(len(x.get("sets") or []) for x in ex))
            elif kind == "sleep":
                e.update(bedtime=p.get("bedtime"), wake=p.get("wake"))
            elif kind == "symptom":
                e.update(day=p.get("day"), symptom=p.get("symptom"),
                         severity=p.get("severity"))
            elif kind == "mood":
                e.update(day=p.get("day"), feeling=p.get("feeling"),
                         labels=p.get("labels"))
            elif kind == "targets":
                e.update(set=p.get("set"))
            if state == "failed":
                try:
                    e["error"] = json.loads(result or "{}").get("error")
                except ValueError:
                    pass
            out.append(e)
        return out

    def entry_day(self, entry_id):
        """The diary day an entry id belongs to: a row the phone has pushed,
        or a row a queued meal is about to become. None when unknown."""
        entry_id = str(entry_id or "")
        row = self.conn.execute(
            "SELECT day FROM diary WHERE entry_id=?", (entry_id,)).fetchone()
        if row:
            return row[0]
        m = re.match(r"^(wb_[0-9a-f]+)-(\d+)$", entry_id)
        if not m:
            return None
        row = self.conn.execute(
            "SELECT payload FROM inbox WHERE id=? AND kind='meal' "
            "AND state IN ('queued','applied')", (m.group(1),)).fetchone()
        if not row:
            return None
        try:
            p = json.loads(row[0])
        except ValueError:
            return None
        if int(m.group(2)) >= len(p.get("items") or []):
            return None
        return p.get("day")

    def _unmirrored_meals(self, floor, present):
        """Meals an AI queued that the diary table does not hold yet:
        still waiting for the phone, or applied with the phone's diary push
        for that day not here yet. Once that push has arrived the diary
        table is the truth (a row missing from it was deleted on the
        phone). Yields (day, entry)."""
        now = time.time()
        seen = {}
        for k, v in self.conn.execute(
                "SELECT k, v FROM wb_state WHERE k LIKE 'diary_day:%'"):
            try:
                seen[k[10:]] = float(v)
            except ValueError:
                pass
        rows = self.conn.execute(
            "SELECT id, payload, client, state, done_at, result FROM inbox "
            "WHERE kind='meal' AND state IN ('queued','applied') "
            "ORDER BY created, rowid").fetchall()
        for wid, payload, client, state, done_at, result in rows:
            try:
                p = json.loads(payload)
            except ValueError:
                continue
            day = str(p.get("day") or "")
            items = p.get("items") or []
            if day < floor:
                continue
            ids = []
            if state == "applied":
                if (done_at or 0) < now - WB_UNMIRRORED_S:
                    continue
                if seen.get(day, 0) > (done_at or 0) + WB_PUSH_SLACK_S:
                    continue
                try:
                    ids = json.loads(result or "{}").get("entry_ids") or []
                except ValueError:
                    ids = []
                if ids and all(i in present for i in ids):
                    continue
            per_item = len(ids) == len(items)
            for n, it in enumerate(items):
                if per_item and ids[n] in present:
                    continue
                # The id the phone gives the row: inbox id + position.
                e = {"id": f"{wid}-{n}",
                     "meal": p.get("meal"), "name": it.get("name"),
                     "kcal": it.get("kcal"), "protein_g": it.get("protein_g"),
                     "carbs_g": it.get("carbs_g"), "fat_g": it.get("fat_g"),
                     "added_by": client or "Your AI", "entry": wid}
                if it.get("grams") is not None:
                    e["grams"] = it["grams"]
                if it.get("nutrients"):
                    e["nutrients"] = it["nutrients"]
                if state == "queued":
                    e["pending"] = True
                yield day, e

    # ---- reads for MCP ----

    def types(self):
        return self.conn.execute(
            "SELECT type, unit, COUNT(*), MIN(day), MAX(day) FROM records "
            "GROUP BY type ORDER BY 3 DESC").fetchall()

    def resolve(self, key):
        # The app's own keys work too (an agent moved from the phone's MCP
        # server keeps its vocabulary).
        key = (KEYMAP.get(key, (None,))[0] or CATEGORY_TYPES.get(key)
               or STREAM_TYPES.get(key) or key)
        for (t,) in self.conn.execute("SELECT DISTINCT type FROM records"):
            if t == key or short_key(t) == key:
                return t
        return None

    # Every read takes a window of phone-local days: `days` back from the
    # phone's today (the day column was written on the phone's calendar, so
    # the server's clock must not pick the floor), or an explicit
    # floor..ceil, both inclusive. FAR leaves the top open.
    FAR = "9999-12-31"

    def today(self):
        return self.local_now().strftime("%Y-%m-%d")

    def _floor(self, days, floor=None):
        if floor:
            return floor
        return (self.local_now() - timedelta(days=days)).strftime("%Y-%m-%d")

    def daily(self, apple_type, days, floor=None, ceil=None):
        floor, ceil = self._floor(days, floor), ceil or self.FAR
        if any(h in apple_type for h in SUM_HINTS):
            # Phone and watch both record the same walk. A flat SUM
            # double-counts (observed +35% on steps). Per-source day totals,
            # richest source wins - the honest approximation of Apple's
            # interval-level dedup, and the same rule the app's math applies.
            return self.conn.execute(
                "SELECT day, MAX(s) FROM ("
                "  SELECT day, source_name, SUM(num) AS s FROM records"
                "  WHERE type=? AND day>=? AND day<=? AND num IS NOT NULL"
                "  GROUP BY day, source_name"
                ") GROUP BY day ORDER BY day", (apple_type, floor, ceil)).fetchall()
        return self.conn.execute(
            "SELECT day, AVG(num) FROM records "
            "WHERE type=? AND day>=? AND day<=? AND num IS NOT NULL "
            "GROUP BY day ORDER BY day", (apple_type, floor, ceil)).fetchall()

    def samples(self, apple_type, days, limit, floor=None, ceil=None, before=None):
        """Newest first. `before` is the start of the last record a caller
        already has: the page after it, with nothing repeated or skipped."""
        floor, ceil = self._floor(days, floor), ceil or self.FAR
        return self.conn.execute(
            "SELECT start_date, end_date, value, num, source_name FROM records "
            "WHERE type=? AND day>=? AND day<=? AND start_date<? "
            "ORDER BY start_date DESC LIMIT ?",
            (apple_type, floor, ceil, before or self.FAR, limit)).fetchall()

    def workouts(self, days, floor=None, ceil=None):
        floor, ceil = self._floor(days, floor), ceil or self.FAR
        return self.conn.execute(
            "SELECT activity_type, start_date, end_date, duration, "
            "duration_unit, source_name FROM workouts WHERE day>=? AND day<=? "
            "ORDER BY start_date DESC", (floor, ceil)).fetchall()

    def nutrition(self, days, floor=None, ceil=None):
        """The meal diary: entries grouped by day, full nutrient detail."""
        floor, ceil = self._floor(days, floor), ceil or self.FAR
        rows = self.conn.execute(
            "SELECT day, meal, name, grams, kcal, protein, carbs, fat, "
            "logged_at, extra, entry_id FROM diary WHERE day>=? AND day<=? "
            "ORDER BY day, logged_at", (floor, ceil)).fetchall()
        out = {}
        present = {r[10] for r in rows}
        for (day, meal, name, grams, kcal, protein, carbs, fat,
             logged, extra, _entry) in rows:
            e = {"id": _entry, "meal": meal, "name": name, "kcal": kcal,
                 "protein_g": protein, "carbs_g": carbs, "fat_g": fat}
            if grams is not None:
                e["grams"] = grams
            try:
                nutrients = json.loads(extra or "{}")
            except ValueError:
                nutrients = {}
            if nutrients:
                e["nutrients"] = nutrients
            out.setdefault(day, []).append(e)
        for day, e in self._unmirrored_meals(floor, present):
            out.setdefault(day, []).append(e)
        return [{"day": d, "entries": es} for d, es in sorted(out.items())]

    def strength(self, days, floor=None, ceil=None):
        """Sets + per-exercise progression + muscle-group volume, mirroring
        what the app's own strength pages derive. Epley for est. 1RM, and
        muscle attribution comes stored per set (the phone sends it. The
        server has no exercise catalog of its own). Every set is listed
        with its type. Warm-ups are not progression, volume or credit, the
        rule the app's rings and records follow."""
        floor, ceil = self._floor(days, floor), ceil or self.FAR
        rows = self.conn.execute(
            "SELECT uuid, session_id, session_name, exercise, idx, weight, "
            "unit, reps, done_at, day, muscles, kind FROM strength_sets "
            "WHERE day>=? AND day<=? ORDER BY done_at, rowid", (floor, ceil)).fetchall()

        def e1rm(w, r):
            if not w or not r:
                return 0.0
            return w if r == 1 else w * (1 + r / 30)

        sets, by_exercise, muscle = [], {}, {}
        for (_, sid, sname, ex, idx, w, unit, reps, done, day, mus, kind) in rows:
            w = w or 0.0
            reps = reps or 0
            kind = kind or "work"
            sets.append({"time": done, "session": sname, "exercise": ex,
                         "set": (idx or 0) + 1, "weight": w, "unit": unit,
                         "reps": reps, "set_type": kind,
                         "est_1rm": round(e1rm(w, reps), 1)})
            if kind == "warmup":
                continue
            by_exercise.setdefault(ex, {}).setdefault(sid, []).append(
                (day, w, reps))
            try:
                credits = json.loads(mus or "[]")
            except ValueError:
                credits = []
            for c in credits:
                g = c.get("group")
                cr = float(c.get("credit") or 0)
                if g:
                    m = muscle.setdefault(g, {"volume": 0.0, "set_credits": 0.0})
                    m["volume"] += w * reps * cr
                    m["set_credits"] += cr

        exercises = []
        for ex in sorted(by_exercise):
            sessions = []
            for sid, ss in by_exercise[ex].items():
                best = max(e1rm(w, r) for _, w, r in ss)
                heaviest = max(ss, key=lambda t: t[1])
                sessions.append({"date": min(d for d, _, _ in ss),
                                 "sets": len(ss),
                                 "top_weight": heaviest[1],
                                 "top_reps": heaviest[2],
                                 "est_1rm": round(best, 1),
                                 "volume": round(sum(w * r for _, w, r in ss), 1)})
            sessions.sort(key=lambda s: s["date"])
            exercises.append({"exercise": ex,
                              "best_est_1rm": max(s["est_1rm"] for s in sessions),
                              "sessions": sessions})

        return {"sets": sets, "exercises": exercises,
                "muscle_groups": [
                    {"group": g, "volume": round(v["volume"], 1),
                     "set_credits": round(v["set_credits"], 1)}
                    for g, v in sorted(muscle.items())]}

    def symptoms_and_mood(self, days, floor=None, ceil=None):
        """Symptoms with Apple's severity words and State of Mind entries
        with their feelings, one entry each, newest last."""
        floor, ceil = self._floor(days, floor), ceil or self.FAR
        presence = {"HKCategoryTypeIdentifierSleepChanges",
                    "HKCategoryTypeIdentifierMoodChanges"}
        appetite = {"HKCategoryTypeIdentifierAppetiteChanges"}

        def severity(t, code):
            try:
                c = int(float(code))
            except (TypeError, ValueError):
                return "unspecified"
            if t in presence:
                return "present" if c == 0 else "not present"
            if t in appetite:
                return (["unspecified", "no change", "decreased", "increased"] + ["code %d" % c])[
                    c if 0 <= c <= 3 else 4]
            # Apple's "unspecified" (0) is a symptom present with no
            # severity. The Health app shows it as Present, and log_symptom
            # writes it for "present", so it reads back as that word.
            words = ["present", "not present", "mild", "moderate", "severe"]
            return words[c] if 0 <= c < len(words) else "code %d" % c

        def feeling(v):
            if v is None:
                return "unknown"
            # The seven values log_mood writes read back as the words it
            # took (a write and the read after it must agree). Any other
            # valence keeps the app's own scale (MoodSymptomExport.swift).
            for word, x in MOOD_FEELINGS.items():
                if abs(v - x) < 0.005:
                    return word
            if v < -0.6: return "very unpleasant"
            if v < -0.2: return "unpleasant"
            if v < -0.05: return "slightly unpleasant"
            if v <= 0.05: return "neutral"
            if v <= 0.2: return "slightly pleasant"
            if v <= 0.6: return "pleasant"
            return "very pleasant"

        symptom_ids = {"HKCategoryTypeIdentifier" + n for n in (
            "Headache Fatigue Nausea Dizziness Fever Coughing ShortnessOfBreath "
            "AbdominalCramps AppetiteChanges Acne BladderIncontinence Bloating BreastPain "
            "ChestTightnessOrPain Chills Constipation Diarrhea DrySkin Fainting "
            "GeneralizedBodyAche HairLoss Heartburn HotFlashes LossOfSmell LossOfTaste "
            "LowerBackPain MemoryLapse MoodChanges NightSweats PelvicPain "
            "RapidPoundingOrFlutteringHeartbeat RunnyNose SinusCongestion SkippedHeartbeat "
            "SleepChanges SoreThroat VaginalDryness Vomiting Wheezing").split()}
        out = []
        rows = self.conn.execute(
            "SELECT type, start_date, value, num, source_name FROM records "
            "WHERE day>=? AND day<=? AND (type LIKE 'HKCategoryTypeIdentifier%' "
            "OR type='HKStateOfMindTypeIdentifier') ORDER BY start_date",
            (floor, ceil)).fetchall()
        symptom_types = set()
        for t, s, v, num, src in rows:
            if t == "HKStateOfMindTypeIdentifier":
                parts = (v or "").split("|")
                kind = "daily mood" if parts[0] == "dailyMood" else "emotion"
                detail = feeling(num)
                if len(parts) > 1 and parts[1]:
                    detail += ", " + parts[1].replace(",", ", ")
                if len(parts) > 2 and parts[2]:
                    detail += " (" + parts[2].replace(",", ", ") + ")"
                out.append({"time": s[:16], "kind": kind, "valence": num, "feeling": detail,
                            "source": src})
            elif t in symptom_ids:
                name = short_key(t).replace("_", " ")
                try:
                    code = int(float(v))
                except (TypeError, ValueError):
                    code = None
                out.append({"time": s[:16], "kind": "symptom", "name": name,
                            "severity": severity(t, v), "severity_code": code,
                            "source": src})
                symptom_types.add(name)
        return {"entries": out, "symptom_types_seen": sorted(symptom_types),
                "scale": {"valence": "-1 very unpleasant to +1 very pleasant",
                          "severity": "Apple Health words"}}

    def sleep(self, days, gap_hours=3.0, min_hours=1.0, floor=None, ceil=None):
        """Nights, attributed to the wake day, by the app's exact rule:
        asleep segments closer than `gap_hours` form one session, the
        longest session per wake day is the night, and EVERY segment of
        that session counts (including the ones before midnight). Summing
        segments by their own calendar day drops the first hours of a night
        that starts before midnight, which is how a 6 h 52 m night reads
        as 5 h 13 m. Awake minutes are reported but not counted as sleep,
        matching Apple Health's "time asleep". With an explicit floor the
        nights are those that END on floor..ceil."""
        first = floor
        if floor:
            floor = (date.fromisoformat(floor) - timedelta(days=1)).isoformat()
        floor = self._floor(days + 1, floor)
        rows = self.conn.execute(
            "SELECT start_date, end_date, value FROM records "
            "WHERE type='HKCategoryTypeIdentifierSleepAnalysis' AND day>=? AND day<=? "
            "ORDER BY start_date", (floor, ceil or self.FAR)).fetchall()
        names = {"AsleepCore": "core", "AsleepDeep": "deep", "AsleepREM": "rem",
                 "AsleepUnspecified": "asleep", "Awake": "awake"}

        def parse(s):
            return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")

        segs = []
        for s, e, v in rows:
            stage = names.get(v.replace("HKCategoryValueSleepAnalysis", ""))
            if not stage:
                continue          # InBed and unknown values are not sleep
            try:
                a, b = parse(s), parse(e)
            except ValueError:
                continue
            if b > a:
                segs.append((a, b, stage))

        sessions, cur = [], None
        for a, b, stage in segs:
            if stage == "awake":
                continue
            if cur and (a - cur[1]).total_seconds() / 3600 <= gap_hours:
                cur[1] = max(cur[1], b)
                cur[2] += (b - a).total_seconds() / 3600
            else:
                if cur:
                    sessions.append(cur)
                cur = [a, b, (b - a).total_seconds() / 3600]
        if cur:
            sessions.append(cur)

        best = {}
        for a, b, h in sessions:
            if h < min_hours:
                continue
            key = b.strftime("%Y-%m-%d")
            if key not in best or h > best[key][2]:
                best[key] = (a, b, h)

        tol = timedelta(minutes=20)
        out = []
        for key in sorted(best):
            a, b, h = best[key]
            stages = {}
            for s, e, stage in segs:
                if s < b + tol and e > a - tol:
                    stages[stage] = stages.get(stage, 0.0) + (e - s).total_seconds() / 60
            out.append({"night": key,
                        "bedtime": a.strftime("%Y-%m-%d %H:%M"),
                        "wake": b.strftime("%Y-%m-%d %H:%M"),
                        "asleep_hours": round(h, 2),
                        "stages_min": {k: round(v) for k, v in sorted(stages.items())}})
        if first:
            return [n for n in out if first <= n["night"] <= (ceil or self.FAR)]
        return out[-days:]


# Every read takes `days` back from the phone's today, or an explicit
# from..to on the phone's calendar. The two are the same window either way.
_RANGE = {
    "from": {"type": "string",
             "description": "First day, YYYY-MM-DD on the phone's calendar "
                            "(optional, used instead of days)"},
    "to": {"type": "string",
           "description": "Last day, YYYY-MM-DD, inclusive (optional, "
                          "default today)"},
}
_DAY_ARG = {"type": "string",
            "description": "YYYY-MM-DD on the phone's calendar (default today)"}
_WINDOW_ARG = {"type": "integer",
               "description": "Days of history a value is scored against "
                              "(default 60, the app's own window, 7 to 90)"}


def _days(text):
    return {"days": {"type": "integer", "description": text}, **_RANGE}


_BY_DAY = {"type": "object", "additionalProperties": {"type": "number"},
           "description": "YYYY-MM-DD to value"}
_STRINGS = {"type": "array", "items": {"type": "string"}}

TOOLS = [
    {"name": "health_summary",
     "description": "The user's recent days as a plain table, one value "
                    "per day: steps, active calories, exercise minutes, "
                    "resting heart rate, heart rate variability, body weight, "
                    "calories eaten and hours asleep. For \"how am I doing\" "
                    "prefer daily_brief, which scores each value against the "
                    "user's own normal.",
     "inputSchema": {"type": "object", "properties":
                     _days("Recent days (default 3, max 30)")},
     "outputSchema": {"type": "object", "additionalProperties": _BY_DAY,
                      "description": "Metric key to its days"}},
    {"name": "list_metrics",
     "description": "Every Apple Health metric synced from the user's "
                    "iPhone, with its unit and the days it covers. Pass a "
                    "'key' from here to metric_daily, metric_samples or "
                    "baselines.",
     "inputSchema": {"type": "object", "properties": {}}},
    {"name": "metric_daily",
     "description": "One metric as one value per day (a day's total for "
                    "steps or calories, its average for heart rate and the "
                    "like). Prefer this to metric_samples for trends and "
                    "comparisons between days.",
     "inputSchema": {"type": "object", "properties": {
         "metric": {"type": "string", "description": "Metric key"},
         **_days("Days back (default 30, max 400)")},
         "required": ["metric"]},
     "outputSchema": {"type": "object", "properties": {
         "metric": {"type": "string"}, "days": _BY_DAY}}},
    {"name": "metric_samples",
     "description": "The individual records behind one metric, each with "
                    "its time, value and source app or device, newest "
                    "first. Use it when the times of single readings "
                    "matter. For daily totals use metric_daily. When a "
                    "page is full (limit records), pass the last record's "
                    "start as before to get the next page.",
     "inputSchema": {"type": "object", "properties": {
         "metric": {"type": "string", "description": "Metric key"},
         **_days("Days back (default 7, max 400)"),
         "limit": {"type": "integer",
                   "description": "Max records (default 200, max 1000)"},
         "before": {"type": "string",
                    "description": "Paging: the start of the last record "
                                   "you already have. Returns the records "
                                   "before it."}},
         "required": ["metric"]}},
    {"name": "workouts",
     "description": "The user's workouts from Apple Health, from every app "
                    "and device: activity, start, end and duration.",
     "inputSchema": {"type": "object", "properties":
                     _days("Days back (default 30, max 400)")}},
    {"name": "nutrition_log",
     "description": "The meal diary: every logged food with full nutrient "
                    "detail, grouped by day. Entries an AI added that the "
                    "phone has not applied yet are included and marked "
                    "pending.",
     "inputSchema": {"type": "object", "properties":
                     _days("Days back (default 7, max 90)")}},
    {"name": "strength_log",
     "description": "Weightlifting detail: every set (exercise, weight, "
                    "reps, set_type: work, warmup, failure or drop) with "
                    "estimated 1RM, per-exercise progression, and volume "
                    "per muscle group. Warm-up sets are listed but left out "
                    "of progression, volume and muscle credits, as in the "
                    "app.",
     "inputSchema": {"type": "object", "properties":
                     _days("Days back (default 90, max 400)")},
     "outputSchema": {"type": "object", "properties": {
         "sets": {"type": "array"}, "exercises": {"type": "array"},
         "muscle_groups": {"type": "array"}}}},
    {"name": "symptoms_and_mood",
     "description": "Symptoms logged in the Health app with their severity in "
                    "words, and State of Mind entries (daily mood or momentary "
                    "emotion) with valence, feelings and what they were about. "
                    "Use this for questions about symptoms or mood the user "
                    "logged in Apple Health or Marrow.",
     "inputSchema": {"type": "object", "properties":
                     _days("Days back (default 30, max 400)")},
     "outputSchema": {"type": "object", "properties": {
         "entries": {"type": "array"}, "symptom_types_seen": _STRINGS,
         "scale": {"type": "object"}}}},
    {"name": "sleep_log",
     "description": "Nights of sleep, one per wake day, exactly as the app "
                    "and Apple Health count them: bedtime, wake time, hours "
                    "asleep, and minutes per stage (core, deep, rem, awake). "
                    "Use this for any sleep question instead of raw "
                    "metric_samples, which cannot be summed by calendar day.",
     "inputSchema": {"type": "object", "properties":
                     _days("Nights back (default 7, max 90)")}},
    # The model-ready reads. Each answers in the app's own terms, scored
    # against the user's trailing normal with the math Home uses
    # (Core/Derive.swift), so an AI starts from a read that is already
    # true for this person instead of judging a number against the
    # population.
    {"name": "daily_brief",
     "description": "One day in the app's own terms: each key metric with "
                    "its value and a phrase against the user's own normal "
                    "(normal for you, above your normal, well below your "
                    "normal), food and water against the targets, last "
                    "night's sleep, training done, mood and symptoms, and "
                    "flags worth mentioning. Start here for how am I "
                    "doing, what should I know today, or before giving any "
                    "advice. Default day is the phone's today.",
     "inputSchema": {"type": "object", "properties": {
         "day": _DAY_ARG, "window": _WINDOW_ARG}},
     "outputSchema": {"type": "object", "properties": {
         "day": {"type": "string"}, "as_of": {"type": "string"},
         "metrics": {"type": "array"}, "food": {"type": "object"},
         "water": {"type": "object"}, "sleep": {"type": ["object", "null"]},
         "training": {"type": "object"}, "mood": {"type": "object"},
         "targets": {"type": ["object", "null"]}, "flags": _STRINGS,
         "data": {"type": "object"}}}},
    {"name": "baselines",
     "description": "The user's own normal per metric: mean and spread over "
                    "the trailing window, the latest value's z and phrase, "
                    "the 7-day mean and the 90-day trend per month. The "
                    "same math as the app's Home page. Use it to say whether "
                    "a number is high or low for this person instead of "
                    "against population figures.",
     "inputSchema": {"type": "object", "properties": {
         "metrics": {"type": "array", "items": {"type": "string"},
                     "description": "Metric keys from list_metrics, or "
                                    "sleep_hours (default: the key metrics)"},
         "window": _WINDOW_ARG,
         "day": {"type": "string",
                 "description": "Score as of this day, YYYY-MM-DD "
                                "(default today)"}}},
     "outputSchema": {"type": "object", "properties": {
         "day": {"type": "string"}, "window_days": {"type": "integer"},
         "metrics": {"type": "array"}, "missing": _STRINGS,
         "how_to_read": {"type": "string"}}}},
    {"name": "what_changed",
     "description": "The recent days against the period before them: per "
                    "metric the recent and prior means, the change and how "
                    "notable it is for this person, plus food, training, "
                    "sleep and bedtime shifts, and any stream that started "
                    "or stopped arriving. Notable items come first. Use it "
                    "for weekly reviews and for what is different lately.",
     "inputSchema": {"type": "object", "properties": {
         "days": {"type": "integer",
                  "description": "Recent window in days (default 7, max 30)"},
         "compare_days": {"type": "integer",
                          "description": "The prior window it is compared "
                                         "with (default 28, max 90)"},
         "day": {"type": "string",
                 "description": "Last day of the recent window, YYYY-MM-DD "
                                "(default today)"}}},
     "outputSchema": {"type": "object", "properties": {
         "recent": {"type": "object"}, "prior": {"type": "object"},
         "metrics": {"type": "array"}, "food": {"type": "object"},
         "training": {"type": "object"}, "sleep": {"type": "object"},
         "streams": {"type": "object"}, "notable": _STRINGS}}},
    {"name": "data_quality",
     "description": "What the mirror actually holds and how fresh it is: "
                    "per metric the latest day, coverage over the recent "
                    "window, cadence (daily or occasional), its sources and "
                    "whether it has gone stale, plus diary, sleep and "
                    "training coverage, when the phone last synced and "
                    "whether write-back is on. Call it before trusting an "
                    "absence: no sleep in the mirror is not the same as no "
                    "sleep.",
     "inputSchema": {"type": "object", "properties": {
         "days": {"type": "integer",
                  "description": "Window for coverage (default 14, max 90)"}}},
     "outputSchema": {"type": "object", "properties": {
         "today": {"type": "string"}, "last_sync": {"type": ["string", "null"]},
         "window_days": {"type": "integer"}, "metrics": {"type": "array"},
         "diary": {"type": "object"}, "sleep": {"type": "object"},
         "training": {"type": "object"}, "write_back": {"type": "object"},
         "issues": _STRINGS}}},
]

_WB_WHEN = {
    "day": {"type": "string",
            "description": "YYYY-MM-DD on the user's phone (default today, "
                           "up to 30 days back, never the future)"},
    "time": {"type": "string",
             "description": "HH:MM, 24 hour, phone time (optional)"},
    "idempotency_key": {"type": "string",
                        "description": "Optional. The same key never logs "
                                       "twice. Use a new one to log an "
                                       "identical entry again on purpose."},
}
_WB_KEY = {"idempotency_key": _WB_WHEN["idempotency_key"]}

WRITE_TOOLS = [
    {"name": "log_meal",
     "description": "Add foods to the user's Marrow food diary. Marrow on "
                    "their phone applies the entry (labelled as added by "
                    "you) and writes it to Apple Health. One call per meal. "
                    "Every item needs calories and macros for the portion "
                    "actually eaten: use label or database values when you "
                    "have them, otherwise your best estimate, and tell the "
                    "user which. Check nutrition_log first when they say "
                    "'the same as yesterday'. Returns an id that undo_entry "
                    "accepts.",
     "inputSchema": {"type": "object", "properties": {
         "meal": {"type": "string",
                  "enum": ["breakfast", "lunch", "dinner", "snack"],
                  "description": "Default: by time of day"},
         "items": {"type": "array", "minItems": 1, "maxItems": WB_MAX_ITEMS,
                   "items": {"type": "object", "properties": {
                       "name": {"type": "string"},
                       "grams": {"type": "number",
                                 "description": "Portion weight, if known"},
                       "kcal": {"type": "number"},
                       "protein_g": {"type": "number"},
                       "carbs_g": {"type": "number"},
                       "fat_g": {"type": "number"},
                       "nutrients": {
                           "type": "object",
                           "description": "Optional extras for the portion, "
                                          "keyed like the dietary_* metrics "
                                          "in list_metrics (dietary_fiber "
                                          "and dietary_sugar in g, "
                                          "dietary_sodium in mg)",
                           "additionalProperties": {"type": "number"}}},
                       "required": ["name", "kcal", "protein_g", "carbs_g",
                                    "fat_g"]}},
         **_WB_WHEN},
         "required": ["items"]}},
    {"name": "edit_entry",
     "description": "Change one food diary entry: its portion, its numbers "
                    "or its meal. Works on any entry in nutrition_log, the "
                    "user's own or one you added, by its id. Give scale to "
                    "multiply the whole entry (0.5 = half), or grams to set "
                    "a new portion (calories and macros follow in "
                    "proportion unless you also give them), or the numbers "
                    "to set outright. undo_entry with the returned id puts "
                    "the entry back as it was.",
     "inputSchema": {"type": "object", "properties": {
         "entry_id": {"type": "string",
                      "description": "The entry's id from nutrition_log"},
         "scale": {"type": "number",
                   "description": "Multiply portion and nutrients by this"},
         "grams": {"type": "number"}, "kcal": {"type": "number"},
         "protein_g": {"type": "number"}, "carbs_g": {"type": "number"},
         "fat_g": {"type": "number"},
         "meal": {"type": "string",
                  "enum": ["breakfast", "lunch", "dinner", "snack"]},
         **_WB_KEY},
         "required": ["entry_id"]}},
    {"name": "delete_entry",
     "description": "Remove one food diary entry (from Marrow and Apple "
                    "Health) by its id from nutrition_log, the user's own "
                    "or one you added. Only when the user asks. undo_entry "
                    "with the returned id restores it.",
     "inputSchema": {"type": "object", "properties": {
         "entry_id": {"type": "string",
                      "description": "The entry's id from nutrition_log"},
         **_WB_KEY},
         "required": ["entry_id"]}},
    {"name": "log_water",
     "description": "Add water the user drank (ml or oz) to Marrow and "
                    "Apple Health.",
     "inputSchema": {"type": "object", "properties": {
         "ml": {"type": "number"}, "oz": {"type": "number"}, **_WB_WHEN}}},
    {"name": "log_weight",
     "description": "Record the user's body weight (kg or lb) in Marrow and "
                    "Apple Health, with body fat percent if they gave it.",
     "inputSchema": {"type": "object", "properties": {
         "kg": {"type": "number"}, "lb": {"type": "number"},
         "body_fat_percent": {"type": "number"}, **_WB_WHEN}}},
    {"name": "log_workout",
     "description": "Record a workout the user did but did not track: a "
                    "run, a ride, a class. Goes to Apple Health as a "
                    "workout. For weightlifting with sets and reps use "
                    "log_strength instead.",
     "inputSchema": {"type": "object", "properties": {
         "activity": {"type": "string", "enum": list(ACTIVITIES)},
         "minutes": {"type": "number", "description": "How long it lasted"},
         "kcal": {"type": "number",
                  "description": "Active calories burned, if known"},
         "name": {"type": "string",
                  "description": "What to call it (optional)"},
         "day": _WB_WHEN["day"],
         "time": {"type": "string",
                  "description": "HH:MM it started, 24 hour, phone time"},
         **_WB_KEY},
         "required": ["activity", "minutes"]}},
    {"name": "log_strength",
     "description": "Record a weightlifting session with its sets: every "
                    "exercise, reps and weight, and a kind when a set is "
                    "a warm-up, a failure set or a drop set. Sets live in "
                    "Marrow's strength log (strength_log shows them). Apple "
                    "Health gets the workout. Use the exercise names the "
                    "user already has in strength_log where they match.",
     "inputSchema": {"type": "object", "properties": {
         "name": {"type": "string",
                  "description": "Session name, e.g. Push day (optional)"},
         "exercises": {"type": "array", "minItems": 1, "maxItems": 30,
                       "items": {"type": "object", "properties": {
                           "name": {"type": "string"},
                           "sets": {"type": "array", "minItems": 1,
                                    "maxItems": 30,
                                    "items": {"type": "object", "properties": {
                                        "reps": {"type": "integer"},
                                        "weight_lb": {"type": "number"},
                                        "weight_kg": {"type": "number"},
                                        "kind": {"type": "string",
                                                 "enum": list(SET_KINDS),
                                                 "description": "Set type "
                                                 "(default work)"}},
                                        "required": ["reps"]}}},
                           "required": ["name", "sets"]}},
         "minutes": {"type": "number",
                     "description": "How long it lasted (default 45)"},
         "day": _WB_WHEN["day"],
         "time": {"type": "string",
                  "description": "HH:MM it started, 24 hour, phone time"},
         **_WB_KEY},
         "required": ["exercises"]}},
    {"name": "log_sleep",
     "description": "Record a night (or nap) the watch missed: when the "
                    "user fell asleep and woke, phone time. Refused when "
                    "that time already has sleep recorded (check sleep_log).",
     "inputSchema": {"type": "object", "properties": {
         "bedtime": {"type": "string",
                     "description": "YYYY-MM-DD HH:MM, 24 hour"},
         "wake": {"type": "string",
                  "description": "YYYY-MM-DD HH:MM, 24 hour"},
         **_WB_KEY},
         "required": ["bedtime", "wake"]}},
    {"name": "log_symptom",
     "description": "Record a symptom the user reports in Apple Health, "
                    "with how bad it is. Only what they tell you. Never "
                    "infer a symptom.",
     "inputSchema": {"type": "object", "properties": {
         "symptom": {"type": "string", "enum": list(SYMPTOMS)},
         "severity": {"type": "string", "enum": list(SEVERITIES),
                      "description": "Default: present (no severity given)"},
         "minutes": {"type": "number",
                     "description": "How long it lasted, if they said"},
         **_WB_WHEN},
         "required": ["symptom"]}},
    {"name": "log_mood",
     "description": "Record how the user feels in Apple Health's State of "
                    "Mind (needs iOS 18): a momentary emotion or the mood "
                    "of the day, on Apple's scale, with the feelings and "
                    "what it is about when they say.",
     "inputSchema": {"type": "object", "properties": {
         "feeling": {"type": "string", "enum": list(MOOD_FEELINGS),
                     "description": "Apple's seven-step scale"},
         "kind": {"type": "string", "enum": ["momentary", "daily"],
                  "description": "Default: momentary"},
         "labels": {"type": "array", "maxItems": 6,
                    "items": {"type": "string", "enum": list(MOOD_LABELS)}},
         "about": {"type": "array", "maxItems": 4,
                   "items": {"type": "string", "enum": list(MOOD_ABOUT)}},
         **_WB_WHEN},
         "required": ["feeling"]}},
    {"name": "set_targets",
     "description": "Change the user's daily targets in Marrow: calories, "
                    "macros, water, steps, sleep, exercise minutes. Only "
                    "the ones you pass change. Calorie and macro targets "
                    "switch to custom (the app stops recomputing them). "
                    "Read targets first, and say what you changed.",
     "inputSchema": {"type": "object", "properties": {
         "kcal": {"type": "number"}, "protein_g": {"type": "number"},
         "carbs_g": {"type": "number"}, "fat_g": {"type": "number"},
         "water_ml": {"type": "number"}, "steps": {"type": "number"},
         "sleep_hours": {"type": "number"},
         "exercise_minutes": {"type": "number"}, **_WB_KEY}}},
    {"name": "undo_entry",
     "description": "Take back something you did, by the id its tool "
                    "returned. Still waiting for the phone: cancelled. "
                    "Already applied: a meal, workout, sleep, symptom or "
                    "mood entry is removed from Marrow and Apple Health. "
                    "an edit, a delete or a targets change is put back as "
                    "it was. Water and weight already on the phone can "
                    "only be removed in the app.",
     "inputSchema": {"type": "object", "properties": {
         "id": {"type": "string", "description": "The id a tool returned"}},
         "required": ["id"]}},
    {"name": "pending_entries",
     "description": "What you sent in the last 48 hours and what became "
                    "of each: queued (waiting for the phone), applied, "
                    "failed, cancelled or undone.",
     "inputSchema": {"type": "object", "properties": {}}},
    {"name": "targets",
     "description": "The user's daily targets as set in Marrow: calories, "
                    "macros, water, steps, sleep and exercise minutes.",
     "inputSchema": {"type": "object", "properties": {}},
     "outputSchema": {"type": "object", "properties": {
         "targets": {"type": ["object", "null"]}, "as_of": {"type": "string"},
         "note": {"type": "string"}}}},
]
READ_TOOLS = TOOLS
TOOLS = READ_TOOLS + WRITE_TOOLS

# ---- what every MCP client sees, the same from both servers ----
# Hand-written titles (Claude's permission prompts show them) and the
# directories' annotations: writes that change or remove data are
# destructive (Claude asks first), only the writes that repeat harmlessly
# are idempotent, nothing reaches outside the user's own mirror.
TOOL_TITLES = {
    "health_summary": "Health summary",
    "list_metrics": "List health metrics",
    "metric_daily": "Daily values for a metric",
    "metric_samples": "Recorded samples for a metric",
    "workouts": "Workouts",
    "nutrition_log": "Food diary",
    "strength_log": "Strength training log",
    "symptoms_and_mood": "Symptoms and mood",
    "sleep_log": "Sleep log",
    "daily_brief": "Daily brief",
    "baselines": "Your baselines",
    "what_changed": "What changed",
    "data_quality": "Data quality",
    "targets": "Daily targets",
    "pending_entries": "Changes sent by the AI",
    "log_meal": "Log a meal",
    "log_water": "Log water",
    "log_weight": "Log weight",
    "log_workout": "Log a workout",
    "log_strength": "Log a lifting session",
    "log_sleep": "Log sleep",
    "log_symptom": "Log a symptom",
    "log_mood": "Log mood",
    "edit_entry": "Edit a food entry",
    "delete_entry": "Delete a food entry",
    "set_targets": "Change daily targets",
    "undo_entry": "Undo a change",
}
DESTRUCTIVE_TOOLS = frozenset(("edit_entry", "delete_entry", "undo_entry", "set_targets"))
IDEMPOTENT_WRITES = frozenset(("set_targets", "delete_entry", "undo_entry"))


def annotate(tools):
    return [
        {**t, "title": TOOL_TITLES.get(t["name"]) or t["name"].replace("_", " ").title(),
         "annotations": {"title": (TOOL_TITLES.get(t["name"])
                                   or t["name"].replace("_", " ").title()),
                         "readOnlyHint": t["name"] not in WRITE_TOOL_NAMES,
                         "destructiveHint": t["name"] in DESTRUCTIVE_TOOLS,
                         "idempotentHint": (t["name"] not in WRITE_TOOL_NAMES
                                            or t["name"] in IDEMPOTENT_WRITES),
                         "openWorldHint": False}}
        for t in tools]


ANNOTATED_TOOLS = annotate(TOOLS)
ANNOTATED_READ_TOOLS = annotate(READ_TOOLS)

CAPABILITIES = {"tools": {"listChanged": False},
                "prompts": {"listChanged": False},
                "resources": {"subscribe": False, "listChanged": False}}

INSTRUCTIONS = (
    "This user's Marrow Health mirror: Apple Health metrics, meals, workouts, "
    "strength sets, sleep, symptoms and mood, synced from their iPhone. Start "
    "with daily_brief (today in the app's own terms, each value scored against "
    "the user's own normal) or what_changed for a review. baselines says "
    "whether a number is high or low for this person. data_quality says what "
    "is missing or stale: check it before concluding something did not happen. "
    "Reads take days back, or from and to on the phone's calendar. Health "
    "metrics are in stored SI units and strength sets carry their unit. If the "
    "user has turned on Allow AI write in the app you can also log and change "
    "things when they ask: meals (log_meal, edit_entry, delete_entry), water, "
    "weight, workouts (log_workout, log_strength), sleep, symptoms, mood and "
    "daily targets. The phone applies each change and undo_entry takes it "
    "back. Never log, change or remove anything the user did not ask for.")

# Prompts: the questions people actually bring, as one click. Each says
# which tools to call and how to speak (the user's own normal, neutral
# wellness wording, missing data named rather than guessed).
PROMPTS = [
    {"name": "how_am_i_doing", "title": "How am I doing",
     "description": "An honest read of today against the user's own normal, "
                    "from daily_brief.",
     "arguments": []},
    {"name": "weekly_review", "title": "Weekly review",
     "description": "The last seven days against the four weeks before: what "
                    "moved, what held, one thing to try.",
     "arguments": []},
    {"name": "doctor_prep", "title": "Prepare for a doctor visit",
     "description": "One page a clinician can read in a minute: 90 days of "
                    "resting heart rate, HRV, weight, sleep, activity and "
                    "symptoms with trends, and questions worth asking.",
     "arguments": [{"name": "focus",
                    "description": "What the visit is about, if known",
                    "required": False}]},
    {"name": "log_from_text", "title": "Log from text",
     "description": "Turn a plain sentence about what the user ate, drank, "
                    "lifted or weighed into the right log calls, then "
                    "confirm what was logged.",
     "arguments": [{"name": "text",
                    "description": "What to log, in the user's words",
                    "required": True}]},
]

PROMPT_TEXT = {
    "how_am_i_doing": (
        "Give me an honest read of how I am doing today.\n\n"
        "Call daily_brief first. If anything in it is flagged, call baselines "
        "for that metric to see the trend. Then answer in a few short lines: "
        "what is normal for me today, what is above or below my normal, how "
        "food and water stand against my targets, and one thing worth doing "
        "with the rest of the day. Use the brief's own phrases (normal for "
        "you, above your normal) rather than population ranges. Do not "
        "diagnose or alarm. If a stream is stale or missing, say so plainly "
        "instead of guessing."),
    "weekly_review": (
        "Review my last seven days.\n\n"
        "Call what_changed (days 7, compare_days 28) and baselines. Then "
        "write a short review in four parts: what moved and whether it is "
        "notable for me, what held steady, what the training and food logs "
        "show, and one specific thing to try next week. Quote the numbers "
        "you lean on with their units. Prefer trends to single days. Neutral "
        "wellness wording, no medical claims. If a stream started or stopped "
        "arriving, mention it."),
    "doctor_prep": (
        "Help me prepare for a doctor visit{focus}.\n\n"
        "Call data_quality, then baselines with window 90 for "
        "resting_heart_rate, heart_rate_variability_s_d_n_n, body_mass, "
        "sleep_hours, step_count and dietary_energy_consumed, then "
        "symptoms_and_mood for 90 days and sleep_log for 30. Write one page "
        "a clinician can read in a minute: a line per measure with the "
        "90-day mean, the range and the trend, the symptoms logged with "
        "dates and severity, the sleep pattern, and the activity level. Then "
        "list three to five questions worth asking at the visit. State which "
        "measures are missing or stale. Report, do not interpret clinically."),
    "log_from_text": (
        "Log this in Marrow: {text}\n\n"
        "Work out what it is (food, water, weight, a workout, lifting sets, "
        "sleep, a symptom or a mood) and call the matching log tool once per "
        "item with sensible amounts. Use today and the current time unless "
        "the text says otherwise. If a quantity is unclear, make a reasonable "
        "estimate and say so. Afterwards, confirm in one line what was "
        "logged. If write-back is turned off in the app, say how to turn it "
        "on (Marrow, Export, Allow AI write)."),
}

# Resources: the context an AI should have before it is asked anything.
RESOURCES = [
    {"uri": "marrow://today", "name": "today", "title": "Today's brief",
     "description": "daily_brief for the phone's today", "mimeType": "application/json"},
    {"uri": "marrow://metrics", "name": "metrics", "title": "Health metrics",
     "description": "list_metrics: every metric the mirror holds", "mimeType": "application/json"},
    {"uri": "marrow://targets", "name": "targets", "title": "Daily targets",
     "description": "The user's daily targets", "mimeType": "application/json"},
    {"uri": "marrow://quality", "name": "quality", "title": "Data quality",
     "description": "data_quality: what is fresh, stale or missing", "mimeType": "application/json"},
]
RESOURCE_TOOLS = {"marrow://today": "daily_brief", "marrow://metrics": "list_metrics",
                  "marrow://targets": "targets", "marrow://quality": "data_quality"}


class RpcError(Exception):
    """A JSON-RPC error (not a tool result): an unknown prompt, resource
    or argument."""
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def get_prompt(name, args):
    name = str(name or "")
    spec = next((p for p in PROMPTS if p["name"] == name), None)
    if not spec:
        raise RpcError(-32602, f"unknown prompt: {name[:40]}")
    args = args if isinstance(args, dict) else {}
    fill = {}
    for a in spec["arguments"]:
        v = args.get(a["name"])
        v = str(v).strip()[:2000] if v not in (None, "") else ""
        if a["required"] and not v:
            raise RpcError(-32602, f"{a['name']} is required")
        fill[a["name"]] = v
    if name == "doctor_prep":
        fill["focus"] = f" about {fill['focus']}" if fill.get("focus") else ""
    text = PROMPT_TEXT[name].format(**fill)
    return {"description": spec["description"],
            "messages": [{"role": "user", "content": {"type": "text", "text": text}}]}


def read_resource(mirror, uri):
    tool = RESOURCE_TOOLS.get(str(uri or ""))
    if not tool:
        raise RpcError(-32002, f"unknown resource: {str(uri or '')[:60]}")
    payload = call_tool(mirror, tool, {})
    return {"contents": [{"uri": uri, "mimeType": "application/json",
                          "text": json.dumps(payload, sort_keys=True)}]}


def mcp_extra(mirror, method, params):
    """prompts/* and resources/*, identical on both servers. None for a
    method that is not one of them."""
    params = params if isinstance(params, dict) else {}
    if method == "prompts/list":
        return {"prompts": PROMPTS}
    if method == "prompts/get":
        return get_prompt(params.get("name"), params.get("arguments"))
    if method == "resources/list":
        return {"resources": RESOURCES}
    if method == "resources/templates/list":
        return {"resourceTemplates": []}
    if method == "resources/read":
        return read_resource(mirror, params.get("uri"))
    return None


def tool_result(payload):
    """A tools/call result: the JSON as text for every client, and the
    same object as structuredContent for the clients that read it (the
    tools with an outputSchema always answer with an object)."""
    out = {"content": [{"type": "text",
                        "text": json.dumps(payload, sort_keys=True)}],
           "isError": False}
    if isinstance(payload, dict):
        out["structuredContent"] = payload
    return out

WB_DELIVERY = ("Sent to the phone. It appears in Marrow within seconds when "
               "the app is open or notifications are on, otherwise the next "
               "time Marrow opens.")


def _wb_num(src, key, lo, hi, required=True):
    v = src.get(key)
    if v is None or isinstance(v, bool):
        if required:
            raise ValueError(f"{key} is required")
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be a number")
    if f != f or f < lo or f > hi:
        raise ValueError(f"{key} must be between {lo:g} and {hi:g}")
    return round(f, 3)


def _wb_day(mirror, args):
    """The diary day an entry lands in, on the PHONE's calendar."""
    now = mirror.local_now()
    today = now.strftime("%Y-%m-%d")
    day = args.get("day")
    if day in (None, "", "today"):
        return today, now
    if day == "yesterday":
        return (now - timedelta(days=1)).strftime("%Y-%m-%d"), now
    try:
        key = datetime.strptime(str(day), "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        raise ValueError("day must be YYYY-MM-DD")
    if key > today:
        raise ValueError(f"day cannot be in the future (it is {today} on "
                         "the phone)")
    if key < (now - timedelta(days=WB_DAYS_BACK)).strftime("%Y-%m-%d"):
        raise ValueError(f"day cannot be more than {WB_DAYS_BACK} days back")
    return key, now


def _wb_time(args):
    t = args.get("time")
    if t in (None, ""):
        return None
    m = re.match(r"^([01]?\d|2[0-3]):([0-5]\d)$", str(t).strip())
    if not m:
        raise ValueError("time must be HH:MM (24 hour)")
    return f"{int(m.group(1)):02d}:{m.group(2)}"


def _wb_nutrients(raw):
    if not isinstance(raw, dict):
        return None
    out = {}
    for k, v in list(raw.items())[:40]:
        if (isinstance(k, str) and NUTRIENT_KEY_RE.match(k)
                and isinstance(v, (int, float)) and not isinstance(v, bool)
                and v == v and 0 < v <= 100000):
            out[k] = round(float(v), 3)
    return out or None


def _wb_meal(args, when, day, now, default=True):
    meal = str(args.get("meal") or "").strip().lower()
    if meal == "snacks":
        meal = "snack"
    if not meal:
        if not default:
            return None
        hour = (int(when[:2]) if when
                else now.hour if day == now.strftime("%Y-%m-%d") else 12)
        meal = ("breakfast" if hour < 10 else "lunch" if hour < 15
                else "dinner" if hour < 21 else "snack")
    if meal not in MEALS:
        raise ValueError("meal must be breakfast, lunch, dinner or snack")
    return meal


def _wb_entry(mirror, args):
    """An existing diary entry by id, inside the editable window."""
    entry_id = str(args.get("entry_id") or "").strip()
    day = mirror.entry_day(entry_id) if entry_id else None
    if not day:
        raise ValueError("No entry with that id. nutrition_log lists every "
                         "entry with its id.")
    now = mirror.local_now()
    if day < (now - timedelta(days=WB_DAYS_BACK)).strftime("%Y-%m-%d"):
        raise ValueError(f"That entry is more than {WB_DAYS_BACK} days old. "
                         "Change it in the app.")
    return entry_id, day


def _wb_clock(mirror, value, what):
    """'YYYY-MM-DD HH:MM' on the phone's clock -> (datetime, normalised)."""
    text = str(value or "").strip().replace("T", " ")[:16]
    try:
        dt = datetime.strptime(text, "%Y-%m-%d %H:%M")
    except ValueError:
        raise ValueError(f"{what} must be YYYY-MM-DD HH:MM (24 hour)")
    return dt, dt.strftime("%Y-%m-%d %H:%M")


def _wb_choices(raw, allowed, what, limit):
    if raw in (None, ""):
        return []
    if not isinstance(raw, list):
        raise ValueError(f"{what} must be a list")
    out = []
    for v in raw[:limit]:
        name = str(v).strip().lower().replace("_", " ")
        if name not in allowed:
            raise ValueError(f"unknown {what} value: {v}")
        if name not in out:
            out.append(name)
    return out


def call_write_tool(mirror, name, args, client=None, on_queued=None):
    """The write tools. `on_queued(kind, id)` fires after a new entry is
    queued (the hosted service sends the phone a push from it)."""
    if name == "pending_entries":
        return mirror.pending_entries()
    if name == "undo_entry":
        out = mirror.undo_write(args.get("id"), client)
        if out.get("status") == "queued":
            if out.pop("new", False) and on_queued:
                on_queued("undo", out["id"])
            out["delivery"] = WB_DELIVERY
        return out

    key = args.get("idempotency_key")
    if name == "log_meal":
        day, now = _wb_day(mirror, args)
        when = _wb_time(args)
        meal = _wb_meal(args, when, day, now)
        items = args.get("items")
        if not isinstance(items, list) or not items:
            raise ValueError("items must be a list with at least one food")
        if len(items) > WB_MAX_ITEMS:
            raise ValueError(f"at most {WB_MAX_ITEMS} foods per call")
        clean = []
        for it in items:
            if not isinstance(it, dict):
                raise ValueError("each item must be an object")
            food = " ".join(str(it.get("name") or "").split())[:120]
            if not food:
                raise ValueError("every item needs a name")
            clean.append({
                "name": food,
                "grams": _wb_num(it, "grams", 0, 5000, required=False),
                "kcal": _wb_num(it, "kcal", 0, 5000),
                "protein_g": _wb_num(it, "protein_g", 0, 1000),
                "carbs_g": _wb_num(it, "carbs_g", 0, 1000),
                "fat_g": _wb_num(it, "fat_g", 0, 1000),
                "nutrients": _wb_nutrients(it.get("nutrients")),
            })
        kind = "meal"
        payload = {"day": day, "meal": meal, "time": when, "items": clean}
        extra = {"day": day, "meal": meal, "totals": {
            k: round(sum(i[k] for i in clean), 1)
            for k in ("kcal", "protein_g", "carbs_g", "fat_g")}}
    elif name == "edit_entry":
        entry_id, day = _wb_entry(mirror, args)
        changes = {}
        for field, hi in (("grams", 5000), ("kcal", 5000), ("protein_g", 1000),
                          ("carbs_g", 1000), ("fat_g", 1000)):
            v = _wb_num(args, field, 0, hi, required=False)
            if v is not None:
                if field == "grams" and v <= 0:
                    raise ValueError("grams must be more than 0")
                changes[field] = v
        meal = _wb_meal(args, None, day, mirror.local_now(), default=False)
        if meal:
            changes["meal"] = meal
        scale = _wb_num(args, "scale", 0.05, 20, required=False)
        if not changes and scale is None:
            raise ValueError("say what to change: scale, grams, kcal, "
                             "protein_g, carbs_g, fat_g or meal")
        if scale is not None and any(k != "meal" for k in changes):
            raise ValueError("give scale or new numbers, not both")
        kind = "edit"
        payload = {"entry_id": entry_id, "day": day, "scale": scale,
                   "set": changes}
        extra = {"entry_id": entry_id, "day": day}
    elif name == "delete_entry":
        entry_id, day = _wb_entry(mirror, args)
        kind = "delete"
        payload = {"entry_id": entry_id, "day": day}
        extra = {"entry_id": entry_id, "day": day}
    elif name == "log_water":
        day, _ = _wb_day(mirror, args)
        if args.get("ml") is not None:
            ml = _wb_num(args, "ml", 1, 5000)
        elif args.get("oz") is not None:
            ml = round(_wb_num(args, "oz", 0.03, 170) * 29.5735, 1)
        else:
            raise ValueError("give ml or oz")
        kind = "water"
        payload = {"day": day, "time": _wb_time(args), "ml": ml}
        extra = {"day": day, "ml": ml}
    elif name == "log_weight":
        day, _ = _wb_day(mirror, args)
        if args.get("kg") is not None:
            kg = _wb_num(args, "kg", 20, 400)
        elif args.get("lb") is not None:
            kg = round(_wb_num(args, "lb", 44, 882) * 0.45359237, 3)
        else:
            raise ValueError("give kg or lb")
        kind = "weight"
        payload = {"day": day, "time": _wb_time(args), "kg": kg}
        extra = {"day": day, "kg": kg}
        fat = _wb_num(args, "body_fat_percent", 1, 75, required=False)
        if fat is not None:
            if "bodyfat" not in (mirror.device().get("kinds") or []):
                raise ValueError(WB_UPDATE)
            payload["body_fat_percent"] = fat
            extra["body_fat_percent"] = fat
    elif name == "log_workout":
        day, _ = _wb_day(mirror, args)
        activity = str(args.get("activity") or "").strip().lower()
        if activity not in ACTIVITIES:
            raise ValueError("activity must be one of: " + ", ".join(ACTIVITIES))
        minutes = _wb_num(args, "minutes", 1, 1440)
        kind = "workout"
        payload = {"day": day, "time": _wb_time(args), "activity": activity,
                   "minutes": minutes,
                   "kcal": _wb_num(args, "kcal", 0, 10000, required=False),
                   "name": " ".join(str(args.get("name") or "").split())[:60] or None}
        extra = {"day": day, "activity": activity, "minutes": minutes}
    elif name == "log_strength":
        day, _ = _wb_day(mirror, args)
        raw = args.get("exercises")
        if not isinstance(raw, list) or not raw:
            raise ValueError("exercises must be a list with at least one exercise")
        if len(raw) > 30:
            raise ValueError("at most 30 exercises per session")
        exercises, total = [], 0
        for ex in raw:
            if not isinstance(ex, dict):
                raise ValueError("each exercise must be an object")
            title = " ".join(str(ex.get("name") or "").split())[:80]
            if not title:
                raise ValueError("every exercise needs a name")
            sets = ex.get("sets")
            if not isinstance(sets, list) or not sets:
                raise ValueError(f"{title} needs at least one set")
            if len(sets) > 30:
                raise ValueError("at most 30 sets per exercise")
            clean = []
            for st in sets:
                if not isinstance(st, dict):
                    raise ValueError("each set must be an object")
                reps = _wb_num(st, "reps", 1, 500)
                if reps != int(reps):
                    raise ValueError("reps must be a whole number")
                if st.get("weight_lb") is not None:
                    lb = _wb_num(st, "weight_lb", 0, 2000)
                elif st.get("weight_kg") is not None:
                    lb = round(_wb_num(st, "weight_kg", 0, 900) / 0.45359237, 2)
                else:
                    lb = 0.0        # bodyweight
                entry = {"reps": int(reps), "weight_lb": lb}
                skind = st.get("kind")
                if skind is not None:
                    if skind not in SET_KINDS:
                        raise ValueError("a set's kind must be work, warmup, "
                                         "failure or drop")
                    if skind != "work":
                        # Older phones store every set as work. Refusing
                        # beats counting a warm-up as a record there.
                        if "setkind" not in (mirror.device().get("kinds") or []):
                            raise ValueError(WB_UPDATE)
                        entry["kind"] = skind
                clean.append(entry)
            total += len(clean)
            exercises.append({"name": title, "sets": clean})
        kind = "strength"
        payload = {"day": day, "time": _wb_time(args),
                   "name": " ".join(str(args.get("name") or "").split())[:60] or None,
                   "minutes": _wb_num(args, "minutes", 1, 600, required=False),
                   "exercises": exercises}
        extra = {"day": day, "exercises": len(exercises), "sets": total}
    elif name == "log_sleep":
        start, bedtime = _wb_clock(mirror, args.get("bedtime"), "bedtime")
        end, wake = _wb_clock(mirror, args.get("wake"), "wake")
        now = mirror.local_now()
        minutes = (end - start).total_seconds() / 60
        if minutes < 20 or minutes > 18 * 60:
            raise ValueError("wake must be 20 minutes to 18 hours after bedtime")
        if end > now + timedelta(minutes=5):
            raise ValueError("wake cannot be in the future (it is "
                             f"{now.strftime('%Y-%m-%d %H:%M')} on the phone)")
        if start < now - timedelta(days=WB_DAYS_BACK):
            raise ValueError(f"bedtime cannot be more than {WB_DAYS_BACK} days back")
        for night in mirror.sleep(WB_DAYS_BACK + 2):
            try:
                a = datetime.strptime(night["bedtime"], "%Y-%m-%d %H:%M")
                b = datetime.strptime(night["wake"], "%Y-%m-%d %H:%M")
            except (KeyError, ValueError):
                continue
            if start < b and end > a:
                raise ValueError("That time already has sleep recorded "
                                 f"({night['bedtime']} to {night['wake']}). "
                                 "Nothing was added.")
        kind = "sleep"
        payload = {"bedtime": bedtime, "wake": wake}
        extra = {"bedtime": bedtime, "wake": wake,
                 "hours": round(minutes / 60, 2)}
    elif name == "log_symptom":
        day, _ = _wb_day(mirror, args)
        symptom = str(args.get("symptom") or "").strip().lower().replace(" ", "_")
        if symptom not in SYMPTOMS:
            raise ValueError("symptom must be one of: " + ", ".join(SYMPTOMS))
        severity = str(args.get("severity") or "present").strip().lower()
        if severity not in SEVERITIES:
            raise ValueError("severity must be present, mild, moderate or severe")
        kind = "symptom"
        payload = {"day": day, "time": _wb_time(args), "symptom": symptom,
                   "severity": severity,
                   "minutes": _wb_num(args, "minutes", 0, 1440, required=False)}
        extra = {"day": day, "symptom": symptom, "severity": severity}
    elif name == "log_mood":
        day, _ = _wb_day(mirror, args)
        feeling = str(args.get("feeling") or "").strip().lower()
        if feeling not in MOOD_FEELINGS:
            raise ValueError("feeling must be one of: " + ", ".join(MOOD_FEELINGS))
        mood_kind = str(args.get("kind") or "momentary").strip().lower()
        if mood_kind not in ("momentary", "daily"):
            raise ValueError("kind must be momentary or daily")
        kind = "mood"
        payload = {"day": day, "time": _wb_time(args), "kind": mood_kind,
                   "feeling": feeling, "valence": MOOD_FEELINGS[feeling],
                   "labels": _wb_choices(args.get("labels"), MOOD_LABELS, "labels", 6),
                   "about": _wb_choices(args.get("about"), MOOD_ABOUT, "about", 4)}
        extra = {"day": day, "feeling": feeling}
    elif name == "set_targets":
        changes = {}
        for field, (lo, hi) in TARGET_BOUNDS.items():
            v = _wb_num(args, field, lo, hi, required=False)
            if v is not None:
                changes[field] = v
        if not changes:
            raise ValueError("give at least one target: "
                             + ", ".join(TARGET_BOUNDS))
        kind = "targets"
        payload = {"set": changes}
        extra = {"set": changes}
    else:
        raise ValueError(f"unknown tool: {name}")

    q = mirror.queue_write(kind, payload, client, key)
    if q["duplicate"]:
        return {"status": "duplicate", "id": q["id"], "state": q["state"],
                "note": "The same entry was sent a moment ago, so it was "
                        "not added twice. To add it again on purpose, pass "
                        "a new idempotency_key."}
    if on_queued:
        on_queued(kind, q["id"])
    out = {"status": "queued", "id": q["id"], **extra, "delivery": WB_DELIVERY}
    if kind not in NO_UNDO_KINDS:
        out["undo"] = "undo_entry with this id"
    return out


def read_targets(mirror):
    """The targets the phone last reported (it sends them with /device)."""
    dev = mirror.device()
    targets = dev.get("targets") or {}
    if not targets:
        return {"targets": None,
                "note": "The phone has not shared its targets yet. They "
                        "arrive the next time a current version of Marrow "
                        "is opened."}
    return {"targets": targets,
            "as_of": datetime.fromtimestamp(dev.get("updated") or 0, timezone.utc)
                             .strftime("%Y-%m-%dT%H:%M:%SZ")}


SUMMARY_KEYS = ["step_count", "active_energy_burned", "apple_exercise_time",
                "resting_heart_rate", "heart_rate_variability_s_d_n_n",
                "body_mass", "dietary_energy_consumed"]


# ---- the app's statistics, ported (Core/Derive.swift, derive.py) ----
# A day is scored against its OWN trailing history, the day itself
# excluded, so a value is always read against this person's normal and an
# AI says about a day what the Home page says.

def _shift(day, delta):
    return (date.fromisoformat(day) + timedelta(days=delta)).isoformat()


def rolling(series, window, min_n=5):
    """{day: (mean, sd, n)} over the preceding `window` days."""
    days = sorted(series)
    out = {}
    for i, d in enumerate(days):
        lo = _shift(d, -window)
        hist = [series[x] for x in days[:i] if x >= lo]
        if len(hist) >= min_n:
            sd = statistics.pstdev(hist) if len(hist) > 1 else 0.0
            out[d] = (statistics.fmean(hist), sd, len(hist))
    return out


def zscores(series, window=60):
    return {d: (series[d] - m) / sd
            for d, (m, sd, _n) in rolling(series, window).items() if sd > 0}


def trend_per_month(series, days_back=90):
    """Least-squares slope over the last `days_back` days with data, per
    month. None under 10 points."""
    days = sorted(series)[-days_back:]
    if len(days) < 10:
        return None
    x0 = date.fromisoformat(days[0])
    xs = [(date.fromisoformat(d) - x0).days for d in days]
    ys = [series[d] for d in days]
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    den = sum((x - mx) ** 2 for x in xs)
    if not den:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den * 30


def z_context(z):
    """The app's words for a z (Display.zContext)."""
    if z is None:
        return "building your baseline"
    if z < -1.5:
        return "well below your normal"
    if z < -0.7:
        return "below your normal"
    if z > 1.5:
        return "well above your normal"
    if z > 0.7:
        return "above your normal"
    return "normal for you"


def _r(v, nd=1):
    return None if v is None else round(v, nd)


def _direction(trend, sd, mean):
    """A slope smaller than half the day-to-day spread (or 1% of the
    level when the spread is zero) is flat, not a trend."""
    if trend is None:
        return "unknown"
    noise = sd * 0.5 if sd else abs(mean or 0) * 0.01
    if abs(trend) <= noise:
        return "flat"
    return "up" if trend > 0 else "down"


# The metrics the model-ready reads cover by default, with the plain
# name an AI should use. sleep_hours is the night rule, not a record type.
BRIEF_KEYS = [
    ("step_count", "steps"),
    ("active_energy_burned", "active calories"),
    ("apple_exercise_time", "exercise minutes"),
    ("resting_heart_rate", "resting heart rate"),
    ("heart_rate_variability_s_d_n_n", "heart rate variability"),
    ("body_mass", "body weight"),
    ("dietary_energy_consumed", "calories eaten"),
    ("dietary_protein", "protein eaten"),
    ("dietary_water", "water"),
    ("sleep_hours", "hours asleep"),
]
BRIEF_NAMES = dict(BRIEF_KEYS)


def _plain(key):
    return BRIEF_NAMES.get(key) or key.replace("_", " ")


HOW_TO_READ = ("z is how far a value sits from this user's own trailing "
               "window, in standard deviations of their day-to-day spread, "
               "the day itself excluded. The phrases are the app's: beyond "
               "0.7 is above or below their normal, beyond 1.5 well above or "
               "well below. direction compares the 90-day slope with that "
               "spread. None means too little history yet.")


def parse_day(s, key="day"):
    s = str(s or "")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        raise ValueError(f"{key} must be a date like 2026-10-22")
    try:
        return date.fromisoformat(s).isoformat()
    except ValueError:
        raise ValueError(f"{key} is not a real date")


def day_arg(mirror, args, key="day"):
    v = args.get(key)
    if v in (None, ""):
        return mirror.today()
    day = parse_day(v, key)
    if day > mirror.today():
        raise ValueError(f"{key} is in the future on the phone's calendar")
    return day


def clamp_int(args, key, default, lo, hi):
    try:
        v = int(args.get(key, default))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(v, hi))


def metric_series(mirror, key, floor, ceil):
    """{day: value} over floor..ceil, or None when the mirror has never
    seen that metric. sleep_hours is a night per wake day."""
    if key == "sleep_hours":
        if not mirror.resolve("sleep_analysis"):
            return None
        return {n["night"]: n["asleep_hours"]
                for n in mirror.sleep(0, floor=floor, ceil=ceil)}
    t = mirror.resolve(key)
    if not t:
        return None
    return {d: v for d, v in mirror.daily(t, 0, floor=floor, ceil=ceil)}


def metric_unit(mirror, key):
    if key == "sleep_hours":
        return "h"
    t = mirror.resolve(key)
    if not t:
        return ""
    row = mirror.conn.execute(
        "SELECT unit FROM records WHERE type=? AND unit IS NOT NULL "
        "AND unit!='' LIMIT 1", (t,)).fetchone()
    return row[0] if row else ""


def baseline_of(mirror, key, series, day, window):
    days = sorted(series)
    latest = days[-1]
    base = rolling(series, window).get(latest)
    z = (series[latest] - base[0]) / base[1] if base and base[1] > 0 else None
    recent = [series[d] for d in days if d > _shift(latest, -7)]
    trend = trend_per_month({d: series[d] for d in days if d > _shift(day, -90)})
    return {"key": key, "name": BRIEF_NAMES.get(key, key.replace("_", " ")),
            "unit": metric_unit(mirror, key),
            "latest_day": latest, "latest": _r(series[latest], 2),
            "days_ago": (date.fromisoformat(day) - date.fromisoformat(latest)).days,
            "baseline": ({"mean": _r(base[0], 2), "sd": _r(base[1], 2), "n": base[2]}
                         if base else None),
            "z": _r(z, 2), "context": z_context(z),
            "mean_7": _r(statistics.fmean(recent), 2),
            "trend_per_month": _r(trend, 2),
            "direction": _direction(trend, base[1] if base else 0,
                                    base[0] if base else series[latest])}


def baselines(mirror, args):
    day = day_arg(mirror, args)
    window = clamp_int(args, "window", 60, 7, 90)
    keys = args.get("metrics")
    if keys in (None, "", []):
        keys = [k for k, _ in BRIEF_KEYS]
    if not isinstance(keys, list) or not all(isinstance(k, str) for k in keys):
        raise ValueError("metrics must be a list of metric keys (see list_metrics)")
    floor = _shift(day, -max(window, 90) - 1)
    out, missing = [], []
    for key in keys[:40]:
        series = metric_series(mirror, key, floor, day)
        if not series:
            missing.append(key)
            continue
        out.append(baseline_of(mirror, key, series, day, window))
    return {"day": day, "window_days": window, "metrics": out,
            "missing": missing, "how_to_read": HOW_TO_READ}


def _bed_minutes(stamp):
    """Bedtime as minutes after 18:00, so a night that starts after
    midnight averages with one that starts before it."""
    h, m = int(stamp[11:13]), int(stamp[14:16])
    return (h * 60 + m - 18 * 60) % 1440


def _per_week(n, days):
    return _r(n * 7 / days, 1) if days else None


def what_changed(mirror, args):
    day = day_arg(mirror, args)
    days = clamp_int(args, "days", 7, 1, 30)
    compare = clamp_int(args, "compare_days", 28, 7, 90)
    r_from, r_to = _shift(day, -(days - 1)), day
    p_from, p_to = _shift(r_from, -compare), _shift(r_from, -1)
    metrics, notable = [], []
    for key, name in BRIEF_KEYS:
        series = metric_series(mirror, key, p_from, r_to)
        if not series:
            continue
        recent = [v for d, v in series.items() if d >= r_from]
        prior = [v for d, v in series.items() if d < r_from]
        unit = metric_unit(mirror, key)
        e = {"key": key, "name": name, "unit": unit,
             "recent_days": len(recent), "prior_days": len(prior)}
        if len(recent) < 2 or len(prior) < 5:
            e["change"] = "not enough data"
            metrics.append(e)
            continue
        rm, pm = statistics.fmean(recent), statistics.fmean(prior)
        sd = statistics.pstdev(prior)
        delta = rm - pm
        pct = delta / pm * 100 if pm else None
        z = delta / sd if sd > 0 else None
        if z is not None:
            is_notable = abs(z) >= 0.7
        else:
            is_notable = delta != 0
        direction = "flat" if (z is not None and abs(z) < 0.25) or delta == 0 \
            else ("up" if delta > 0 else "down")
        e.update({"recent_mean": _r(rm, 2), "prior_mean": _r(pm, 2),
                  "change": _r(delta, 2), "change_pct": _r(pct, 1), "z": _r(z, 2),
                  "direction": direction, "notable": is_notable})
        metrics.append(e)
        if is_notable:
            u = f" {unit}" if unit and unit != "count" else ""
            how = f"{abs(pct):.0f}%" if pct is not None else f"{abs(delta):g}{u}"
            notable.append(f"{name.capitalize()} {direction} {how}: "
                           f"{_r(rm, 1):g} vs {_r(pm, 1):g}{u} before")
    metrics.sort(key=lambda e: (not e.get("notable"), -abs(e.get("z") or 0)))

    def diary_days(lo, hi):
        rows = mirror.conn.execute(
            "SELECT day, SUM(kcal), SUM(protein) FROM diary WHERE day>=? AND day<=? "
            "GROUP BY day", (lo, hi)).fetchall()
        return {"days_logged": len(rows),
                "kcal_per_logged_day": _r(statistics.fmean([r[1] or 0 for r in rows]))
                if rows else None,
                "protein_g_per_logged_day": _r(statistics.fmean([r[2] or 0 for r in rows]))
                if rows else None}

    def lifting(lo, hi, span):
        sessions, sets, volume = mirror.conn.execute(
            "SELECT COUNT(DISTINCT session_id), COUNT(*), "
            "COALESCE(SUM(COALESCE(weight,0)*COALESCE(reps,0)),0) FROM strength_sets "
            "WHERE kind!='warmup' AND day>=? AND day<=?", (lo, hi)).fetchone()
        workouts = mirror.conn.execute(
            "SELECT COUNT(*) FROM workouts WHERE day>=? AND day<=?", (lo, hi)).fetchone()[0]
        return {"lifting_sessions_per_week": _per_week(sessions, span),
                "working_sets_per_week": _per_week(sets, span),
                "volume_per_week": _per_week(volume, span),
                "workouts_per_week": _per_week(workouts, span)}

    nights = mirror.sleep(0, floor=p_from, ceil=r_to)
    rn = [n for n in nights if n["night"] >= r_from]
    pn = [n for n in nights if n["night"] < r_from]
    sleep = {"recent_nights": len(rn), "prior_nights": len(pn)}
    if len(rn) >= 2 and len(pn) >= 5:
        rh = statistics.fmean(n["asleep_hours"] for n in rn)
        ph = statistics.fmean(n["asleep_hours"] for n in pn)
        shift = (statistics.fmean(_bed_minutes(n["bedtime"]) for n in rn)
                 - statistics.fmean(_bed_minutes(n["bedtime"]) for n in pn))
        sleep.update({"recent_hours": _r(rh, 2), "prior_hours": _r(ph, 2),
                      "change_hours": _r(rh - ph, 2),
                      "bedtime_shift_min": round(shift)})
        if abs(shift) >= 30:
            notable.append(f"Bedtime is {abs(round(shift))} min "
                           f"{'later' if shift > 0 else 'earlier'} than before")
        if abs(rh - ph) >= 0.5:
            notable.append(f"Sleep {'up' if rh > ph else 'down'} "
                           f"{abs(rh - ph):.1f} h a night: {rh:.1f} vs {ph:.1f} h before")

    new, stopped = [], []
    for t, _u, _n, lo, hi in mirror.types():
        key = short_key(t)
        rec = mirror.conn.execute(
            "SELECT COUNT(DISTINCT day) FROM records WHERE type=? AND day>=? AND day<=?",
            (t, r_from, r_to)).fetchone()[0]
        pri = mirror.conn.execute(
            "SELECT COUNT(DISTINCT day) FROM records WHERE type=? AND day>=? AND day<=?",
            (t, p_from, p_to)).fetchone()[0]
        if rec and not pri and lo >= r_from:
            new.append(key)
        elif pri >= compare * 0.5 and not rec:
            stopped.append(key)
    for key in stopped[:5]:
        notable.append(f"{key} stopped arriving (none in the last {days} days)")
    return {"recent": {"from": r_from, "to": r_to, "days": days},
            "prior": {"from": p_from, "to": p_to, "days": compare},
            "metrics": metrics,
            "food": {"recent": diary_days(r_from, r_to), "prior": diary_days(p_from, p_to)},
            "training": {"recent": lifting(r_from, r_to, days),
                         "prior": lifting(p_from, p_to, compare)},
            "sleep": sleep, "streams": {"new": new, "stopped": stopped},
            "notable": notable, "how_to_read": HOW_TO_READ}


def stale_streams(mirror, today, floor60):
    """Metrics that arrive most days but have not for three: the phone has
    not synced, or the source stopped. Only these make an absence suspect."""
    out = []
    cutoff = _shift(today, -2)
    for t, _u, _n, lo, hi in mirror.types():
        if t == "HKCategoryTypeIdentifierSleepAnalysis" or hi >= cutoff:
            continue
        span = (date.fromisoformat(today) - date.fromisoformat(max(lo, floor60))).days + 1
        cov = mirror.conn.execute(
            "SELECT COUNT(DISTINCT day) FROM records WHERE type=? AND day>=?",
            (t, floor60)).fetchone()[0]
        if span >= 7 and cov >= 0.7 * span:
            out.append((short_key(t), hi))
    return out


def data_quality(mirror, args):
    days = clamp_int(args, "days", 14, 1, 90)
    today = mirror.today()
    floor = _shift(today, -(days - 1))
    floor60 = _shift(today, -59)
    stale = dict(stale_streams(mirror, today, floor60))
    metrics, issues = [], []
    for t, unit, n, lo, hi in mirror.types():
        if t == "HKCategoryTypeIdentifierSleepAnalysis":
            continue
        key = short_key(t)
        cov = mirror.conn.execute(
            "SELECT COUNT(DISTINCT day) FROM records WHERE type=? AND day>=?",
            (t, floor)).fetchone()[0]
        cov60 = mirror.conn.execute(
            "SELECT COUNT(DISTINCT day) FROM records WHERE type=? AND day>=?",
            (t, floor60)).fetchone()[0]
        span60 = (date.fromisoformat(today) - date.fromisoformat(max(lo, floor60))).days + 1
        cadence = "daily" if span60 >= 7 and cov60 >= 0.7 * span60 else "occasional"
        sources = [s for (s,) in mirror.conn.execute(
            "SELECT source_name FROM records WHERE type=? AND day>=? "
            "GROUP BY source_name ORDER BY COUNT(*) DESC LIMIT 3", (t, floor60))]
        metrics.append({"key": key, "unit": unit or "", "first_day": lo, "latest_day": hi,
                        "days_with_data": cov, "coverage_pct": round(cov * 100 / days),
                        "cadence": cadence, "sources": sources,
                        "stale": key in stale})
        if key in stale:
            issues.append(f"{_plain(key).capitalize()} has not synced since {hi}")
        if len(sources) > 1 and any(h in t for h in SUM_HINTS):
            issues.append(f"{key} comes from {len(sources)} sources, the richest "
                          "source wins each day (no double counting)")
    d_days, d_last, d_n = mirror.conn.execute(
        "SELECT COUNT(DISTINCT day), MAX(day), COUNT(*) FROM diary WHERE day>=?",
        (floor,)).fetchone()
    d_last_any = mirror.conn.execute("SELECT MAX(day) FROM diary").fetchone()[0]
    diary = {"days_logged": d_days, "entries": d_n, "latest_day": d_last or d_last_any,
             "coverage_pct": round(d_days * 100 / days)}
    if d_last_any and d_days < days * 0.5:
        issues.append(f"Food was logged on {d_days} of the last {days} days: "
                      "calorie and protein figures cover only those days")
    nights = mirror.sleep(0, floor=floor, ceil=today)
    last_night = mirror.conn.execute(
        "SELECT MAX(day) FROM records WHERE type='HKCategoryTypeIdentifierSleepAnalysis'"
    ).fetchone()[0]
    sleep = {"nights": len(nights), "latest_night": nights[-1]["night"] if nights else None,
             "latest_record_day": last_night, "coverage_pct": round(len(nights) * 100 / days)}
    if last_night and not nights:
        issues.append(f"No sleep in the last {days} nights (last recorded {last_night})")
    if not last_night:
        issues.append("Sleep is not synced to this mirror")
    s_n, s_last = mirror.conn.execute(
        "SELECT COUNT(DISTINCT session_id), MAX(day) FROM strength_sets WHERE day>=?",
        (floor,)).fetchone()
    s_last_any = mirror.conn.execute("SELECT MAX(day) FROM strength_sets").fetchone()[0]
    w_n = mirror.conn.execute("SELECT COUNT(*) FROM workouts WHERE day>=?",
                              (floor,)).fetchone()[0]
    training = {"lifting_sessions": s_n, "latest_lifting_day": s_last or s_last_any,
                "workouts": w_n}
    dev = mirror.device()
    st = mirror.status()
    return {"today": today, "as_of": mirror.local_now().strftime("%Y-%m-%d %H:%M"),
            "last_sync": st.get("last_ingest"), "window_days": days,
            "records": st.get("records"), "first_day": st.get("first_day"),
            "metrics": metrics, "diary": diary, "sleep": sleep, "training": training,
            "write_back": {"enabled": bool(dev.get("write_enabled")),
                           "kinds": dev.get("kinds") or [],
                           "app_version": dev.get("app_version") or "",
                           "timezone_offset_hours": (dev.get("tz_offset_seconds") or 0) / 3600},
            "issues": issues}


def daily_brief(mirror, args):
    day = day_arg(mirror, args)
    today = mirror.today()
    window = clamp_int(args, "window", 60, 7, 90)
    floor = _shift(day, -window - 1)
    dev = mirror.device()
    targets = dev.get("targets") or {}
    metrics, flags, value = [], [], {}
    for key, name in BRIEF_KEYS:
        if key == "sleep_hours":
            continue
        series = metric_series(mirror, key, floor, day)
        if series is None:
            continue
        unit = metric_unit(mirror, key)
        v = series.get(day)
        e = {"key": key, "name": name, "unit": unit, "value": _r(v, 2)}
        if v is None:
            latest = max(series) if series else None
            if latest:
                e.update({"latest_day": latest, "latest": _r(series[latest], 2)})
            e["context"] = "nothing for this day"
        else:
            base = rolling(series, window).get(day)
            z = (v - base[0]) / base[1] if base and base[1] > 0 else None
            e.update({"z": _r(z, 2), "context": z_context(z),
                      "usual": _r(base[0], 2) if base else None})
            if z is not None and abs(z) > 1.5:
                u = f" {unit}" if unit and unit != "count" else ""
                flags.append(f"{name.capitalize()}: {z_context(z)} "
                             f"({_r(v, 1):g} vs a usual {_r(base[0], 1):g}{u})")
        value[key] = v
        metrics.append(e)

    rows = mirror.conn.execute(
        "SELECT meal, name, kcal, protein, carbs, fat FROM diary WHERE day=? "
        "ORDER BY logged_at", (day,)).fetchall()
    meals = []
    for meal, _n, _k, _p, _c, _f in rows:
        if meal and meal not in meals:
            meals.append(meal)
    food = {"entries": len(rows),
            "kcal": _r(sum(r[2] or 0 for r in rows)) if rows else None,
            "protein_g": _r(sum(r[3] or 0 for r in rows)) if rows else None,
            "carbs_g": _r(sum(r[4] or 0 for r in rows)) if rows else None,
            "fat_g": _r(sum(r[5] or 0 for r in rows)) if rows else None,
            "meals": meals, "source": "diary" if rows else None}
    if not rows and value.get("dietary_energy_consumed") is not None:
        food.update({"kcal": _r(value["dietary_energy_consumed"]),
                     "protein_g": _r(value.get("dietary_protein")),
                     "source": "apple_health"})
    if food["kcal"] is None:
        if day == today:
            if mirror.local_now().hour >= 11:
                flags.append("No food logged yet today")
        else:
            flags.append("No food was logged that day")

    water_ml = value.get("dietary_water")
    water = {"ml": _r(water_ml)}
    if targets.get("water_ml"):
        water["target_ml"] = targets["water_ml"]
        water["remaining_ml"] = _r(max(targets["water_ml"] - (water_ml or 0), 0))

    night = mirror.sleep(0, floor=day, ceil=day)
    sleep = None
    if night:
        n = night[0]
        series = metric_series(mirror, "sleep_hours", floor, day) or {}
        base = rolling(series, window).get(day)
        z = (n["asleep_hours"] - base[0]) / base[1] if base and base[1] > 0 else None
        sleep = {**n, "z": _r(z, 2), "context": z_context(z),
                 "usual_hours": _r(base[0], 2) if base else None}
        if z is not None and abs(z) > 1.5:
            flags.append(f"Sleep: {z_context(z)} ({n['asleep_hours']:.1f} h "
                         f"vs a usual {base[0]:.1f} h)")
    elif mirror.resolve("sleep_analysis"):
        flags.append(f"No sleep recorded for the night ending {day}")

    lifts = [{"session": name or "Workout", "sets": sets,
              "working_sets": work, "volume": _r(vol)}
             for name, sets, work, vol in mirror.conn.execute(
                 "SELECT session_name, COUNT(*), "
                 "SUM(CASE WHEN kind!='warmup' THEN 1 ELSE 0 END), "
                 "SUM(CASE WHEN kind!='warmup' THEN COALESCE(weight,0)*COALESCE(reps,0) "
                 "ELSE 0 END) FROM strength_sets WHERE day=? GROUP BY session_id "
                 "ORDER BY MIN(done_at)", (day,))]
    workouts = [{"activity": a.replace("HKWorkoutActivityType", ""),
                 "start": s[:16], "duration": d, "unit": u}
                for a, s, _e, d, u, _src in mirror.workouts(0, floor=day, ceil=day)]
    training = {"lifting": lifts, "workouts": workouts,
                "rest_day": not lifts and not workouts}

    sm = mirror.symptoms_and_mood(0, floor=day, ceil=day)
    moods = [e for e in sm["entries"] if e["kind"] != "symptom"]
    symptoms = [e for e in sm["entries"] if e["kind"] == "symptom"]
    mood = {"entries": len(moods),
            "latest_feeling": moods[-1]["feeling"] if moods else None,
            "symptoms": [f"{e['name']} ({e['severity']})" for e in symptoms]}
    if symptoms:
        flags.append("Symptoms logged: " + ", ".join(mood["symptoms"][:4]))

    progress = None
    if targets:
        so_far = {"kcal": food["kcal"], "protein_g": food["protein_g"],
                  "carbs_g": food["carbs_g"], "fat_g": food["fat_g"],
                  "water_ml": water_ml, "steps": value.get("step_count"),
                  "exercise_minutes": value.get("apple_exercise_time"),
                  "sleep_hours": sleep["asleep_hours"] if sleep else None}
        progress = {}
        for k, t in targets.items():
            if k in so_far and isinstance(t, (int, float)):
                v = so_far[k]
                progress[k] = {"target": t, "so_far": _r(v),
                               "remaining": _r(max(t - v, 0)) if v is not None else t,
                               "pct": round(v * 100 / t) if v is not None and t else None}
    stale = stale_streams(mirror, today, _shift(today, -59))
    for key, last in stale[:3]:
        flags.append(f"{_plain(key).capitalize()} has not synced since {last}")
    st = mirror.status()
    return {"day": day, "is_today": day == today,
            "as_of": mirror.local_now().strftime("%Y-%m-%d %H:%M") + " phone time",
            "metrics": metrics, "food": food, "water": water, "sleep": sleep,
            "training": training, "mood": mood,
            "targets": ({"mode": targets.get("mode"), "progress": progress}
                        if targets else None),
            "flags": flags,
            "data": {"last_sync": st.get("last_ingest"),
                     "stale": [k for k, _ in stale],
                     "scored_against_days": window},
            "how_to_read": HOW_TO_READ}


def call_tool(mirror, name, args, client=None, on_queued=None):
    if name in WRITE_TOOL_NAMES or name == "pending_entries":
        return call_write_tool(mirror, name, args, client, on_queued)
    if name == "targets":
        return read_targets(mirror)
    if name == "daily_brief":
        return daily_brief(mirror, args)
    if name == "baselines":
        return baselines(mirror, args)
    if name == "what_changed":
        return what_changed(mirror, args)
    if name == "data_quality":
        return data_quality(mirror, args)

    def clamp(key, default, cap):
        try:
            v = int(args.get(key, default))
        except (TypeError, ValueError):
            v = default
        return max(1, min(v, cap))

    def window(default, cap):
        """(days, floor, ceil): from..to when given, else days back."""
        lo, hi = args.get("from"), args.get("to")
        if lo in (None, "") and hi in (None, ""):
            return clamp("days", default, cap), None, None
        hi = parse_day(hi, "to") if hi not in (None, "") else mirror.today()
        lo = parse_day(lo, "from") if lo not in (None, "") else _shift(hi, -default)
        if lo > hi:
            raise ValueError("from must not be after to")
        if (date.fromisoformat(hi) - date.fromisoformat(lo)).days > cap:
            raise ValueError(f"from and to may be at most {cap} days apart")
        return cap, lo, hi

    if name == "list_metrics":
        return [{"key": short_key(t), "unit": u or "", "records": n,
                 "first_day": lo, "latest_day": hi}
                for t, u, n, lo, hi in mirror.types()]
    if name == "health_summary":
        days, lo, hi = window(3, 30)
        out = {}
        for key in SUMMARY_KEYS:
            t = mirror.resolve(key)
            if t:
                series = mirror.daily(t, days, floor=lo, ceil=hi)
                if series:
                    out[key] = {d: round(v, 2) for d, v in series}
        nights = mirror.sleep(days, floor=lo, ceil=hi)
        if nights:
            out["sleep_hours"] = {n["night"]: n["asleep_hours"] for n in nights}
        return out
    if name == "sleep_log":
        days, lo, hi = window(7, 90)
        return mirror.sleep(days, floor=lo, ceil=hi)
    if name == "symptoms_and_mood":
        days, lo, hi = window(30, 400)
        return mirror.symptoms_and_mood(days, floor=lo, ceil=hi)
    if name in ("metric_daily", "metric_samples"):
        if args.get("metric") in (None, ""):
            raise ValueError("metric is required: pass a key from list_metrics")
        t = mirror.resolve(str(args["metric"]))
        if not t:
            raise ValueError(f"unknown metric: {args.get('metric')} "
                             "(see list_metrics)")
        if name == "metric_daily":
            days, lo, hi = window(30, 400)
            return {"metric": short_key(t),
                    "days": {d: round(v, 3)
                             for d, v in mirror.daily(t, days, floor=lo, ceil=hi)}}
        days, lo, hi = window(7, 400)
        before = args.get("before")
        before = str(before)[:40] if before not in (None, "") else None
        rows = mirror.samples(t, days, clamp("limit", 200, 1000),
                              floor=lo, ceil=hi, before=before)
        return [{"start": s, "end": e,
                 "value": num if num is not None else v, "source": src}
                for s, e, v, num, src in rows]
    if name == "workouts":
        days, lo, hi = window(30, 400)
        return [{"activity": a, "start": s, "end": e, "duration": d,
                 "unit": u, "source": src}
                for a, s, e, d, u, src in mirror.workouts(days, floor=lo, ceil=hi)]
    if name == "nutrition_log":
        days, lo, hi = window(7, 90)
        return mirror.nutrition(days, floor=lo, ceil=hi)
    if name == "strength_log":
        days, lo, hi = window(90, 400)
        return mirror.strength(days, floor=lo, ceil=hi)
    raise ValueError(f"unknown tool: {name}")


def pair_page(pairing):
    info = json.dumps(pairing)
    try:
        import segno
        import base64
        import io
        buf = io.BytesIO()
        segno.make(info).save(buf, kind="png", scale=8)
        img = base64.b64encode(buf.getvalue()).decode()
        qr = f'<img src="data:image/png;base64,{img}" alt="pairing QR">'
    except ImportError:
        qr = "<p>(install <code>segno</code> for a QR. Manual JSON below)</p>"
    return f"""<!doctype html><meta charset="utf-8">
<title>Pair Marrow</title>
<body style="font-family:-apple-system,sans-serif;max-width:32em;margin:3em auto">
<h2>Pair Marrow with this server</h2>
<p>On your iPhone: <b>Marrow → Export → Server MCP → Pair with a server</b>, then scan:</p>
{qr}
<p style="color:#666">Manual fallback, pairing JSON:</p>
<pre style="background:#f4f4f2;padding:1em;overflow:auto">{info}</pre>
</body>""".encode()


def make_handler(mirror, app_token, mcp_token, pairing):
    # One sqlite connection serves every thread: a push and a tool call must
    # not interleave their statements.
    lock = threading.RLock()

    class Handler(BaseHTTPRequestHandler):
        server_version = f"Marrow/{__version__}"
        protocol_version = "HTTP/1.1"
        timeout = 30          # a peer that stops sending frees the thread

        def log_message(self, *a):
            pass

        def _json(self, code, obj=None):
            body = json.dumps(obj).encode() if obj is not None else b""
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _app_authed(self):
            # ?token= exists for the pairing page, which is opened from a
            # link. Only reads accept it. Writes must carry the header.
            supplied = self.headers.get("X-HealthBridge-Token") or ""
            if not supplied and self.command == "GET":
                q = parse_qs(urlparse(self.path).query)
                supplied = (q.get("token") or [""])[0]
            return hmac.compare_digest(supplied, app_token)

        def _mcp_authed(self):
            return hmac.compare_digest(self.headers.get("Authorization", ""),
                                       f"Bearer {mcp_token}")

        def do_GET(self):
            path = urlparse(self.path).path.rstrip("/")
            if path in ("", "/health"):
                return self._json(200, {"server": "marrow", "ok": True})
            if not self._app_authed():
                return self._json(401, {"error": "bad or missing token"})
            if path == "/pair":
                body = pair_page(pairing)
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/status":
                with lock:
                    return self._json(200, mirror.status())
            if path == "/inbox":
                with lock:
                    return self._json(200, mirror.inbox_items())
            return self._json(404, {"error": "not found"})

        def do_POST(self):
            path = urlparse(self.path).path.rstrip("/")
            # Only a plain Content-Length body is read. A chunked request
            # or a malformed length is refused before anything is read.
            if self.headers.get("Transfer-Encoding"):
                self.close_connection = True
                return self._json(400, {"error": "chunked bodies are not accepted"})
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                self.close_connection = True
                return self._json(400, {"error": "bad length"})
            if length <= 0 or length > MAX_BODY:
                return self._json(411 if length <= 0 else 413,
                                  {"error": "bad length"})
            raw = self.rfile.read(length)

            if path == "/mcp":
                if not self._mcp_authed():
                    return self._json(401, {"error": "bad bearer token"})
                return self.mcp(raw)

            if not self._app_authed():
                return self._json(401, {"error": "bad or missing token"})
            if path == "/ping":
                return self._json(200, {"status": "ok", "server": "marrow"})
            if path == "/ingest":
                try:
                    payload = json.loads(raw)
                    assert isinstance(payload, dict)
                except Exception:
                    return self._json(400, {"error": "body must be JSON object"})
                try:
                    with lock:
                        return self._json(200, mirror.ingest(payload))
                except ValueError as exc:
                    # Malformed shape = client bug: 4xx so a broken payload
                    # can't wedge the app's outbox in an infinite retry.
                    return self._json(400, {"error": str(exc)})
            if path in ("/device", "/inbox/ack"):
                try:
                    body = json.loads(raw)
                    assert isinstance(body, dict)
                except Exception:
                    return self._json(400, {"error": "body must be JSON object"})
                if path == "/device":
                    with lock:
                        d = mirror.set_device(body)
                    # No push from a self-hosted server: the phone fetches
                    # the inbox whenever it wakes or opens.
                    return self._json(200, {"ok": True,
                                            "write_enabled": d["write_enabled"],
                                            "push": "unsupported"})
                with lock:
                    return self._json(200, mirror.inbox_ack(body.get("results")))
            return self._json(404, {"error": "not found"})

        def mcp(self, raw):
            try:
                msg = json.loads(raw)
            except Exception:
                return self._json(400, {"error": "not JSON"})
            method = msg.get("method", "")
            if "id" not in msg:
                return self._json(202)
            rid = msg["id"]
            params = msg.get("params") or {}
            try:
                if method == "initialize":
                    result = {"protocolVersion": PROTOCOL_VERSION,
                              "capabilities": CAPABILITIES,
                              "serverInfo": {"name": "marrow-selfhost",
                                             "title": "Marrow Health (self-hosted)",
                                             "version": __version__},
                              "instructions": INSTRUCTIONS}
                elif method == "ping":
                    result = {}
                elif method == "tools/list":
                    # The write tools exist once the phone has registered
                    # for write-back (today only Marrow Connect does), so a
                    # self-hosted server lists what actually works.
                    with lock:
                        writes = bool(mirror.device().get("write_enabled"))
                    result = {"tools": ANNOTATED_TOOLS if writes else ANNOTATED_READ_TOOLS}
                elif method == "tools/call":
                    if not isinstance(params, dict):
                        raise ValueError("params must be an object")
                    with lock:
                        payload = call_tool(mirror, params.get("name", ""),
                                            params.get("arguments") or {},
                                            client="Your AI")
                    result = tool_result(payload)
                else:
                    with lock:
                        result = mcp_extra(mirror, method, params)
                    if result is None:
                        return self._json(200, {"jsonrpc": "2.0", "id": rid,
                                                "error": {"code": -32601,
                                                          "message": f"no {method}"}})
            except RpcError as e:
                return self._json(200, {"jsonrpc": "2.0", "id": rid,
                                        "error": {"code": e.code, "message": e.message}})
            except Exception as e:
                # A tool's own ValueError is written for the AI. Anything
                # else stays generic (no paths or internals on the wire).
                text = (f"Error: {e}" if type(e) is ValueError
                        else "Error: the server could not complete that call")
                result = {"content": [{"type": "text", "text": text}],
                          "isError": True}
            return self._json(200, {"jsonrpc": "2.0", "id": rid,
                                    "result": result})

    return Handler


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--port", type=int, default=8800)
    ap.add_argument("--data", default="./data",
                    help="data directory (mirror + tokens)")
    ap.add_argument("--url", default=None,
                    help="advertised base URL for pairing "
                         "(default http://<lan-ip>:<port>)")
    args = ap.parse_args()

    data = Path(args.data).resolve()
    data.mkdir(parents=True, exist_ok=True)
    app_token = load_token(data / ".app-token")
    mcp_token = load_token(data / ".mcp-token")
    mirror = Mirror(data)
    base = args.url or f"http://{lan_ip()}:{args.port}"
    pairing = {"url": base, "token": app_token, "name": "Self-hosted server"}

    print(f"Marrow self-hosted server on 0.0.0.0:{args.port}")
    print(f"  pair:   {base}/pair?token={app_token}")
    print(f"  agents: {base}/mcp   (Authorization: Bearer {mcp_token})")
    ThreadingHTTPServer(("0.0.0.0", args.port),
                        make_handler(mirror, app_token, mcp_token,
                                     pairing)).serve_forever()


if __name__ == "__main__":
    main()

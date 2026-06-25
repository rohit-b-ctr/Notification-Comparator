#!/usr/bin/env python3
"""
Notification Comparator — Flask Web UI
Run: python app.py
Then open: http://localhost:5050
"""

import json
import queue
import shutil
import subprocess
import threading
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

try:
    from flask import Flask, Response, jsonify, render_template_string, request
    import psycopg2
    import sshtunnel
    from deepdiff import DeepDiff
except ImportError:
    print("Missing dependencies. Run:")
    print("  pip install flask psycopg2-binary sshtunnel deepdiff")
    raise

# websocket-client is only needed for the Topic Compare feature (Kowl/Kafka UI).
# Keep it optional so the DB flow still runs if it's not installed.
try:
    import websocket  # websocket-client
except ImportError:
    websocket = None

# PyMuPDF is only needed for the ISD (PDF) golden-capture feature.
try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

# ─── CONFIG ──────────────────────────────────────────────────────────────────

import os

CONFIG_PATH = Path(__file__).parent / "config.json"

# Fields that are NEVER written to disk — must be entered in UI each session
SECRET_FIELDS = {"db_pass", "ssh_key"}

DEFAULT_CONFIG = {
    "ssh_host": "172.29.32.137",
    "ssh_port": 22,
    "ssh_user": "rohit_b_ctr_greyorange_com",
    "db_host":  "10.57.117.201",
    "db_port":  5432,
    "db_name":  "wms_notification",
    "db_user":  "postgres",
    "db_table": "subscriber_history",
    "subscriber_put":   None,
    "subscriber_pick":  None,
    "subscriber_audit": None,
    "subscriber_other": None,
    "poll_interval": 3,
    # ── Golden categorization ──
    "project": "",                       # current project — golden saved under golden/{project}/...
    # ── Topic Compare (Kowl / Kafka UI) ──
    "topic_host": "172.29.32.39:9003",   # baseline setup Kowl host:port
    "topic_host_b": "172.29.32.39:9003", # target setup Kowl host:port
    "topic_count": 50,                   # recent N messages to pull per topic
    "topics": [
        {"label": "PUT",  "topic": "stpfunction-sbscloud.put_information.events"},
        {"label": "SR",   "topic": "stpfunction-sbscloud.service-request-update.events"},
        {"label": "PICK", "topic": "stpfunction-sbscloud.order_information.events"},
    ],
}

# In-memory only — never persisted to disk
RUNTIME_SECRETS = {
    "db_pass": "",
    "ssh_key": "",
}

def parse_int(value, default=None):
    """int() that tolerates None/'' (blank form fields) and returns default instead of crashing."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def load_config():
    if CONFIG_PATH.exists():
        try:
            saved = json.loads(CONFIG_PATH.read_text())
            # strip any secrets that may have been saved by older versions
            for f in SECRET_FIELDS:
                saved.pop(f, None)
            # strip whitespace from all string values
            saved = {k: v.strip() if isinstance(v, str) else v for k, v in saved.items()}
            return {**DEFAULT_CONFIG, **saved}
        except Exception:
            pass
    return dict(DEFAULT_CONFIG)

def save_config(data):
    # Never write secrets to disk
    safe = {k: v for k, v in data.items() if k not in SECRET_FIELDS}
    CONFIG_PATH.write_text(json.dumps(safe, indent=2))

def get_cfg():
    """Return merged config — disk config + in-memory secrets."""
    cfg = load_config()
    cfg.update(RUNTIME_SECRETS)
    return cfg

def secrets_ready():
    return bool(RUNTIME_SECRETS.get("db_pass")) and bool(RUNTIME_SECRETS.get("ssh_key"))

SECRETS_PATH = Path(__file__).parent / ".secrets"

def load_saved_secrets():
    """Auto-load secrets from .secrets file on startup if it exists."""
    if SECRETS_PATH.exists():
        try:
            data = json.loads(SECRETS_PATH.read_text())
            if data.get("db_pass"):
                RUNTIME_SECRETS["db_pass"] = data["db_pass"]
            if data.get("ssh_key"):
                RUNTIME_SECRETS["ssh_key"] = data["ssh_key"]
            return True
        except Exception:
            pass
    return False

def save_secrets_to_disk(db_pass, ssh_key):
    SECRETS_PATH.write_text(json.dumps({"db_pass": db_pass, "ssh_key": ssh_key}))

def clear_saved_secrets():
    if SECRETS_PATH.exists():
        SECRETS_PATH.unlink()

# Auto-load secrets at startup
_secrets_auto_loaded = load_saved_secrets()

GOLDEN_DIR  = Path(__file__).parent / "golden"
REPORTS_DIR = Path(__file__).parent / "reports"
TOPIC_DIR   = Path(__file__).parent / "topic_baseline"   # legacy store (read-only fallback)
GOLDEN_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)
# Kowl baselines now live under golden/{project}/kowl/... — TOPIC_DIR is no longer created,
# only read as a fallback for any pre-migration baselines that might still exist.

app = Flask(__name__)

# ─── GLOBALS FOR WATCH MODE ───────────────────────────────────────────────────

watch_state = {
    "running": False,
    "results": [],
    "log_queue": queue.Queue(),
    "thread": None,
    "mode": "full",
}

# Full Run = live compare across ALL configured flows at once (time-bounded).
full_watch_state = {
    "running": False,
    "results": [],
    "log_queue": queue.Queue(),
    "thread": None,
    "mode": "full",
    "started_at": None,
}

capture_state = {
    "running": False,
    "seen":    set(),
    "saved":   {},
    "log_queue": queue.Queue(),
    "thread":  None,
}

# Live capture of Kowl topic messages -> kowl golden (mirrors DB live capture)
kowl_capture_state = {
    "running": False,
    "saved":   {},
    "log_queue": queue.Queue(),
    "thread":  None,
}

# ─── CORE LOGIC (shared with CLI) ────────────────────────────────────────────

def normalize(obj):
    if isinstance(obj, dict):
        return {k: normalize(v) for k, v in obj.items() if k != "@type"}
    elif isinstance(obj, list):
        if len(obj) == 2 and isinstance(obj[0], str) and obj[0].startswith("java.") and isinstance(obj[1], list):
            return [normalize(i) for i in obj[1]]
        return [normalize(i) for i in obj]
    return obj

IGNORE_FIELDS = {
    "id", "eventdata_id", "notification_id", "execution_id",
    "createdOn", "updatedOn", "receivedOn", "create_time",
    "externalServiceRequestId", "sr_parent", "sr_parentsIds",
}

def strip_dynamic(obj):
    if isinstance(obj, dict):
        return {k: strip_dynamic(v) for k, v in obj.items() if k not in IGNORE_FIELDS}
    elif isinstance(obj, list):
        return [strip_dynamic(i) for i in obj]
    return obj

def clean_payload(raw):
    data = json.loads(raw) if isinstance(raw, str) else raw
    return strip_dynamic(normalize(data))

def notif_key(payload):
    # Derive key from type, state, status — all from notification_data
    # Shape 1: { notification_data: { type, state, status }, ... }
    # Shape 2: flat { type, state, status }
    nd     = payload.get("notification_data") or {}

    def pick(field, default="UNKNOWN"):
        val = nd.get(field) or payload.get(field)
        return str(val).strip() if val else default

    ftype  = pick("type").upper()
    state  = pick("state").lower()
    status = pick("status").upper()
    return f"{ftype}__{state}__{status}"

SCHEMA_ONLY_TYPES = {"dictionary_item_added", "dictionary_item_removed"}

def diff_to_list(diff, mode="full"):
    """
    mode='full'   — report all differences (missing keys, extra keys, value/type changes)
    mode='schema' — report only missing/extra keys, ignore value and type changes
    """
    out = []
    for change_type, changes in diff.items():
        # Schema-only: skip value/type/list-item changes
        if mode == "schema" and change_type not in SCHEMA_ONLY_TYPES:
            continue
        if change_type in ("dictionary_item_added", "dictionary_item_removed",
                           "iterable_item_added", "iterable_item_removed"):
            for path in changes:
                out.append({"type": change_type.replace("_", " "), "path": str(path), "detail": ""})
        elif change_type in ("values_changed", "type_changes"):
            for path, info in changes.items():
                if change_type == "values_changed":
                    detail = f"{info['old_value']!r} → {info['new_value']!r}"
                else:
                    detail = f"{type(info['old_value']).__name__} → {type(info['new_value']).__name__}"
                out.append({"type": change_type.replace("_", " "), "path": str(path), "detail": detail})
    return out

def open_tunnel(cfg=None):
    cfg = cfg or get_cfg()
    t = sshtunnel.SSHTunnelForwarder(
        (cfg["ssh_host"], int(cfg["ssh_port"])),
        ssh_username=cfg["ssh_user"],
        ssh_pkey=os.path.expanduser(cfg["ssh_key"]),
        remote_bind_address=(cfg["db_host"], int(cfg["db_port"])),
    )
    t.start()
    return t

def connect_db(tunnel, cfg=None):
    cfg = cfg or get_cfg()
    conn = psycopg2.connect(
        host="127.0.0.1", port=tunnel.local_bind_port,
        dbname=cfg["db_name"], user=cfg["db_user"], password=cfg["db_pass"],
        options="-c default_transaction_read_only=on",
    )
    return conn

def fetch_notifications(cursor, subscriber_ids, since=None, ext_id=None, limit=300):
    """
    subscriber_ids : int or list of ints
    since          : ISO datetime string — filter by create_time >= since
    ext_id         : externalServiceRequestId string — fetch only notifications for this flow run
    """
    cfg = get_cfg()
    if isinstance(subscriber_ids, int):
        subscriber_ids = [subscriber_ids]
    placeholders = ",".join(["%s"] * len(subscriber_ids))
    q = f"""SELECT id, create_time, status, status_code, subscriber_id, payload
            FROM {cfg['db_table']}
            WHERE subscriber_id IN ({placeholders})
            AND payload IS NOT NULL"""
    params = list(subscriber_ids)
    if ext_id:
        # externalServiceRequestId lives inside notification_data in the JSONB payload
        q += " AND payload->'notification_data'->>'externalServiceRequestId' = %s"
        params.append(ext_id)
    if since:
        q += " AND create_time >= %s"
        params.append(since)
    q += " ORDER BY id ASC LIMIT %s"
    params.append(limit)
    cursor.execute(q, params)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, r)) for r in cursor.fetchall()]

def current_project():
    return (load_config().get("project") or "").strip()

def golden_root():
    """Root dir for goldens — golden/{project} when a project is set, else golden/."""
    proj = current_project()
    return (GOLDEN_DIR / proj) if proj else GOLDEN_DIR

# Capture sources — golden data is filed under golden/{project}/{source}/{FLOW}/...
GOLDEN_SOURCES = ("db", "isd", "kowl")

def golden_path(key, source="db"):
    """Write path: golden/{project}/{source}/{FLOW}/{key}.json"""
    flow_type = key.split("__")[0].upper()
    return golden_root() / source / flow_type / f"{key}.json"

def save_golden(key, payload, source="db"):
    path = golden_path(key, source)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str))

def load_golden(key, source=None):
    """
    Find a golden by key.
    - source given  -> look ONLY in that source (golden/{project}/{source}/...).
    - source None   -> search all sources, then legacy locations.
    """
    flow_type = key.split("__")[0].upper()
    if source:
        candidates = [
            golden_root() / source / flow_type / f"{key}.json",
            golden_root() / source / f"{key}.json",
        ]
    else:
        candidates = []
        for s in GOLDEN_SOURCES:
            candidates.append(golden_root() / s / flow_type / f"{key}.json")
            candidates.append(golden_root() / s / f"{key}.json")
        candidates += [
            golden_root() / flow_type / f"{key}.json",   # legacy golden/{project}/{FLOW}/{key}.json
            golden_root() / f"{key}.json",               # legacy golden/{project}/{key}.json
            GOLDEN_DIR / flow_type / f"{key}.json",       # legacy golden/{FLOW}/{key}.json
            GOLDEN_DIR / f"{key}.json",                   # legacy golden/{key}.json
        ]
    for path in candidates:
        if path.exists():
            return json.loads(path.read_text())
    return None

def list_goldens(project=None):
    """
    List goldens as relative paths (e.g. PROJECT/PUT/PUT__created__order_information).
    project=None lists everything; pass a name to scope to one project root.
    """
    root = (GOLDEN_DIR / project) if project else GOLDEN_DIR
    if not root.exists():
        return []
    return [str(p.relative_to(GOLDEN_DIR).with_suffix("")) for p in sorted(root.rglob("*.json"))]

def list_projects():
    """Top-level golden subdirs that look like project folders (not flow folders)."""
    flow_names = {"PUT", "PICK", "AUDIT", "OTHER"}
    projects = []
    for p in sorted(GOLDEN_DIR.iterdir()):
        if p.is_dir() and p.name.upper() not in flow_names:
            projects.append(p.name)
    return projects

def extract_ext_id(raw_payload):
    """Extract externalServiceRequestId before stripping — used for grouping only."""
    try:
        data = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
        data = normalize(data)
        nd = data.get("notification_data") or data
        return nd.get("externalServiceRequestId", "")
    except Exception:
        return ""

def process_rows(rows, mode="full", source=None):
    results = []
    for row in rows:
        try:
            ext_id  = extract_ext_id(row["payload"])
            payload = clean_payload(row["payload"])
            key     = notif_key(payload)
            golden  = load_golden(key, source=source)
            if golden is None:
                results.append({"db_id": row["id"], "create_time": str(row["create_time"]),
                                 "key": key, "ext_id": ext_id, "status": "NO GOLDEN", "findings": [],
                                 "payload": payload})
            else:
                diff = DeepDiff(golden, payload, ignore_order=True, verbose_level=2)
                findings = diff_to_list(diff, mode=mode)
                results.append({"db_id": row["id"], "create_time": str(row["create_time"]),
                                 "key": key, "ext_id": ext_id,
                                 "status": "PASS" if not findings else "FAIL",
                                 "findings": findings,
                                 "payload": payload})
        except Exception as e:
            results.append({"db_id": row.get("id"), "create_time": "?",
                             "key": "ERROR", "ext_id": "", "status": "ERROR",
                             "findings": [{"type": "exception", "path": "", "detail": str(e)}]})
    return results

# ─── TOPIC COMPARE (Kowl / Kafka UI) ──────────────────────────────────────────
# Pull notification messages straight from the Kowl topic viewer over its
# WebSocket API instead of the DB, then store one setup as a baseline and diff
# another setup's topics against it.  Reuses normalize()/DeepDiff/diff_to_list.

# Volatile fields stripped from topic envelopes before diffing (in addition to
# the DB IGNORE_FIELDS). These differ on every message / every setup.
TOPIC_IGNORE_FIELDS = IGNORE_FIELDS | {
    "message_id", "entity_id", "timestamp", "source_service",
    "transactionId", "transaction_start_time", "transaction_end_time",
}

def strip_fields(obj, fields):
    if isinstance(obj, dict):
        return {k: strip_fields(v, fields) for k, v in obj.items() if k not in fields}
    if isinstance(obj, list):
        return [strip_fields(i, fields) for i in obj]
    return obj

def clean_topic_payload(env):
    """env = the message's value.payload envelope from Kowl."""
    return strip_fields(normalize(env), TOPIC_IGNORE_FIELDS)

def topic_short(topic):
    """stpfunction-sbscloud.put_information.events -> put_information"""
    parts = topic.split(".")
    return parts[-2] if len(parts) >= 2 else topic

def topic_notif_key(env, label, topic):
    """Pairing key across setups: {LABEL}__{name}__{state}."""
    name  = env.get("name") or topic_short(topic)
    inner = env.get("payload") if isinstance(env.get("payload"), dict) else {}
    state = (inner.get("state") or env.get("state")
             or inner.get("status") or env.get("status") or "all")
    return f"{label}__{name}__{str(state).strip().lower()}"

def fetch_topic_messages(host, topic, count=50, start_offset=-1,
                         idle_timeout=12, hard_timeout=45):
    """
    Consume up to `count` messages from a Kowl topic over its WebSocket API.
    start_offset: -1 = newest (recent N), -2 = oldest.
    Returns the raw Kowl `message` objects (with .value.payload).
    """
    if websocket is None:
        raise RuntimeError("websocket-client not installed. Run: pip install websocket-client")
    url = f"ws://{host}/api/topics/{topic}/messages"
    ws = websocket.create_connection(url, timeout=idle_timeout)
    ws.settimeout(idle_timeout)
    req = {
        "topicName": topic, "startOffset": int(start_offset), "startTimestamp": 0,
        "partitionId": -1, "maxResults": int(count),
        "filterInterpreterCode": "", "enterprise": None,
    }
    ws.send(json.dumps(req))
    msgs, start = [], time.time()
    try:
        while time.time() - start < hard_timeout:
            try:
                raw = ws.recv()
            except Exception:
                break  # idle timeout / connection closed -> assume consume finished
            if not raw:
                break
            o = json.loads(raw)
            t = o.get("type")
            if t == "message":
                msgs.append(o.get("message"))
                if len(msgs) >= count:
                    break
            elif t == "done":
                break
            elif t == "error":
                raise RuntimeError(o.get("message") or "Kowl returned an error")
            # "phase" / "progress" messages are progress updates — keep reading
    finally:
        try:
            ws.close()
        except Exception:
            pass
    return msgs

def message_envelope(msg):
    """Extract the notification payload envelope (value.payload) from a Kowl message."""
    if not msg or msg.get("isValueNull"):
        return None
    val = msg.get("value") or {}
    return val.get("payload")

# Kowl baselines are golden data: stored under golden/{project}/kowl/{FLOW}/{key}.json
def topic_baseline_path(key):
    return golden_path(key, source="kowl")

def save_topic_baseline(key, payload):
    save_golden(key, payload, source="kowl")

def load_topic_baseline(key):
    p = topic_baseline_path(key)
    if p.exists():
        return json.loads(p.read_text())
    # fallback to the legacy flat store for baselines captured before this change
    legacy = TOPIC_DIR / f"{key}.json"
    return json.loads(legacy.read_text()) if legacy.exists() else None

def list_topic_baselines():
    root = golden_root() / "kowl"
    keys = {p.stem for p in root.rglob("*.json")} if root.exists() else set()
    keys |= {p.stem for p in TOPIC_DIR.glob("*.json")}  # include any legacy baselines
    return sorted(keys)

def capture_topics(host, topics, count):
    """Fetch each topic and store one baseline file per derived key. Returns summary."""
    saved = {}
    for spec in topics:
        label, topic = spec["label"], spec["topic"]
        for msg in fetch_topic_messages(host, topic, count):
            env = message_envelope(msg)
            if env is None:
                continue
            key = topic_notif_key(env, label, topic)
            save_topic_baseline(key, clean_topic_payload(env))  # last message per key wins
            entry = saved.setdefault(key, {"key": key, "topic": topic, "count": 0})
            entry["count"] += 1
    return list(saved.values())

def kowl_capture_thread(host, interval):
    """Live-poll Kowl topics and save each new message as a kowl golden, until stopped.
    Mirrors the DB 'Live Poll & Capture' flow."""
    cfg    = load_config()
    count  = int(cfg.get("topic_count") or 50)
    topics = cfg.get("topics", [])
    log    = kowl_capture_state["log_queue"]
    if not host:
        log.put({"type": "error", "msg": "No Kowl host configured (Config tab)."})
        kowl_capture_state["running"] = False
        return
    if not topics:
        log.put({"type": "error", "msg": "No topics configured (Config tab)."})
        kowl_capture_state["running"] = False
        return
    seen = set()
    primed = False   # ignore messages already in the topic at Start; capture only new ones
    try:
        while kowl_capture_state["running"]:
            for spec in topics:
                if not kowl_capture_state["running"]:
                    break
                label, topic = spec["label"], spec["topic"]
                try:
                    for msg in fetch_topic_messages(host, topic, count):
                        env = message_envelope(msg)
                        if env is None:
                            continue
                        rid = f"{topic}@p{msg.get('partitionID','?')}@{msg.get('offset','?')}"
                        if rid in seen:
                            continue
                        seen.add(rid)
                        if not primed:
                            continue   # pre-existing message at start — ignore
                        key = topic_notif_key(env, label, topic)
                        save_topic_baseline(key, clean_topic_payload(env))  # last msg per key wins
                        first = key not in kowl_capture_state["saved"]
                        kowl_capture_state["saved"][key] = True
                        log.put({"type": "saved" if first else "info",
                                 "msg": f"{'💾 NEW' if first else '↻ update'} {key}", "key": key})
                except Exception as e:
                    log.put({"type": "error", "msg": f"{topic}: {e}"})
            if not primed:
                primed = True
                log.put({"type": "info",
                         "msg": f"Capturing from Kowl {host} — {len(seen)} existing message(s) ignored. Trigger your flow now..."})
            time.sleep(interval)
        log.put({"type": "done", "msg": f"Capture stopped. {len(kowl_capture_state['saved'])} golden key(s) saved.",
                 "saved": sorted(kowl_capture_state["saved"].keys())})
    except Exception as e:
        log.put({"type": "error", "msg": f"Error: {e}"})
    finally:
        kowl_capture_state["running"] = False

def kowl_notification_data(env):
    """Extract a DB/ISD-style notification object from a Kowl envelope so it can be
    keyed with notif_key and compared against db/isd goldens."""
    if isinstance(env.get("notification_data"), dict):
        return env
    inner = env.get("payload")
    if isinstance(inner, dict):
        if isinstance(inner.get("notification_data"), dict):
            return inner
        return inner
    return env

def compare_kowl_env(env, label, topic, mode, golden_source, row_id):
    """Diff a single Kowl message envelope against the chosen golden source.
    golden_source='kowl' -> kowl baseline by topic key; 'db'/'isd' -> notif_key."""
    ext_id = env.get("entity_id") or ""
    if golden_source == "kowl":
        key     = topic_notif_key(env, label, topic)
        payload = clean_topic_payload(env)
        golden  = load_topic_baseline(key)
    else:  # 'isd' or 'db' — match by notification key
        nd      = kowl_notification_data(env)
        payload = clean_payload(nd)
        key     = notif_key(payload)
        golden  = load_golden(key, source=golden_source)
    base = {"db_id": row_id, "create_time": topic_short(topic), "key": key,
            "ext_id": ext_id, "flow": label}
    if golden is None:
        return {**base, "status": "NO GOLDEN", "findings": [], "payload": payload}
    diff = DeepDiff(golden, payload, ignore_order=True, verbose_level=2)
    findings = diff_to_list(diff, mode=mode)
    return {**base, "status": "PASS" if not findings else "FAIL",
            "findings": findings, "payload": payload}

def compare_topics(host, topics, count, mode="full", golden_source="kowl"):
    """Fetch Kowl topics and diff each message against the chosen golden source."""
    results = []
    for spec in topics:
        label, topic = spec["label"], spec["topic"]
        for msg in fetch_topic_messages(host, topic, count):
            env = message_envelope(msg)
            if env is None:
                continue
            row_id = f"p{msg.get('partitionID','?')}@{msg.get('offset','?')}"
            try:
                results.append(compare_kowl_env(env, label, topic, mode, golden_source, row_id))
            except Exception as e:
                results.append({"db_id": row_id, "create_time": topic_short(topic),
                                "key": "ERROR", "ext_id": "", "status": "ERROR",
                                "findings": [{"type": "exception", "path": "", "detail": str(e)}]})
    return results

def kowl_watch_loop(state, interval):
    """Live-poll Kowl topics and compare new messages against state['source'] golden.
    Shared by Watch and Full Run when the live data origin is Kowl."""
    cfg     = load_config()
    host    = (cfg.get("topic_host_b") or cfg.get("topic_host") or "").strip()
    count   = int(cfg.get("topic_count") or 50)
    topics  = cfg.get("topics", [])
    mode    = state.get("mode", "full")
    gsource = state.get("source", "kowl")
    log     = state["log_queue"]
    if not host:
        log.put({"type": "error", "msg": "No Kowl host configured (Config tab)."}); return
    if not topics:
        log.put({"type": "error", "msg": "No topics configured (Config tab)."}); return
    seen = set()
    primed = False   # first sweep baselines existing messages; only newer ones are compared
    try:
        while state["running"]:
            for spec in topics:
                if not state["running"]:
                    break
                label, topic = spec["label"], spec["topic"]
                try:
                    for msg in fetch_topic_messages(host, topic, count):
                        env = message_envelope(msg)
                        if env is None:
                            continue
                        rid = f"{topic}@p{msg.get('partitionID','?')}@{msg.get('offset','?')}"
                        if rid in seen:
                            continue
                        seen.add(rid)
                        if not primed:
                            continue   # pre-existing message at start — ignore
                        r = compare_kowl_env(env, label, topic, mode, gsource,
                                             f"p{msg.get('partitionID','?')}@{msg.get('offset','?')}")
                        state["results"].append(r)
                        icon = {"PASS": "✅", "FAIL": "❌", "NO GOLDEN": "⚠️", "ERROR": "🔥"}.get(r["status"], "?")
                        log.put({"type": r["status"].lower().replace(" ", "_"),
                                 "msg": f"{icon} {label} {r['key']} — {len(r['findings'])} diff(s)",
                                 "result": r})
                except Exception as e:
                    log.put({"type": "error", "msg": f"{topic}: {e}"})
            if not primed:
                primed = True
                log.put({"type": "info",
                         "msg": f"Watching Kowl {host} vs {gsource} golden — {len(seen)} existing message(s) ignored. Trigger your flow now..."})
            time.sleep(interval)
    except Exception as e:
        log.put({"type": "error", "msg": f"Error: {e}"})

# ─── ISD GOLDEN CAPTURE (read PDF spec → golden) ──────────────────────────────
# The ISD (Interface Specification Document) is a PDF containing both a field
# spec and sample payloads. We extract every embedded JSON object as a candidate
# golden, derive its key, and store it under the current project.

def clean_isd_text(text):
    """Strip PDF furniture that gets injected into the middle of multi-page JSON:
    page footers ('Page 39'), the GreyOrange logo line, and zero-width spaces."""
    import re
    text = text.replace("​", "")
    noise = re.compile(r"^\s*(Page\s+\d+|Grey\s?Orange.*)\s*$", re.I)
    return "\n".join(l for l in text.split("\n") if not noise.match(l))

def _try_json(chunk):
    """json.loads with a tolerant repair pass for common ISD-PDF corruption:
    smart quotes used as delimiters and trailing commas."""
    import re
    try:
        return json.loads(chunk)
    except Exception:
        pass
    repaired = (chunk.replace("“", '"').replace("”", '"')
                     .replace("‘", "'").replace("’", "'"))
    repaired = re.sub(r",(\s*[}\]])", r"\1", repaired)   # trailing commas
    try:
        return json.loads(repaired)
    except Exception:
        return None

def extract_json_objects(text):
    """Scan text for balanced JSON objects. Returns (objects, attempted_count)."""
    objs, attempts, i, n = [], 0, 0, len(text)
    while i < n:
        if text[i] != "{":
            i += 1
            continue
        depth, instr, esc, j = 0, False, False, i
        while j < n:
            c = text[j]
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                instr = not instr
            elif not instr:
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        chunk = text[i:j + 1]
                        attempts += 1
                        obj = _try_json(chunk)
                        if isinstance(obj, dict) and obj:
                            objs.append(obj)
                        break
            j += 1
        i = j + 1
    return objs, attempts

def guess_project_name(text, filename=""):
    """Best-effort project/interface name from the ISD header or filename."""
    import re
    for line in (text or "").splitlines():
        s = line.strip()
        m = re.match(r"(?i)(project|interface|module)\s*[:\-]\s*(.+)", s)
        if m and m.group(2).strip():
            return m.group(2).strip()[:60]
    if filename:
        return Path(filename).stem[:60]
    return ""

def parse_isd_pdf(data, filename=""):
    """
    Returns {"project": str, "pages": int, "payloads": [raw dicts], "text_len": int}.
    payloads = sample JSON notifications found in the document.
    """
    if fitz is None:
        raise RuntimeError("PyMuPDF not installed. Run: pip install PyMuPDF")
    doc = fitz.open(stream=data, filetype="pdf")
    pages = doc.page_count
    raw_text = "\n".join(page.get_text() for page in doc)
    doc.close()
    text = clean_isd_text(raw_text)
    payloads, attempts = extract_json_objects(text)
    return {
        "project": guess_project_name(raw_text, filename),
        "pages": pages,
        "text_len": len(text),
        "payloads": payloads,
        "attempts": attempts,           # how many balanced {...} blocks we tried
    }

def unwrap_notification(obj):
    """From an ISD Kafka envelope, return (notification_data, notification_type).
    Handles {payload:{notification_type, notification_data}} and flat shapes."""
    pl = obj.get("payload") if isinstance(obj.get("payload"), dict) else obj
    nt = pl.get("notification_type") or obj.get("notification_type") or ""
    nd = pl.get("notification_data") or obj.get("notification_data")
    if isinstance(nd, dict):
        return nd, nt
    return None, nt

def isd_golden_key(nd, notification_type):
    """Build a golden key from a notification_data block. Prefers {FLOW}__{state}__{status};
    falls back to the notification_type when type/state/status are absent (dock, cancel, tag)."""
    flow   = str(nd.get("type") or "").strip().upper()
    state  = str(nd.get("state") or "").strip().lower()
    status = str(nd.get("status") or "").strip().upper()
    if not flow:
        flow = (notification_type or "NOTIFICATION").strip().upper()
    import re
    parts = [re.sub(r"[^A-Za-z0-9.-]", "_", p) for p in (flow, state, status) if p]
    return "__".join(parts)

def capture_isd_goldens(payloads):
    """Unwrap each ISD envelope to its notification_data, key it, and save the
    largest payload per key as golden. Returns summary list."""
    best = {}
    for raw in payloads:
        nd, nt = unwrap_notification(raw)
        if not nd:
            continue
        try:
            payload = clean_payload(nd)
            key = isd_golden_key(nd, nt)
            if not key:
                continue
            size = len(json.dumps(payload, default=str))
            if key not in best or size > best[key]["size"]:
                best[key] = {"payload": payload, "size": size}
        except Exception:
            continue
    saved = []
    for key, info in best.items():
        save_golden(key, info["payload"], source="isd")
        saved.append({"key": key, "count": 1})
    return saved

# ─── HTML REPORT (downloadable execution report) ──────────────────────────────

def _diff_dot_paths(findings):
    """DeepDiff paths (root['a']['b'][0]) -> dot-paths (a.b.0). Mirrors the UI colorizer."""
    import re
    out = set()
    for f in findings or []:
        segs = re.findall(r"\[['\"]?[^\]'\"]+['\"]?\]", f.get("path", ""))
        path = ".".join(seg.strip("[]'\"") for seg in segs)
        if path:
            out.add(path)
    return out

def _dot_path_bad(path, diff_paths):
    if not path:
        return False
    if path in diff_paths:
        return True
    return any(path.startswith(d + ".") for d in diff_paths)

def color_payload_html(payload, findings):
    """Pretty-print payload to HTML lines: green = matches golden, red = exact mismatch."""
    dp = _diff_dot_paths(findings)

    def esc(s):
        return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    lines = []

    def walk(node, path, indent, key, comma):
        pad = "  " * indent
        bad = _dot_path_bad(path, dp)
        prefix = f'"{key}": ' if key is not None else ""
        if isinstance(node, (dict, list)):
            is_arr = isinstance(node, list)
            lines.append((pad + prefix + ("[" if is_arr else "{"), bad))
            ents = list(enumerate(node)) if is_arr else list(node.items())
            for i, (k, v) in enumerate(ents):
                cp = f"{path}.{k}" if path else str(k)
                walk(v, cp, indent + 1, (None if is_arr else k), i < len(ents) - 1)
            lines.append((pad + ("]" if is_arr else "}") + ("," if comma else ""), bad))
        else:
            lines.append((pad + prefix + json.dumps(node, default=str) + ("," if comma else ""), bad))

    walk(payload, "", 0, None, False)
    spans = []
    for text, bad in lines:
        style = ("color:#b91c1c;background:#fee2e2;display:block;padding:0 4px"
                 if bad else "color:#15803d;display:block;padding:0 4px")
        spans.append(f'<span style="{style}">{esc(text)}</span>')
    return "".join(spans)

def build_html_report(results, title="Notification Comparison Report", meta=None):
    """Self-contained HTML report of a comparison run (green=pass, red=fail)."""
    meta = meta or {}
    total = len(results)
    npass = sum(1 for r in results if r["status"] == "PASS")
    nfail = sum(1 for r in results if r["status"] == "FAIL")
    nother = total - npass - nfail

    def esc(s):
        return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

    rows = []
    for r in results:
        st = r["status"]
        color = {"PASS": "#16a34a", "FAIL": "#dc2626"}.get(st, "#b45309")
        findings = "".join(
            f'<div style="color:#b91c1c;font-family:monospace;font-size:12px;padding:2px 0">'
            f'<b>{esc(f["type"])}</b> {esc(f["path"])} {esc(f.get("detail",""))}</div>'
            for f in r.get("findings", [])
        ) or '<div style="color:#16a34a;font-size:12px">✓ matches golden</div>'
        if r.get("payload") is not None:
            findings += (
                '<details style="margin-top:6px"' + (' open' if r.get("findings") else '') + '>'
                '<summary style="cursor:pointer;font-size:11px;color:#475569">'
                'payload (<span style="color:#15803d">green = matches golden</span>, '
                '<span style="color:#b91c1c">red = mismatch</span>)</summary>'
                '<pre style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:6px;'
                'padding:10px;overflow:auto;font-size:11px;line-height:1.55;margin:6px 0 0;'
                'font-family:ui-monospace,Menlo,monospace;white-space:pre">'
                + color_payload_html(r["payload"], r.get("findings", []))
                + '</pre></details>'
            )
        rows.append(f"""
        <tr style="border-bottom:1px solid #e5e7eb">
          <td style="padding:8px;font-family:monospace;font-size:12px">{esc(r.get('db_id',''))}</td>
          <td style="padding:8px;font-family:monospace;font-size:12px">{esc(r.get('key',''))}</td>
          <td style="padding:8px;font-family:monospace;font-size:11px;color:#6b7280">{esc(r.get('ext_id','') or '—')}</td>
          <td style="padding:8px"><b style="color:{color}">{esc(st)}</b></td>
          <td style="padding:8px">{findings}</td>
        </tr>""")

    meta_rows = "".join(
        f'<span style="margin-right:18px"><b>{esc(k)}:</b> {esc(v)}</span>' for k, v in meta.items()
    )

    # SVG donut chart (circumference-based dash segments)
    import math
    R, C = 60, 2 * math.pi * 60
    base = total or 1
    seg_pass, seg_fail = C * npass / base, C * nfail / base
    seg_other = C * nother / base
    pct_pass = round(npass / base * 100)
    donut = f"""<svg width="160" height="160" viewBox="0 0 160 160">
      <circle cx="80" cy="80" r="{R}" fill="none" stroke="#e5e7eb" stroke-width="22"/>
      <g transform="rotate(-90 80 80)">
        <circle cx="80" cy="80" r="{R}" fill="none" stroke="#16a34a" stroke-width="22"
                stroke-dasharray="{seg_pass:.2f} {C - seg_pass:.2f}" stroke-dashoffset="0"/>
        <circle cx="80" cy="80" r="{R}" fill="none" stroke="#dc2626" stroke-width="22"
                stroke-dasharray="{seg_fail:.2f} {C - seg_fail:.2f}" stroke-dashoffset="{-seg_pass:.2f}"/>
        <circle cx="80" cy="80" r="{R}" fill="none" stroke="#eab308" stroke-width="22"
                stroke-dasharray="{seg_other:.2f} {C - seg_other:.2f}" stroke-dashoffset="{-(seg_pass + seg_fail):.2f}"/>
      </g>
      <text x="80" y="74" text-anchor="middle" font-size="26" font-weight="700" fill="#0f172a">{pct_pass}%</text>
      <text x="80" y="94" text-anchor="middle" font-size="11" fill="#64748b">pass</text>
    </svg>"""

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{esc(title)}</title></head>
<body style="font-family:-apple-system,Segoe UI,sans-serif;background:#f8fafc;color:#0f172a;margin:0;padding:28px">
  <h1 style="font-size:20px;margin:0 0 4px">{esc(title)}</h1>
  <div style="font-size:12px;color:#64748b;margin-bottom:16px">{meta_rows}</div>
  <div style="display:flex;align-items:center;gap:28px;margin-bottom:20px;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:16px">
    {donut}
    <div style="font-size:13px">
      <div style="margin:5px 0"><span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:#16a34a;margin-right:7px"></span>Pass <b>{npass}</b></div>
      <div style="margin:5px 0"><span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:#dc2626;margin-right:7px"></span>Fail <b>{nfail}</b></div>
      <div style="margin:5px 0"><span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:#eab308;margin-right:7px"></span>Other <b>{nother}</b></div>
    </div>
  </div>
  <div style="display:flex;gap:12px;margin-bottom:20px">
    <div style="flex:1;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px;text-align:center"><div style="font-size:26px;font-weight:700">{total}</div><div style="font-size:11px;color:#64748b">TOTAL</div></div>
    <div style="flex:1;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px;text-align:center"><div style="font-size:26px;font-weight:700;color:#16a34a">{npass}</div><div style="font-size:11px;color:#64748b">PASS</div></div>
    <div style="flex:1;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px;text-align:center"><div style="font-size:26px;font-weight:700;color:#dc2626">{nfail}</div><div style="font-size:11px;color:#64748b">FAIL</div></div>
    <div style="flex:1;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px;text-align:center"><div style="font-size:26px;font-weight:700;color:#b45309">{nother}</div><div style="font-size:11px;color:#64748b">OTHER</div></div>
  </div>
  <table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden">
    <thead><tr style="background:#f1f5f9;text-align:left"><th style="padding:8px;font-size:11px;color:#64748b">ID</th><th style="padding:8px;font-size:11px;color:#64748b">KEY</th><th style="padding:8px;font-size:11px;color:#64748b">REQUEST ID</th><th style="padding:8px;font-size:11px;color:#64748b">STATUS</th><th style="padding:8px;font-size:11px;color:#64748b">FINDINGS</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</body></html>"""

def save_report(html, prefix="report"):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"{prefix}_{ts}.html"
    (REPORTS_DIR / name).write_text(html)
    return name

def save_report_meta(report_name, results, project="", mode="full", per_flow=None,
                     created=None, kind="run", allure_zip=None, allure_html=None):
    """Write a sidecar so the dashboard list can show project/time/pass-fail
    (and persistent Allure links) without opening each HTML report."""
    total = len(results)
    npass = sum(1 for r in results if r.get("status") == "PASS")
    nfail = sum(1 for r in results if r.get("status") == "FAIL")
    meta = {
        "name": report_name,
        "project": project or "(none)",
        "created": created or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": mode,
        "kind": kind,
        "total": total,
        "pass": npass,
        "fail": nfail,
        "other": total - npass - nfail,
        "per_flow": per_flow or {},
        "allure_zip": allure_zip,
        "allure_html": allure_html,
    }
    (REPORTS_DIR / f"{report_name}.meta.json").write_text(json.dumps(meta, indent=2))
    return meta

def _created_from_name(name):
    """Parse 'prefix_YYYYMMDD_HHMMSS.html' -> 'YYYY-MM-DD HH:MM:SS' (best effort)."""
    try:
        stem = name.rsplit(".", 1)[0]
        d, t = stem.split("_")[-2], stem.split("_")[-1]
        return f"{d[:4]}-{d[4:6]}-{d[6:8]} {t[:2]}:{t[2:4]}:{t[4:6]}"
    except Exception:
        return ""

def list_reports_meta():
    """Return report descriptors (newest first), reading sidecars when present."""
    out = []
    for p in sorted(REPORTS_DIR.glob("*.html"), key=lambda x: x.name, reverse=True):
        sidecar = REPORTS_DIR / f"{p.name}.meta.json"
        if sidecar.exists():
            try:
                out.append(json.loads(sidecar.read_text()))
                continue
            except Exception:
                pass
        out.append({"name": p.name, "created": _created_from_name(p.name)})
    return out

# ─── RUN-ALL (collective comparison across all DB flows) ──────────────────────

FLOW_SUBSCRIBER_KEYS = {
    "PUT":   "subscriber_put",
    "PICK":  "subscriber_pick",
    "AUDIT": "subscriber_audit",
    "OTHER": "subscriber_other",
}

def run_all_db_flows(since=None, limit=200, mode="full", source="db"):
    """Compare recent notifications for every configured subscriber flow against goldens."""
    cfg = get_cfg()
    tunnel = open_tunnel(cfg)
    try:
        conn = connect_db(tunnel, cfg)
        cur = conn.cursor()
        all_results, per_flow = [], {}
        for flow, cfg_key in FLOW_SUBSCRIBER_KEYS.items():
            sub = cfg.get(cfg_key)
            if not sub:
                continue
            rows = fetch_notifications(cur, int(sub), since=since, limit=limit)
            res = process_rows(rows, mode=mode, source=source)
            for r in res:
                r["flow"] = flow
            per_flow[flow] = {
                "total": len(res),
                "pass":  sum(1 for r in res if r["status"] == "PASS"),
                "fail":  sum(1 for r in res if r["status"] == "FAIL"),
            }
            all_results.extend(res)
        cur.close()
        conn.close()
        return all_results, per_flow
    finally:
        tunnel.stop()

# ─── LIVE CAPTURE THREAD ──────────────────────────────────────────────────────

def capture_live_thread(subscriber_id, interval, ext_id=None):
    cfg = get_cfg()
    log = capture_state["log_queue"]
    since = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    try:
        log.put({"type": "info", "msg": f"Opening SSH tunnel to {cfg['ssh_host']}..."})
        tunnel = open_tunnel(cfg)
        conn   = connect_db(tunnel, cfg)
        cur    = conn.cursor()
        mode_msg = f"ext_id={ext_id}" if ext_id else "polling by time"
        log.put({"type": "info", "msg": f"Connected. Watching ({mode_msg}) — trigger your flow now..."})

        while capture_state["running"]:
            rows = fetch_notifications(cur, subscriber_id, since=since, ext_id=ext_id)
            new  = [r for r in rows if r["id"] not in capture_state["seen"]]
            for row in new:
                capture_state["seen"].add(row["id"])
                try:
                    payload = clean_payload(row["payload"])
                    key     = notif_key(payload)
                    if key not in capture_state["saved"]:
                        save_golden(key, payload)
                        capture_state["saved"][key] = True
                        log.put({"type": "pass", "msg": f"📸 [{row['id']}] Saved golden: {key}"})
                    else:
                        log.put({"type": "info", "msg": f"⏭  [{row['id']}] Already captured: {key} (keeping first)"})
                except Exception as e:
                    log.put({"type": "error", "msg": f"⚠️  [{row['id']}] Error: {e}"})
            time.sleep(interval)

        cur.close(); conn.close(); tunnel.stop()
        saved = list(capture_state["saved"].keys())
        log.put({"type": "done", "msg": f"Stopped. {len(saved)} golden snapshot(s) saved.", "saved": saved})
    except Exception as e:
        log.put({"type": "error", "msg": f"Error: {e}"})
    finally:
        capture_state["running"] = False

# ─── WATCH THREAD ─────────────────────────────────────────────────────────────

def watch_thread_fn(subscriber_id, interval):
    # NOTE: running is set True by the start endpoint before this thread starts,
    # so a fast stop() can't be clobbered by a late-scheduled thread.
    watch_state["results"] = []
    # Live data origin: Kowl (kowl golden, or isd golden vs kowl) -> poll topics.
    if watch_state.get("data_source") == "kowl":
        try:
            kowl_watch_loop(watch_state, interval)
            watch_state["log_queue"].put({"type": "done", "msg": "Watch stopped."})
        finally:
            watch_state["running"] = False
        return
    seen = set()
    since = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log = watch_state["log_queue"]

    try:
        cfg = get_cfg()
        log.put({"type": "info", "msg": f"Opening SSH tunnel to {cfg['ssh_host']}..."})
        tunnel = open_tunnel(cfg)
        conn = connect_db(tunnel, cfg)
        cur = conn.cursor()
        log.put({"type": "info", "msg": "Connected. Watching for new notifications..."})

        ext_id = watch_state.get("ext_id")
        mode_msg = f"ext_id={ext_id}" if ext_id else "polling by time"
        log.put({"type": "info", "msg": f"Connected. Watching ({mode_msg})..."})

        while watch_state["running"]:
            rows = fetch_notifications(cur, subscriber_id, since=since, ext_id=ext_id)
            new = [r for r in rows if r["id"] not in seen]
            for row in new:
                seen.add(row["id"])
                results = process_rows([row], mode=watch_state.get("mode", "full"),
                                       source=watch_state.get("source", "db"))
                r = results[0]
                watch_state["results"].append(r)
                icon   = {"PASS": "✅", "FAIL": "❌", "NO GOLDEN": "⚠️", "ERROR": "🔥"}.get(r["status"], "?")
                # NOTE: use a distinct name — do NOT reassign `ext_id`, which is the
                # query filter for the next poll; clobbering it pins the watch to one flow.
                row_ext_id = r.get("ext_id", "")
                ext_str = f" [{row_ext_id}]" if row_ext_id else ""
                log.put({"type": r["status"].lower().replace(" ", "_"), "msg": f"{icon} [{r['db_id']}]{ext_str} {r['key']} — {len(r['findings'])} diff(s)", "result": r})
            time.sleep(interval)

        cur.close(); conn.close(); tunnel.stop()
        log.put({"type": "done", "msg": "Watch stopped."})
    except Exception as e:
        log.put({"type": "error", "msg": f"Error: {e}"})
    finally:
        watch_state["running"] = False

# ─── ALLURE REPORT ────────────────────────────────────────────────────────────
# We can't render Allure HTML without the `allure` CLI + Java, so we always emit
# allure-results (the JSON the Allure CLI consumes) and zip it for download.
# If the CLI happens to be installed at runtime, we also generate the HTML report.

ALLURE_DIR = Path(__file__).parent / "allure-results"

ALLURE_STATUS = {
    "PASS": "passed", "FAIL": "failed",
    "NO GOLDEN": "skipped", "NO BASELINE": "skipped", "ERROR": "broken",
}

def build_allure_results(results, meta, start_ms, stop_ms):
    """Write allure-results JSON files into a fresh per-run dir. Returns (dir, run_id)."""
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ALLURE_DIR / run_id
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    project = meta.get("Project", "") or "default"
    gsource = meta.get("Golden source", "db")
    for r in results:
        u = str(uuid.uuid4())
        key = r.get("key", "?")
        flow = r.get("flow") or key.split("__")[0]
        findings = r.get("findings", [])
        trace = "\n".join(f"{f.get('type','')}: {f.get('path','')} {f.get('detail','')}".strip()
                          for f in findings)
        attachments = []
        if r.get("payload") is not None:
            att = f"{u}-payload.json"
            (out / att).write_text(json.dumps(r["payload"], indent=2, default=str))
            attachments.append({"name": "payload.json", "source": att, "type": "application/json"})
        res = {
            "uuid": u,
            "historyId": f"{project}.{key}",
            "name": key + (f"  [{r.get('ext_id')}]" if r.get("ext_id") else ""),
            "fullName": f"{flow}.{key}",
            "status": ALLURE_STATUS.get(r.get("status"), "unknown"),
            "statusDetails": {
                "message": f"{len(findings)} difference(s)" if findings else "matches golden",
                "trace": trace,
            },
            "stage": "finished",
            "start": start_ms,
            "stop": stop_ms,
            "labels": [
                {"name": "feature", "value": flow},
                {"name": "suite", "value": project},
                {"name": "parentSuite", "value": f"{gsource} golden"},
                {"name": "framework", "value": "NotificationComparator"},
            ],
            "parameters": [{"name": "request id", "value": str(r.get("ext_id") or "")}],
            "attachments": attachments,
        }
        (out / f"{u}-result.json").write_text(json.dumps(res, indent=2))

    (out / "environment.properties").write_text(
        "\n".join(f"{k.replace(' ', '_')}={v}" for k, v in meta.items()))
    (out / "categories.json").write_text(json.dumps([
        {"name": "Schema / value mismatches", "matchedStatuses": ["failed"]},
        {"name": "Missing golden", "matchedStatuses": ["skipped"]},
        {"name": "Errors", "matchedStatuses": ["broken"]},
    ], indent=2))
    return out, run_id

def zip_dir(src_dir, zip_path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for p in src_dir.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(src_dir))

def try_allure_generate(results_dir):
    """If the allure CLI is installed, build an HTML report. Returns dir name or None."""
    allure = shutil.which("allure")
    if not allure:
        return None
    html = results_dir.parent / f"{results_dir.name}-html"
    try:
        subprocess.run([allure, "generate", str(results_dir), "-o", str(html), "--clean"],
                       check=True, capture_output=True, timeout=180)
        return html
    except Exception:
        return None

def generate_allure(results, meta, start_dt, stop_dt):
    """Build allure-results, zip them, and (if CLI present) the HTML report.
    Returns {'zip': name, 'html': name|None, 'run_id': id}."""
    start_ms = int(start_dt.timestamp() * 1000)
    stop_ms = int(stop_dt.timestamp() * 1000)
    out, run_id = build_allure_results(results, meta, start_ms, stop_ms)
    zip_name = f"allure_{run_id}.zip"
    zip_dir(out, REPORTS_DIR / zip_name)
    html_dir = try_allure_generate(out)
    return {"zip": zip_name, "html": (html_dir.name if html_dir else None), "run_id": run_id}

# ─── FULL RUN THREAD (live compare across ALL flows) ──────────────────────────

def full_watch_thread_fn(interval):
    """Live-watch every configured subscriber at once, time-bounded.
    On stop, build + save a collective report (+ metadata sidecar)."""
    full_watch_state["results"] = []
    seen = set()
    started = datetime.now()
    since = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    full_watch_state["started_at"] = started.strftime("%Y-%m-%d %H:%M:%S")
    log = full_watch_state["log_queue"]
    mode = full_watch_state.get("mode", "full")
    source = full_watch_state.get("source", "db")

    try:
        if full_watch_state.get("data_source") == "kowl":
            # Live data origin: Kowl topics, compared vs kowl/isd golden.
            kowl_watch_loop(full_watch_state, interval)
        else:
            cfg = get_cfg()
            # Build subscriber_id -> flow map from configured subscribers.
            sub_to_flow, sub_ids = {}, []
            for flow, cfg_key in FLOW_SUBSCRIBER_KEYS.items():
                sub = cfg.get(cfg_key)
                if not sub:
                    continue
                sub_to_flow[int(sub)] = flow
                sub_ids.append(int(sub))
            if not sub_ids:
                log.put({"type": "error", "msg": "No subscriber IDs configured. Set them on the Config tab."})
                return

            log.put({"type": "info", "msg": f"Opening SSH tunnel to {cfg['ssh_host']}..."})
            tunnel = open_tunnel(cfg)
            conn = connect_db(tunnel, cfg)
            cur = conn.cursor()
            flows_str = ", ".join(f"{f}={s}" for s, f in sub_to_flow.items())
            log.put({"type": "info", "msg": f"Connected. Full Run watching all flows ({flows_str}) — trigger your automation now..."})

            while full_watch_state["running"]:
                rows = fetch_notifications(cur, sub_ids, since=since)
                new = [r for r in rows if r["id"] not in seen]
                for row in new:
                    seen.add(row["id"])
                    results = process_rows([row], mode=mode, source=source)
                    r = results[0]
                    r["flow"] = sub_to_flow.get(row.get("subscriber_id"), "OTHER")
                    full_watch_state["results"].append(r)
                    icon = {"PASS": "✅", "FAIL": "❌", "NO GOLDEN": "⚠️", "ERROR": "🔥"}.get(r["status"], "?")
                    row_ext_id = r.get("ext_id", "")
                    ext_str = f" [{row_ext_id}]" if row_ext_id else ""
                    log.put({"type": r["status"].lower().replace(" ", "_"),
                             "msg": f"{icon} {r['flow']} [{r['db_id']}]{ext_str} {r['key']} — {len(r['findings'])} diff(s)",
                             "result": r})
                time.sleep(interval)

            cur.close(); conn.close(); tunnel.stop()

        # Finalize: build per-flow summary + report.
        results = full_watch_state["results"]
        per_flow = {}
        for r in results:
            f = r.get("flow", "OTHER")
            s = per_flow.setdefault(f, {"total": 0, "pass": 0, "fail": 0})
            s["total"] += 1
            if r["status"] == "PASS":
                s["pass"] += 1
            elif r["status"] == "FAIL":
                s["fail"] += 1
        stopped_dt = datetime.now()
        stopped = stopped_dt.strftime("%Y-%m-%d %H:%M:%S")
        project = current_project()
        meta = {
            "Project": project or "(none)",
            "Golden source": source,
            "Mode": mode,
            "Started": full_watch_state["started_at"],
            "Stopped": stopped,
            "Flows": ", ".join(f"{k}({v['pass']}/{v['total']})" for k, v in per_flow.items()) or "none",
        }
        report_name = save_report(build_html_report(results, "Full Run Report", meta), prefix="full_run")
        # Allure: always emit allure-results (+ zip); HTML too if the CLI is installed.
        allure = {"zip": None, "html": None}
        try:
            allure = generate_allure(results, meta, started, stopped_dt)
        except Exception as e:
            log.put({"type": "info", "msg": f"(Allure generation skipped: {e})"})
        # Persist Allure links in the sidecar so Past Reports can show them later.
        save_report_meta(report_name, results, project=project, mode=mode,
                         per_flow=per_flow, created=stopped, kind="full_run",
                         allure_zip=allure.get("zip"), allure_html=allure.get("html"))
        log.put({"type": "done",
                 "msg": f"Full Run stopped. {len(results)} notification(s) compared. Report saved.",
                 "report": report_name,
                 "allure_zip": allure.get("zip"),
                 "allure_html": allure.get("html")})
    except Exception as e:
        log.put({"type": "error", "msg": f"Error: {e}"})
    finally:
        full_watch_state["running"] = False

# ─── ROUTES ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML_UI)

@app.route("/api/config", methods=["GET"])
def api_get_config():
    cfg = load_config()  # disk only — no secrets
    cfg["secrets_ready"] = secrets_ready()
    return jsonify(cfg)

@app.route("/api/config", methods=["POST"])
def api_save_config():
    data    = request.json
    current = load_config()
    merged  = {**current, **{k: v for k, v in data.items() if k not in SECRET_FIELDS}}
    for key in ("ssh_port", "db_port", "poll_interval", "topic_count",
                "subscriber_put", "subscriber_pick", "subscriber_audit", "subscriber_other"):
        try:
            merged[key] = int(merged[key])
        except (ValueError, KeyError, TypeError):
            pass
    save_config(merged)
    return jsonify({"ok": True})

@app.route("/api/secrets", methods=["POST"])
def api_set_secrets():
    """Store secrets in memory. Optionally persist to .secrets file."""
    data = request.json
    db_pass = data.get("db_pass", "")
    ssh_key = data.get("ssh_key", "")
    if db_pass:
        RUNTIME_SECRETS["db_pass"] = db_pass
    if ssh_key:
        RUNTIME_SECRETS["ssh_key"] = ssh_key
    if data.get("save_to_disk"):
        # Persist the merged runtime values so updating just one field
        # (e.g. a new DB password) doesn't require re-entering the other.
        if RUNTIME_SECRETS.get("db_pass") and RUNTIME_SECRETS.get("ssh_key"):
            save_secrets_to_disk(
                RUNTIME_SECRETS["db_pass"],
                RUNTIME_SECRETS["ssh_key"],
            )
        else:
            return jsonify({"ok": False,
                            "error": "Need both DB password and SSH key to save to disk"}), 400
    return jsonify({"ok": True, "secrets_ready": secrets_ready()})

@app.route("/api/secrets/saved", methods=["GET"])
def api_secrets_saved_status():
    return jsonify({"saved": SECRETS_PATH.exists(), "secrets_ready": secrets_ready()})

@app.route("/api/secrets/clear", methods=["POST"])
def api_clear_saved_secrets():
    clear_saved_secrets()
    return jsonify({"ok": True})

@app.route("/api/config/test", methods=["POST"])
def api_test_connection():
    if not secrets_ready():
        return jsonify({"ok": False, "msg": "⚠️ Enter DB password and SSH key path first"}), 400
    try:
        cfg = get_cfg()
        tunnel = open_tunnel(cfg)
        conn = connect_db(tunnel, cfg)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close(); conn.close(); tunnel.stop()
        return jsonify({"ok": True, "msg": "Connection successful ✅"})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})

@app.route("/api/goldens")
def api_goldens():
    return jsonify(list_goldens())

@app.route("/api/capture", methods=["POST"])
def api_capture():
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    data       = request.json
    subscriber = parse_int(data.get("subscriber"))
    if subscriber is None:
        return jsonify({"ok": False, "error": "Select a flow (or enter a Subscriber ID) first"}), 400
    since      = data.get("since")  or None
    ext_id     = data.get("ext_id") or None
    if not since and not ext_id:
        return jsonify({"ok": False, "error": "Provide either a since time or an External Request ID"}), 400
    try:
        tunnel = open_tunnel()
        conn = connect_db(tunnel)
        cur = conn.cursor()
        rows = fetch_notifications(cur, subscriber, since=since, ext_id=ext_id)
        saved = {}
        for row in rows:
            try:
                payload = clean_payload(row["payload"])
                key = notif_key(payload)
                if key not in saved:
                    save_golden(key, payload)
                    saved[key] = True
            except Exception as e:
                pass  # silently skip malformed rows
        cur.close(); conn.close(); tunnel.stop()
        return jsonify({"ok": True, "saved": list(saved.keys()), "total_fetched": len(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/compare", methods=["POST"])
def api_compare():
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    data       = request.json
    subscriber = parse_int(data.get("subscriber"))
    if subscriber is None:
        return jsonify({"ok": False, "error": "Select a flow (or enter a Subscriber ID) first"}), 400
    mode       = data.get("mode", "full")
    since      = data.get("since") or None
    ext_id     = data.get("ext_id") or None
    gsource    = data.get("golden_source") or "db"   # db | isd (kowl handled via /api/topics/compare)

    if not since and not ext_id:
        return jsonify({"ok": False, "error": "Provide either a time range (since) or an External Request ID"}), 400
    try:
        tunnel = open_tunnel()
        conn = connect_db(tunnel)
        cur = conn.cursor()
        rows = fetch_notifications(cur, subscriber, since=since, ext_id=ext_id)
        results = process_rows(rows, mode=mode, source=gsource)
        cur.close(); conn.close(); tunnel.stop()
        return jsonify({"ok": True, "results": results, "total": len(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/capture/live/start", methods=["POST"])
def api_capture_live_start():
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    t_old = capture_state.get("thread")
    if capture_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"ok": False, "error": "Already running"}), 400
    data = request.json
    subscriber = parse_int(data.get("subscriber"))
    if subscriber is None:
        return jsonify({"ok": False, "error": "Select a flow (or enter a Subscriber ID) first"}), 400
    interval   = parse_int(data.get("interval"), 3)
    ext_id     = data.get("ext_id") or None
    t = threading.Thread(target=capture_live_thread, args=(subscriber, interval, ext_id), daemon=True)
    capture_state["running"] = True
    capture_state["seen"]    = set()
    capture_state["saved"]   = {}
    capture_state["log_queue"] = queue.Queue()
    capture_state["thread"]  = t
    t.start()
    return jsonify({"ok": True})

@app.route("/api/capture/live/stop", methods=["POST"])
def api_capture_live_stop():
    capture_state["running"] = False
    return jsonify({"ok": True, "saved": list(capture_state.get("saved", {}).keys())})

@app.route("/api/capture/live/stream")
def api_capture_live_stream():
    def generate():
        while True:
            try:
                item = capture_state["log_queue"].get(timeout=30)
                yield f"data: {json.dumps(item)}\n\n"
                if item.get("type") == "done":
                    break
            except queue.Empty:
                yield 'data: {"type":"ping"}\n\n'
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# ─── KOWL LIVE CAPTURE (start -> run flow -> stop; saves kowl goldens live) ────

@app.route("/api/kowl-capture/start", methods=["POST"])
def api_kowl_capture_start():
    t_old = kowl_capture_state.get("thread")
    if kowl_capture_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"ok": False, "error": "Already running"}), 400
    data = request.json or {}
    cfg  = load_config()
    host = (data.get("host") or cfg.get("topic_host") or "").strip()
    if not host:
        return jsonify({"ok": False, "error": "No Kowl host configured."}), 400
    interval = parse_int(data.get("interval"), 3)
    kowl_capture_state["running"]   = True
    kowl_capture_state["saved"]     = {}
    kowl_capture_state["log_queue"] = queue.Queue()
    t = threading.Thread(target=kowl_capture_thread, args=(host, interval), daemon=True)
    kowl_capture_state["thread"] = t
    t.start()
    return jsonify({"ok": True})

@app.route("/api/kowl-capture/stop", methods=["POST"])
def api_kowl_capture_stop():
    kowl_capture_state["running"] = False
    return jsonify({"ok": True, "saved": sorted(kowl_capture_state.get("saved", {}).keys())})

@app.route("/api/kowl-capture/stream")
def api_kowl_capture_stream():
    def generate():
        while True:
            try:
                item = kowl_capture_state["log_queue"].get(timeout=30)
                yield f"data: {json.dumps(item)}\n\n"
                if item.get("type") == "done":
                    break
            except queue.Empty:
                yield 'data: {"type":"ping"}\n\n'
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

def resolve_data_source(golden, requested):
    """db golden -> db; kowl golden -> kowl; isd golden -> caller's choice (db/kowl)."""
    if golden == "kowl":
        return "kowl"
    if golden == "db":
        return "db"
    return "kowl" if requested == "kowl" else "db"   # isd

@app.route("/api/watch/start", methods=["POST"])
def api_watch_start():
    # Only block if a watch thread is actually still alive — a stale "running"
    # flag from a crashed/finished thread must not wedge restarts.
    t_old = watch_state.get("thread")
    if watch_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"ok": False, "error": "Already running"}), 400
    data = request.json
    golden = data.get("golden_source") or "db"
    origin = resolve_data_source(golden, data.get("data_source"))
    interval = parse_int(data.get("interval"), 3)
    subscriber = 0
    if origin == "db":
        if not secrets_ready():
            return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
        subscriber = parse_int(data.get("subscriber"))
        if subscriber is None:
            return jsonify({"ok": False, "error": "Select a flow (or enter a Subscriber ID) first"}), 400
    watch_state["mode"]   = data.get("mode", "full")
    watch_state["ext_id"] = data.get("ext_id") or None
    watch_state["source"] = golden
    watch_state["data_source"] = origin
    watch_state["running"] = True  # set before start() so a fast stop() wins the race
    t = threading.Thread(target=watch_thread_fn, args=(subscriber, interval), daemon=True)
    watch_state["thread"] = t
    t.start()
    return jsonify({"ok": True})

@app.route("/api/watch/stop", methods=["POST"])
def api_watch_stop():
    watch_state["running"] = False
    return jsonify({"ok": True})

@app.route("/api/watch/stream")
def api_watch_stream():
    def generate():
        while True:
            try:
                item = watch_state["log_queue"].get(timeout=30)
                yield f"data: {json.dumps(item)}\n\n"
                if item.get("type") == "done":
                    break
            except queue.Empty:
                yield "data: {\"type\":\"ping\"}\n\n"
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# ─── FULL RUN ROUTES (live compare across all flows) ──────────────────────────

@app.route("/api/full-run/start", methods=["POST"])
def api_full_run_start():
    t_old = full_watch_state.get("thread")
    if full_watch_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"ok": False, "error": "Full Run already running"}), 400
    data = request.json or {}
    golden = data.get("golden_source") or "db"
    origin = resolve_data_source(golden, data.get("data_source"))
    if origin == "db" and not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    interval = int(data.get("interval", 3))
    full_watch_state["mode"] = data.get("mode", "full")
    full_watch_state["source"] = golden
    full_watch_state["data_source"] = origin
    full_watch_state["log_queue"] = queue.Queue()
    full_watch_state["results"] = []
    full_watch_state["running"] = True  # set before start() so a fast stop() wins the race
    t = threading.Thread(target=full_watch_thread_fn, args=(interval,), daemon=True)
    full_watch_state["thread"] = t
    t.start()
    return jsonify({"ok": True})

@app.route("/api/full-run/stop", methods=["POST"])
def api_full_run_stop():
    full_watch_state["running"] = False
    return jsonify({"ok": True})

@app.route("/api/full-run/stream")
def api_full_run_stream():
    def generate():
        while True:
            try:
                item = full_watch_state["log_queue"].get(timeout=30)
                yield f"data: {json.dumps(item)}\n\n"
                if item.get("type") == "done":
                    break
            except queue.Empty:
                yield "data: {\"type\":\"ping\"}\n\n"
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.route("/api/golden/<path:key>")
def api_get_golden(key):
    # key may be "PUT/PUT__complete__PROCESSED" or just "PUT__complete__PROCESSED"
    path = GOLDEN_DIR / f"{key}.json"
    if not path.exists():
        return jsonify({"error": "not found"}), 404
    return jsonify(json.loads(path.read_text()))

@app.route("/api/golden/<path:key>", methods=["DELETE"])
def api_delete_golden(key):
    path = GOLDEN_DIR / f"{key}.json"
    if path.exists():
        path.unlink()
    _prune_empty_dirs(GOLDEN_DIR)
    return jsonify({"ok": True})

def _prune_empty_dirs(root):
    """Remove now-empty subdirectories under root (keeps root itself)."""
    for p in sorted(root.rglob("*"), key=lambda x: len(x.parts), reverse=True):
        if p.is_dir() and not any(p.iterdir()):
            try:
                p.rmdir()
            except OSError:
                pass

@app.route("/api/goldens/delete", methods=["POST"])
def api_goldens_delete():
    """Bulk delete goldens: by explicit keys, by folder prefix, or all."""
    data = request.get_json(force=True) or {}
    if data.get("all"):
        keys = list_goldens()
    elif data.get("prefix"):
        pref = data["prefix"].strip("/")
        keys = [k for k in list_goldens() if k == pref or k.startswith(pref + "/")]
    else:
        keys = data.get("keys") or []
    deleted = 0
    for k in keys:
        p = GOLDEN_DIR / f"{k}.json"
        if p.exists():
            p.unlink()
            deleted += 1
    _prune_empty_dirs(GOLDEN_DIR)
    return jsonify({"deleted": deleted})

# ─── TOPIC COMPARE ROUTES ─────────────────────────────────────────────────────

def _topics_from_request(data):
    """Use topics from the request if provided, else fall back to config defaults."""
    topics = data.get("topics")
    if topics:
        return [t for t in topics if t.get("topic")]
    return load_config().get("topics", [])

@app.route("/api/topics/baselines")
def api_topic_baselines():
    return jsonify(list_topic_baselines())

@app.route("/api/topics/baseline/<path:key>", methods=["DELETE"])
def api_delete_topic_baseline(key):
    p = topic_baseline_path(key)
    if p.exists():
        p.unlink()
    return jsonify({"ok": True})

@app.route("/api/topics/baseline/<path:key>")
def api_get_topic_baseline(key):
    data = load_topic_baseline(key)
    if data is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(data)

@app.route("/api/topics/capture", methods=["POST"])
def api_topics_capture():
    data  = request.get_json(force=True) or {}
    cfg   = load_config()
    host  = (data.get("host") or cfg.get("topic_host") or "").strip()
    count = int(data.get("count") or cfg.get("topic_count") or 50)
    if not host:
        return jsonify({"error": "No Kowl host configured."}), 400
    try:
        saved = capture_topics(host, _topics_from_request(data), count)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    total = sum(s["count"] for s in saved)
    return jsonify({"saved": saved, "keys": len(saved), "messages": total})

@app.route("/api/topics/compare", methods=["POST"])
def api_topics_compare():
    data  = request.get_json(force=True) or {}
    cfg   = load_config()
    host    = (data.get("host") or cfg.get("topic_host_b") or cfg.get("topic_host") or "").strip()
    count   = int(data.get("count") or cfg.get("topic_count") or 50)
    mode    = data.get("mode", "full")
    gsource = data.get("golden_source") or "kowl"   # kowl | isd | db
    if not host:
        return jsonify({"error": "No Kowl host configured."}), 400
    if gsource == "kowl" and not list_topic_baselines():
        return jsonify({"error": "No kowl baseline stored yet. Capture one in Capture → From Kowl first."}), 400
    try:
        results = compare_topics(host, _topics_from_request(data), count, mode=mode, golden_source=gsource)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"results": results})

@app.route("/api/compare/json", methods=["POST"])
def api_compare_json():
    """Diff two arbitrary JSON documents pasted/uploaded by the user."""
    data = request.get_json(force=True) or {}
    mode = data.get("mode", "full")
    raw_a, raw_b = data.get("a"), data.get("b")

    def parse(label, val):
        if isinstance(val, (dict, list)):
            return val
        try:
            return json.loads(val)
        except Exception as e:
            raise ValueError(f"{label} is not valid JSON: {e}")

    try:
        obj_a, obj_b = parse("Expected (A)", raw_a), parse("Actual (B)", raw_b)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    a, b = normalize(obj_a), normalize(obj_b)
    if data.get("ignore_dynamic"):
        a, b = strip_dynamic(a), strip_dynamic(b)

    diff = DeepDiff(a, b, ignore_order=True, verbose_level=2)
    findings = diff_to_list(diff, mode=mode)
    return jsonify({
        "status": "PASS" if not findings else "FAIL",
        "findings": findings,
        "count": len(findings),
        "payload": b,
    })

# ─── ISD / PROJECT / RUN-ALL / REPORT ROUTES ──────────────────────────────────

@app.route("/api/projects")
def api_projects():
    return jsonify({"current": current_project(), "projects": list_projects()})

@app.route("/api/golden/from-isd", methods=["POST"])
def api_golden_from_isd():
    """Upload an ISD PDF; extract sample payloads and store them as goldens."""
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded (field 'file')."}), 400
    f = request.files["file"]
    data = f.read()
    if not data:
        return jsonify({"error": "Empty file."}), 400
    try:
        parsed = parse_isd_pdf(data, filename=f.filename)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    saved = capture_isd_goldens(parsed["payloads"])
    attempts = parsed.get("attempts", 0)
    parsed_ok = len(parsed["payloads"])
    return jsonify({
        "project": current_project(),
        "isd_project_hint": parsed.get("project", ""),
        "pages": parsed.get("pages", 0),
        "blocks_seen": attempts,            # balanced {...} blocks found in the PDF
        "blocks_parsed": parsed_ok,         # of those, valid JSON after repair
        "blocks_unparseable": max(0, attempts - parsed_ok),
        "saved": saved,
        "keys": len(saved),
    })

@app.route("/api/golden/from-json", methods=["POST"])
def api_golden_from_json():
    """Save golden(s) from pasted JSON — one payload, an array, or several
    concatenated objects. Used for ISD payloads the PDF parser can't extract."""
    data = request.get_json(force=True) or {}
    raw = (data.get("text") or "").strip()
    if not raw:
        return jsonify({"error": "Paste one or more JSON payloads."}), 400
    # Try: whole thing as JSON (object or array), else scan for embedded objects.
    objs = []
    try:
        parsed = json.loads(raw)
        objs = parsed if isinstance(parsed, list) else [parsed]
    except Exception:
        found, _ = extract_json_objects(clean_isd_text(raw))
        objs = found
    if not objs:
        return jsonify({"error": "No valid JSON found. Use Beautify to spot the syntax error."}), 400
    saved = capture_isd_goldens(objs)
    if not saved:
        # Not an envelope — treat each pasted object as a raw notification payload.
        best = {}
        for o in objs:
            try:
                payload = clean_payload(o)
                key = notif_key(payload)
                if key == "UNKNOWN__unknown__UNKNOWN":
                    key = isd_golden_key(o, o.get("notification_type", ""))
                if key:
                    save_golden(key, payload, source="isd")
                    best[key] = True
            except Exception:
                continue
        saved = [{"key": k, "count": 1} for k in best]
    return jsonify({"saved": saved, "keys": len(saved), "objects": len(objs)})

@app.route("/api/run-all", methods=["POST"])
def api_run_all():
    data    = request.get_json(force=True) or {}
    since   = data.get("since")
    mode    = data.get("mode", "full")
    limit   = int(data.get("limit") or 200)
    gsource = data.get("source") or "db"     # db | isd | kowl
    cfg     = load_config()

    try:
        if gsource == "kowl":
            # Execute-all for Kowl = diff every configured topic against the kowl baseline
            if not list_topic_baselines():
                return jsonify({"error": "No kowl baseline stored. Capture one in Capture → From Kowl first."}), 400
            host  = (cfg.get("topic_host_b") or cfg.get("topic_host") or "").strip()
            count = int(cfg.get("topic_count") or 50)
            if not host:
                return jsonify({"error": "No Kowl host configured (Config tab)."}), 400
            results = compare_topics(host, cfg.get("topics", []), count, mode=mode)
            per_flow = {}
            for r in results:
                label = (r.get("key") or "?").split("__")[0]
                d = per_flow.setdefault(label, {"total": 0, "pass": 0, "fail": 0})
                d["total"] += 1
                if r["status"] == "PASS": d["pass"] += 1
                elif r["status"] == "FAIL": d["fail"] += 1
        else:
            if not secrets_ready():
                return jsonify({"error": "DB secrets not set. Enter them on the Config tab first."}), 400
            results, per_flow = run_all_db_flows(since=since, limit=limit, mode=mode, source=gsource)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    meta = {
        "Project": current_project() or "(none)",
        "Golden source": gsource,
        "Mode": mode,
        "Run at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Flows": ", ".join(f"{k}({v['pass']}/{v['total']})" for k, v in per_flow.items()) or "none configured",
    }
    report_name = save_report(build_html_report(results, "Collective Notification Report", meta),
                              prefix="run_all")
    save_report_meta(report_name, results, project=current_project(), mode=mode,
                     per_flow=per_flow, kind="run_all")
    return jsonify({"results": results, "per_flow": per_flow, "report": report_name})

@app.route("/api/reports")
def api_reports():
    return jsonify(list_reports_meta())

@app.route("/api/report/<path:name>")
def api_get_report(name):
    p = REPORTS_DIR / name
    if not p.exists() or p.suffix != ".html":
        return jsonify({"error": "not found"}), 404
    download = request.args.get("download") == "1"
    return Response(
        p.read_text(),
        mimetype="text/html",
        headers={"Content-Disposition": f'attachment; filename="{name}"'} if download else {},
    )

@app.route("/api/allure/status")
def api_allure_status():
    """Report whether the allure CLI (and a JRE) are available for HTML generation."""
    allure = shutil.which("allure")
    java = shutil.which("java")
    return jsonify({
        "cli": bool(allure),
        "java": bool(java),
        "html_capable": bool(allure and java),
    })

@app.route("/api/allure/<path:name>")
def api_get_allure_zip(name):
    """Download the allure-results .zip produced by a Full Run."""
    p = REPORTS_DIR / name
    if not p.exists() or p.suffix != ".zip":
        return jsonify({"error": "not found"}), 404
    return Response(p.read_bytes(), mimetype="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{name}"'})

@app.route("/api/allure-html/<run_id>/")
@app.route("/api/allure-html/<run_id>/<path:sub>")
def api_get_allure_html(run_id, sub="index.html"):
    """Serve a generated Allure HTML report (only present if the allure CLI was installed)."""
    base = (ALLURE_DIR / f"{run_id}-html").resolve()
    target = (base / sub).resolve()
    if base not in target.parents and target != base or not target.exists():
        return jsonify({"error": "not found"}), 404
    mime = ("text/html" if target.suffix == ".html" else
            "application/javascript" if target.suffix == ".js" else
            "text/css" if target.suffix == ".css" else
            "application/json" if target.suffix == ".json" else "application/octet-stream")
    return Response(target.read_bytes(), mimetype=mime)

def _delete_report(name):
    p = REPORTS_DIR / name
    if p.suffix != ".html" or not p.exists():
        return False
    sidecar = REPORTS_DIR / f"{name}.meta.json"
    # Clean up associated Allure artifacts (zip in reports/, html dir + results in allure-results/).
    if sidecar.exists():
        try:
            meta = json.loads(sidecar.read_text())
            if meta.get("allure_zip"):
                (REPORTS_DIR / meta["allure_zip"]).unlink(missing_ok=True)
                run_id = meta["allure_zip"].replace("allure_", "").replace(".zip", "")
                shutil.rmtree(ALLURE_DIR / run_id, ignore_errors=True)
            if meta.get("allure_html"):
                shutil.rmtree(ALLURE_DIR / meta["allure_html"], ignore_errors=True)
        except Exception:
            pass
        sidecar.unlink()
    p.unlink()
    return True

@app.route("/api/report/<path:name>", methods=["DELETE"])
def api_delete_report(name):
    return jsonify({"ok": _delete_report(name)})

@app.route("/api/reports/delete", methods=["POST"])
def api_reports_delete():
    """Bulk delete reports: by explicit names or all."""
    data = request.get_json(force=True) or {}
    if data.get("all"):
        names = [p.name for p in REPORTS_DIR.glob("*.html")]
    else:
        names = data.get("names") or []
    deleted = sum(1 for n in names if _delete_report(n))
    return jsonify({"deleted": deleted})

# ─── HTML UI ──────────────────────────────────────────────────────────────────

HTML_UI = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Notification Comparator</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #0f1117; color: #e2e8f0; min-height: 100vh; }

  /* Layout */
  .sidebar { width: 260px; background: #1a1d27; border-right: 1px solid #2d3148;
             position: fixed; top: 0; left: 0; height: 100vh; overflow-y: auto; padding: 20px 0; }
  .main { margin-left: 260px; padding: 28px 32px; }

  /* Sidebar nav */
  .logo { padding: 0 20px 20px; border-bottom: 1px solid #2d3148; margin-bottom: 16px; }
  .logo h1 { font-size: 15px; font-weight: 700; color: #818cf8; }
  .logo p  { font-size: 11px; color: #64748b; margin-top: 3px; }
  .nav-item { display: flex; align-items: center; gap: 10px; padding: 10px 20px;
              cursor: pointer; font-size: 13px; color: #94a3b8; transition: all .15s;
              border-left: 3px solid transparent; }
  .nav-item:hover { background: #23273a; color: #e2e8f0; }
  .nav-item.active { background: #23273a; color: #818cf8; border-left-color: #818cf8; }
  .nav-icon { font-size: 16px; }
  .nav-label { font-weight: 500; }

  /* Cards */
  .card { background: #1a1d27; border: 1px solid #2d3148; border-radius: 10px; padding: 24px; margin-bottom: 20px; }
  .card-title { font-size: 14px; font-weight: 600; color: #818cf8; margin-bottom: 16px;
                display: flex; align-items: center; gap: 8px; }

  /* Form elements */
  label { font-size: 12px; color: #94a3b8; display: block; margin-bottom: 5px; margin-top: 12px; }
  label:first-child { margin-top: 0; }
  input[type=text], input[type=number], input[type=datetime-local] {
    width: 100%; background: #0f1117; border: 1px solid #2d3148; border-radius: 6px;
    padding: 8px 12px; color: #e2e8f0; font-size: 13px; outline: none; }
  input:focus { border-color: #818cf8; }

  /* Buttons */
  .btn { display: inline-flex; align-items: center; gap: 6px; padding: 9px 18px;
         border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer;
         border: none; transition: all .15s; }
  .btn-primary { background: #4f46e5; color: white; }
  .btn-primary:hover { background: #6366f1; }
  .btn-success { background: #16a34a; color: white; }
  .btn-success:hover { background: #22c55e; }
  .btn-danger  { background: #dc2626; color: white; }
  .btn-danger:hover  { background: #ef4444; }
  .btn-ghost   { background: #23273a; color: #94a3b8; border: 1px solid #2d3148; }
  .btn-ghost:hover { color: #e2e8f0; }
  .btn:disabled { opacity: .4; cursor: not-allowed; }

  /* Badges */
  .badge { display: inline-block; padding: 2px 8px; border-radius: 20px; font-size: 11px; font-weight: 700; }
  .badge-pass { background: #14532d; color: #86efac; }
  .badge-fail { background: #7f1d1d; color: #fca5a5; }
  .badge-warn { background: #78350f; color: #fcd34d; }
  .badge-error { background: #581c87; color: #d8b4fe; }
  .badge-info  { background: #1e3a5f; color: #93c5fd; }

  /* Results table */
  .results-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .results-table th { text-align: left; padding: 10px 12px; background: #0f1117;
                      color: #64748b; font-weight: 600; font-size: 11px; text-transform: uppercase;
                      border-bottom: 1px solid #2d3148; }
  .results-table td { padding: 10px 12px; border-bottom: 1px solid #1e2235; vertical-align: top; }
  .results-table tr:hover td { background: #1e2235; }
  .results-table tr.expandable { cursor: pointer; }
  .diff-detail { background: #0d1017; border-radius: 6px; padding: 12px; margin-top: 8px; display: none; }
  .diff-row { display: grid; grid-template-columns: 140px 1fr 1fr; gap: 8px;
              padding: 5px 0; border-bottom: 1px solid #1e2235; font-size: 12px; }
  .diff-row:last-child { border-bottom: none; }
  .diff-type { color: #818cf8; font-weight: 600; }
  .diff-path { color: #94a3b8; font-family: monospace; }
  .diff-detail-text { color: #fbbf24; font-family: monospace; }
  .diff-row-fail { border-left: 3px solid #dc2626; padding-left: 8px; }
  .payload-json { background: #0a0d13; border: 1px solid #1e2235; border-radius: 6px;
                  padding: 12px; margin: 0; max-height: 420px; overflow: auto;
                  font-family: monospace; font-size: 11px; line-height: 1.5;
                  color: #94a3b8; white-space: pre; }
  /* Green = matches golden, Red = schema mismatch */
  .payload-json .jl-ok      { display: block; color: #86efac; }
  .payload-json .jl-neutral { display: block; color: #94a3b8; }
  .payload-json .jl-bad     { display: block; color: #fca5a5; background: #450a0a;
                              border-left: 3px solid #dc2626; margin-left: -12px; padding-left: 9px; }
  .no-results { text-align: center; padding: 40px; color: #4b5563; font-size: 14px; }

  /* Log stream */
  .log-box { background: #0d1017; border-radius: 8px; padding: 14px; height: 320px;
             overflow-y: auto; font-family: monospace; font-size: 12px; }
  .log-line { padding: 3px 0; border-bottom: 1px solid #1e2235; }
  .log-pass { color: #86efac; }
  .log-fail { color: #fca5a5; }
  .log-no_golden { color: #fcd34d; }
  .log-error { color: #f87171; }
  .log-info  { color: #93c5fd; }
  .log-done  { color: #818cf8; }

  /* Golden list */
  .golden-item { display: flex; align-items: center; justify-content: space-between;
                 padding: 8px 12px; background: #0f1117; border-radius: 6px;
                 margin-bottom: 6px; font-size: 12px; font-family: monospace; }
  .golden-item:hover { background: #1e2235; }
  .golden-name { color: #a5b4fc; }
  .golden-actions { display: flex; gap: 6px; }
  .btn-xs { padding: 3px 8px; font-size: 11px; border-radius: 4px; cursor: pointer;
            border: none; font-weight: 600; }
  .btn-xs-view { background: #23273a; color: #94a3b8; }
  .btn-xs-del  { background: #450a0a; color: #fca5a5; }

  /* Golden tree (collapsible) */
  .tree-group { margin: 4px 0; }
  .tree-group > summary { cursor: pointer; padding: 7px 10px; border-radius: 6px;
                          background: #0f1117; font-size: 13px; color: #e2e8f0;
                          list-style: none; user-select: none; border: 1px solid #1e2235; }
  .tree-group > summary::-webkit-details-marker { display: none; }
  .tree-group > summary::before { content: '▸'; color: #64748b; margin-right: 8px; display: inline-block; transition: transform .15s; }
  .tree-group[open] > summary::before { transform: rotate(90deg); }
  .tree-group > summary:hover { background: #1e2235; }
  .tree-count { float: right; background: #23273a; color: #94a3b8; border-radius: 10px;
                padding: 0 8px; font-size: 11px; font-weight: 600; }
  .tree-children { margin-left: 18px; padding-left: 10px; border-left: 1px solid #2d3148; margin-top: 4px; }
  .tree-leaf { margin: 4px 0; }
  .tree-leaf-row { display: flex; align-items: center; justify-content: space-between;
                   padding: 6px 10px; background: #0f1117; border-radius: 6px; }
  .tree-leaf-row:hover { background: #1e2235; }
  .tree-leaf-name { font-family: monospace; font-size: 12px; color: #a5b4fc; }
  .tree-json { margin: 4px 0 8px 14px; }
  .g-check { display: none; margin-right: 6px; vertical-align: middle; accent-color: #818cf8; }
  #goldens-list.select-on .g-check { display: inline-block; }
  .folder-del { float: right; margin-right: 8px; cursor: pointer; opacity: .45; font-size: 12px; }
  .folder-del:hover { opacity: 1; }
  .rep-check { margin-right: 8px; accent-color: #818cf8; vertical-align: middle; }

  /* Modal */
  .modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,.7); z-index: 100;
                   display: none; align-items: center; justify-content: center; }
  .modal-overlay.open { display: flex; }
  .modal { background: #1a1d27; border: 1px solid #2d3148; border-radius: 12px;
           width: 800px; max-width: 95vw; max-height: 80vh; overflow: hidden;
           display: flex; flex-direction: column; }
  .modal-header { padding: 16px 20px; border-bottom: 1px solid #2d3148;
                  display: flex; justify-content: space-between; align-items: center; }
  .modal-header h3 { font-size: 14px; font-weight: 600; color: #818cf8; }
  .modal-body { padding: 16px 20px; overflow-y: auto; }
  .modal-close { background: none; border: none; color: #64748b; cursor: pointer;
                 font-size: 20px; line-height: 1; }

  /* Summary strip */
  .summary-strip { display: flex; gap: 12px; margin-bottom: 20px; }
  .summary-card { flex: 1; background: #1a1d27; border: 1px solid #2d3148; border-radius: 8px;
                  padding: 14px 16px; text-align: center; }
  .summary-card .num { font-size: 28px; font-weight: 700; }
  .summary-card .lbl { font-size: 11px; color: #64748b; margin-top: 2px; }
  .num-pass { color: #86efac; }
  .num-fail { color: #fca5a5; }
  .num-warn { color: #fcd34d; }
  /* Donut chart (pure CSS conic-gradient) */
  .donut { width: 150px; height: 150px; border-radius: 50%;
           background: conic-gradient(#2d3148 0 100%); position: relative; flex-shrink: 0; }
  .donut-hole { position: absolute; inset: 26px; background: #1a1d27; border-radius: 50%;
                display: flex; flex-direction: column; align-items: center; justify-content: center; }
  .donut-hole span { font-size: 26px; font-weight: 700; color: #e2e8f0; }
  .donut-hole small { font-size: 11px; color: #64748b; }
  .dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; }
  .num-total{ color: #93c5fd; }

  /* Tabs inside page */
  .tabs { display: flex; gap: 0; border-bottom: 1px solid #2d3148; margin-bottom: 20px; }
  .tab  { padding: 10px 18px; font-size: 13px; cursor: pointer; color: #64748b;
          border-bottom: 2px solid transparent; margin-bottom: -1px; }
  .tab.active { color: #818cf8; border-bottom-color: #818cf8; font-weight: 600; }

  /* Page visibility */
  .page { display: none; }
  .page.active { display: block; }

  /* Spinner */
  @keyframes spin { to { transform: rotate(360deg); } }
  .spinner { width: 14px; height: 14px; border: 2px solid #4f46e5;
             border-top-color: transparent; border-radius: 50%;
             animation: spin .6s linear infinite; display: inline-block; }

  /* Pulse dot */
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
  .pulse { width: 8px; height: 8px; border-radius: 50%; background: #22c55e;
           animation: pulse 1.5s ease-in-out infinite; display: inline-block; }

  pre { white-space: pre-wrap; word-break: break-all; font-size: 12px; color: #94a3b8; }
  .mt { margin-top: 12px; }
  .flex-row { display: flex; gap: 12px; align-items: flex-end; }
  .flex-row > * { flex: 1; }
  .flex-row > .btn { flex: none; }
  h2 { font-size: 18px; font-weight: 700; margin-bottom: 4px; }
  .subtitle { font-size: 13px; color: #64748b; margin-bottom: 24px; }

  /* Toggle switch */
  .toggle-wrap { width: 44px; height: 24px; background: #2d3148; border-radius: 12px;
                 position: relative; cursor: pointer; transition: background .2s; flex-shrink:0; }
  .toggle-wrap.on { background: #4f46e5; }
  .toggle-knob { width: 18px; height: 18px; background: #fff; border-radius: 50%;
                 position: absolute; top: 3px; left: 3px; transition: left .2s; }
  .toggle-wrap.on .toggle-knob { left: 23px; }
  .toggle-wrap.disabled { opacity: .45; cursor: not-allowed; }

  /* Flow type pill selector */
  .flow-pills { display: flex; gap: 6px; flex-wrap: nowrap; }
  .flow-pill {
    padding: 7px 16px; border-radius: 20px; font-size: 12px; font-weight: 600;
    cursor: pointer; border: 1px solid #2d3148; background: #0f1117; color: #64748b;
    transition: all .15s; user-select: none; white-space: nowrap; letter-spacing: .3px;
  }
  .flow-pill:hover { color: #c4b5fd; border-color: #4f46e5; }
  .flow-pill.active                { color: #fff; }
  .flow-pill.active.pill-put       { background: #4338ca; border-color: #4338ca; box-shadow: 0 0 0 2px #4338ca44; }
  .flow-pill.active.pill-pick      { background: #0e7490; border-color: #0e7490; box-shadow: 0 0 0 2px #0e749044; }
  .flow-pill.active.pill-audit     { background: #b45309; border-color: #b45309; box-shadow: 0 0 0 2px #b4530944; }
  .flow-pill.active.pill-other     { background: #7c3aed; border-color: #7c3aed; box-shadow: 0 0 0 2px #7c3aed44; }
  /* subscriber row layout */
  .flow-sub-row { display: flex; align-items: flex-end; gap: 14px; flex-wrap: wrap; }
  .flow-sub-row .sub-input-wrap { width: 140px; flex-shrink: 0; }
</style>
</head>
<body>

<!-- Sidebar -->
<nav class="sidebar">
  <div class="logo">
    <h1>🔔 Notif Comparator</h1>
    <p>WMS Notification Validator</p>
  </div>
  <div class="nav-item active" onclick="showPage('dashboard')">
    <span class="nav-icon">🏠</span><span class="nav-label">Dashboard</span>
  </div>
  <div class="nav-item" onclick="showPage('capture')">
    <span class="nav-icon">📸</span><span class="nav-label">Capture Golden</span>
  </div>
  <div class="nav-item" onclick="showPage('compare')">
    <span class="nav-icon">🔍</span><span class="nav-label">Compare</span>
  </div>
  <div class="nav-item" onclick="showPage('watch')">
    <span class="nav-icon">👁</span><span class="nav-label">Watch (Live)</span>
  </div>
  <div class="nav-item" onclick="showPage('goldens')">
    <span class="nav-icon">🗂</span><span class="nav-label">Golden Snapshots</span>
  </div>
  <div class="nav-item" onclick="showPage('config')">
    <span class="nav-icon">⚙️</span><span class="nav-label">Config</span>
  </div>
</nav>

<!-- Main -->
<main class="main">

  <!-- DASHBOARD -->
  <div class="page active" id="page-dashboard">
    <h2>Dashboard</h2>
    <p class="subtitle">WMS Notification Schema Comparator</p>

    <!-- FULL RUN (live, all flows) -->
    <div class="card mt">
      <div class="card-title">▶ Full Run (Live)</div>
      <p style="font-size:13px;color:#64748b;margin-bottom:14px">Click <b>Start</b>, then run your whole automation (PUT / PICK / AUDIT). Every notification across <b>all configured flows</b> is compared live against the chosen golden source. Click <b>Stop</b> to finish — an HTML report <b>and an Allure report</b> are generated and saved under <b>Past Reports</b>.</p>
      <div id="allure-cli-warn" style="display:none;background:#78350f;color:#fcd34d;border:1px solid #b45309;border-radius:6px;padding:10px 12px;font-size:12px;margin-bottom:14px">
        ⚠️ Allure CLI not detected on the server — Full Run will still produce a downloadable <b>allure-results .zip</b>, but no HTML report.
        Install it to auto-generate HTML: <code>brew install allure</code> (needs Java) or <code>npm i -g allure-commandline</code>.
      </div>
      <label style="margin-bottom:6px">Compare against golden source</label>
      <div class="flow-pills" style="margin-bottom:8px">
        <div class="flow-pill active pill-put" id="fullrun-gs-db"   onclick="setFullRunGolden('db')">🗄 DB</div>
        <div class="flow-pill pill-other"      id="fullrun-gs-isd"  onclick="setFullRunGolden('isd')">📄 ISD</div>
        <div class="flow-pill pill-pick"       id="fullrun-gs-kowl" onclick="setFullRunGolden('kowl')">🧬 Kowl</div>
      </div>
      <div id="fullrun-isd-data" class="flow-pills" style="margin-bottom:14px;display:none">
        <span style="font-size:11px;color:#64748b;margin-right:2px">ISD golden vs live data from:</span>
        <div class="flow-pill active pill-put" id="fullrun-isd-db"   onclick="setFullRunIsdData('db')">🗄 DB</div>
        <div class="flow-pill pill-pick"       id="fullrun-isd-kowl" onclick="setFullRunIsdData('kowl')">🧬 Kowl</div>
      </div>
      <div class="flex-row">
        <div>
          <label>Poll Interval (seconds)</label>
          <input type="number" id="fullrun-interval" value="3" min="1" style="width:120px">
        </div>
        <button class="btn btn-success" onclick="startFullRun()" id="fullrun-start-btn">▶ Start Full Run</button>
        <button class="btn btn-danger" onclick="stopFullRun()" id="fullrun-stop-btn" disabled>⏹ Stop</button>
      </div>
      <div style="margin-top:14px">
        <label style="margin:0;display:flex;align-items:center;gap:10px;cursor:pointer;width:fit-content">
          <div class="toggle-wrap" id="fullrun-mode-wrap" onclick="toggleMode('fullrun')" title="Toggle comparison mode">
            <div class="toggle-knob" id="fullrun-mode-knob"></div>
          </div>
          <span id="fullrun-mode-label" style="font-size:13px;color:#94a3b8">Full Compare</span>
        </label>
        <div id="fullrun-mode-hint" style="font-size:11px;color:#4b5563;margin-top:4px">Compares keys, values, and types</div>
      </div>
      <div style="margin-top:12px;display:flex;align-items:center;gap:8px;font-size:12px;color:#64748b">
        <span id="fullrun-status-dot"></span>
        <span id="fullrun-status-text">Idle</span>
      </div>
    </div>

    <div id="fullrun-summary" style="display:none">
      <div class="summary-strip">
        <div class="summary-card"><div class="num num-total" id="fr-total">0</div><div class="lbl">Total</div></div>
        <div class="summary-card"><div class="num num-pass"  id="fr-pass">0</div><div class="lbl">Pass ✅</div></div>
        <div class="summary-card"><div class="num num-fail"  id="fr-fail">0</div><div class="lbl">Fail ❌</div></div>
        <div class="summary-card"><div class="num num-warn"  id="fr-nogolden">0</div><div class="lbl">No Golden ⚠️</div></div>
      </div>
    </div>

    <div class="card" id="fullrun-chart-card" style="display:none">
      <div class="card-title">📊 Pass / Fail Breakdown</div>
      <div style="display:flex;align-items:center;gap:32px;flex-wrap:wrap">
        <div class="donut" id="fr-donut">
          <div class="donut-hole"><span id="fr-donut-pct">0%</span><small>pass</small></div>
        </div>
        <div style="font-size:13px">
          <div style="margin:6px 0"><span class="dot" style="background:#22c55e"></span> Pass <b id="fr-leg-pass">0</b></div>
          <div style="margin:6px 0"><span class="dot" style="background:#ef4444"></span> Fail <b id="fr-leg-fail">0</b></div>
          <div style="margin:6px 0"><span class="dot" style="background:#eab308"></span> Other <b id="fr-leg-other">0</b></div>
        </div>
      </div>
    </div>

    <div class="card" id="fullrun-allure-card" style="display:none">
      <div class="card-title">📦 Allure Report</div>
      <div id="fullrun-allure-body" style="font-size:13px;color:#94a3b8"></div>
    </div>

    <div class="card" id="fullrun-log-card" style="display:none">
      <div class="card-title">Live Log</div>
      <div class="log-box" id="fullrun-log"></div>
    </div>
    <div class="card" id="fullrun-results-card" style="display:none">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <div class="card-title" style="margin:0">Results</div>
        <a id="fullrun-report-dl" class="btn btn-success" href="#" download style="display:none">⬇ Download HTML Report</a>
      </div>
      <table class="results-table">
        <thead><tr><th>DB ID</th><th>Flow / Time</th><th>Flow Request ID</th><th>Notification Key</th><th>Status</th><th>Diffs</th></tr></thead>
        <tbody id="fullrun-results-body"></tbody>
      </table>
    </div>

    <div class="card mt">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:8px">
        <div class="card-title" style="margin:0">🗒 Past Reports</div>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <button class="btn btn-danger" onclick="deleteSelectedReports()">🗑 Delete Selected (<span id="rep-sel-count">0</span>)</button>
          <button class="btn btn-danger" onclick="deleteAllReports()">🗑 Delete All</button>
          <button class="btn btn-ghost" onclick="loadReports()">↻ Refresh</button>
        </div>
      </div>
      <div id="dash-reports" style="color:#64748b;font-size:13px">No reports yet.</div>
    </div>
  </div>

  <!-- CAPTURE -->
  <div class="page" id="page-capture">
    <h2>📸 Capture Golden Snapshots</h2>
    <p class="subtitle">Save notifications as the expected baseline. Project: <b id="cap-project-label" style="color:#a5b4fc">(none)</b></p>

    <div class="flow-pills" style="margin-bottom:18px">
      <div class="flow-pill active pill-put"  id="cap-src-tab-db"   onclick="switchCapSource('db')">🗄 From DB</div>
      <div class="flow-pill pill-pick"         id="cap-src-tab-kowl" onclick="switchCapSource('kowl')">🧬 From Kowl</div>
      <div class="flow-pill pill-other"        id="cap-src-tab-isd"  onclick="switchCapSource('isd')">📄 From ISD</div>
    </div>

    <!-- SOURCE: DB -->
    <div id="cap-src-db">
    <div class="tabs">
      <div class="tab active" id="cap-tab-time" onclick="switchCapTab('time')">📅 By Time Range</div>
      <div class="tab" id="cap-tab-live" onclick="switchCapTab('live')">⚡ Live Poll &amp; Capture</div>
    </div>

    <!-- Tab 1: By Time/ExtID -->
    <div id="cap-panel-time">
      <div class="card">
        <div class="card-title">Fetch notifications and save as golden</div>

        <div class="tabs" style="margin-bottom:14px;border-bottom:1px solid #2d3148">
          <div class="tab active" id="cap-fetch-tab-time"  onclick="switchCapFetchTab('time')">📅 By Time</div>
          <div class="tab"        id="cap-fetch-tab-extid" onclick="switchCapFetchTab('extid')">🔖 By Request ID</div>
        </div>

        <div class="flow-sub-row">
          <div>
            <label>Flow Type</label>
            <div class="flow-pills" id="cap-flow-pills">
              <div class="flow-pill pill-put"   data-flow="PUT"   onclick="selectFlowType('cap','PUT')">PUT</div>
              <div class="flow-pill pill-pick"  data-flow="PICK"  onclick="selectFlowType('cap','PICK')">PICK</div>
              <div class="flow-pill pill-audit" data-flow="AUDIT" onclick="selectFlowType('cap','AUDIT')">AUDIT</div>
              <div class="flow-pill pill-other" data-flow="OTHER" onclick="selectFlowType('cap','OTHER')">Other</div>
            </div>
          </div>
          <div class="sub-input-wrap">
            <label>Subscriber ID</label>
            <input type="number" id="cap-subscriber" placeholder="auto-filled">
          </div>
        </div>

        <div id="cap-fetch-panel-time" style="margin-top:12px">
          <label>Since (optional — leave blank for last 100)</label>
          <div style="display:flex;gap:8px;align-items:center">
            <input type="datetime-local" id="cap-since" style="flex:1">
            <button class="btn btn-ghost" style="padding:7px 12px;font-size:12px" onclick="document.getElementById('cap-since').value=''">✕ Clear</button>
          </div>
        </div>

        <div id="cap-fetch-panel-extid" style="margin-top:12px;display:none">
          <label>External Request ID (externalServiceRequestId)</label>
          <input type="text" id="cap-extid" placeholder="e.g. rohit_ext_09">
        </div>

        <div class="mt">
          <button class="btn btn-success" onclick="doCapture()" id="cap-btn">📸 Capture</button>
        </div>
      </div>
      <div class="card" id="cap-result" style="display:none">
        <div class="card-title">Result</div>
        <div id="cap-result-body"></div>
      </div>
    </div>

    <!-- Tab 2: Live Poll -->
    <div id="cap-panel-live" style="display:none">
      <div class="card">
        <div class="card-title">Poll DB live — click Stop when your flow finishes to save all captured notifications as golden</div>

        <div class="tabs" style="margin-bottom:14px;border-bottom:1px solid #2d3148">
          <div class="tab active" id="cap-live-fetch-tab-time"  onclick="switchCapLiveFetchTab('time')">📅 By Time</div>
          <div class="tab"        id="cap-live-fetch-tab-extid" onclick="switchCapLiveFetchTab('extid')">🔖 By Request ID</div>
        </div>

        <div style="margin-bottom:14px">
          <label>Flow Type</label>
          <div class="flow-pills" id="cap-live-flow-pills">
            <div class="flow-pill pill-put"   data-flow="PUT"   onclick="selectFlowType('cap-live','PUT')">PUT</div>
            <div class="flow-pill pill-pick"  data-flow="PICK"  onclick="selectFlowType('cap-live','PICK')">PICK</div>
            <div class="flow-pill pill-audit" data-flow="AUDIT" onclick="selectFlowType('cap-live','AUDIT')">AUDIT</div>
            <div class="flow-pill pill-other" data-flow="OTHER" onclick="selectFlowType('cap-live','OTHER')">Other</div>
          </div>
        </div>
        <div class="flex-row">
          <div class="sub-input-wrap">
            <label>Subscriber ID</label>
            <input type="number" id="cap-live-subscriber" placeholder="auto-filled">
          </div>
          <div>
            <label>Poll Interval (seconds)</label>
            <input type="number" id="cap-live-interval" value="3" min="1">
          </div>
          <button class="btn btn-success" onclick="startLiveCapture()" id="cap-live-start-btn">▶ Fetch Now</button>
          <button class="btn btn-danger"  onclick="stopLiveCapture()"  id="cap-live-stop-btn" disabled>⏹ Stop &amp; Save</button>
        </div>

        <div id="cap-live-fetch-panel-extid" style="margin-top:12px;display:none">
          <label>External Request ID (externalServiceRequestId)</label>
          <input type="text" id="cap-live-extid" placeholder="e.g. rohit_ext_09">
        </div>

        <div style="margin-top:10px;display:flex;align-items:center;gap:8px;font-size:12px;color:#64748b">
          <span id="cap-live-dot"></span>
          <span id="cap-live-status">Idle — trigger your flow after clicking Fetch Now</span>
        </div>
      </div>
      <div class="card">
        <div class="card-title">Live Log</div>
        <div class="log-box" id="cap-live-log"></div>
      </div>
      <div class="card" id="cap-live-result" style="display:none">
        <div class="card-title">Saved Goldens</div>
        <div id="cap-live-result-body"></div>
      </div>
    </div>
    </div><!-- /cap-src-db -->

    <!-- SOURCE: KOWL -->
    <div id="cap-src-kowl" style="display:none">
      <div class="card">
        <div class="card-title">Capture golden baseline from Kowl topics</div>
        <p style="font-size:13px;color:#64748b;margin-bottom:12px">Pull recent messages from the Kowl topics and store them as golden under
          <b id="cap-kowl-project" style="color:#a5b4fc">project</b> → <code>kowl/</code>.</p>
        <div class="flow-sub-row">
          <div style="flex:1;min-width:220px">
            <label>Kowl host:port</label>
            <input type="text" id="tc-cap-host" placeholder="172.29.32.39:9003">
          </div>
          <div class="sub-input-wrap">
            <label>Recent N</label>
            <input type="number" id="tc-cap-count" placeholder="50">
          </div>
        </div>
        <label>Topics</label>
        <div id="tc-cap-topics" style="font-size:12px;font-family:monospace;color:#a5b4fc"></div>
        <div class="mt">
          <button class="btn btn-success" onclick="captureTopics()" id="tc-cap-btn">📥 Capture Baseline (snapshot)</button>
        </div>
      </div>

      <div class="card">
        <div class="card-title">⚡ Live Capture</div>
        <p style="font-size:13px;color:#64748b;margin-bottom:12px">Click <b>Start</b>, then run your flow. Every new Kowl topic message is saved as kowl golden (one per state/key, latest wins) until you click <b>Stop</b>.</p>
        <div class="flex-row" style="align-items:flex-end">
          <div class="sub-input-wrap">
            <label>Poll Interval (seconds)</label>
            <input type="number" id="kc-interval" value="3" min="1">
          </div>
          <button class="btn btn-success" onclick="startKowlCapture()" id="kc-start-btn">▶ Start Live Capture</button>
          <button class="btn btn-danger" onclick="stopKowlCapture()" id="kc-stop-btn" disabled>⏹ Stop</button>
        </div>
        <div style="margin-top:10px;display:flex;align-items:center;gap:8px;font-size:12px;color:#64748b">
          <span id="kc-dot"></span><span id="kc-status">Idle</span>
        </div>
        <div class="log-box" id="kc-log" style="display:none;margin-top:12px"></div>
      </div>

      <div class="card" id="tc-cap-result" style="display:none">
        <div class="card-title">Stored baseline keys</div>
        <div id="tc-cap-result-body"></div>
      </div>
      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <div class="card-title" style="margin:0">Current baseline (<span id="tc-baseline-count">0</span> keys)</div>
          <button class="btn btn-ghost" onclick="loadTopicBaselines()">↻ Refresh</button>
        </div>
        <div id="tc-baseline-list"><div class="no-results">No baseline captured yet.</div></div>
      </div>
    </div>

    <!-- SOURCE: ISD -->
    <div id="cap-src-isd" style="display:none">
      <div class="card">
        <div class="card-title">📄 Generate golden from an ISD document (PDF)</div>
        <p style="font-size:13px;color:#64748b;margin-bottom:14px">
          Upload the Interface Specification Document. Sample notification payloads found in the PDF are
          extracted and saved as golden data under the current project
          (<b id="cap-isd-project" style="color:#a5b4fc">none</b>).</p>
        <label>ISD PDF file</label>
        <input type="file" id="cap-isd-file" accept="application/pdf"
               style="width:100%;background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:8px 12px;color:#e2e8f0;font-size:13px">
        <div class="mt">
          <button class="btn btn-success" onclick="uploadISD()" id="cap-isd-btn">📄 Read ISD &amp; Capture Golden</button>
        </div>
      </div>
      <div class="card" id="cap-isd-result" style="display:none">
        <div class="card-title">ISD extraction result</div>
        <div id="cap-isd-result-body"></div>
      </div>

      <div class="card">
        <div class="card-title">➕ Paste payload(s) as golden</div>
        <p style="font-size:13px;color:#64748b;margin-bottom:10px">Some ISD payloads (large PICK/audit samples) can't be auto-extracted because the PDF's JSON is malformed (smart quotes, line-wrapped tokens). Copy a payload from the doc, paste it here, click <b>✨ Beautify</b> to validate/fix, then save. Accepts one object, an array, or several pasted objects.</p>
        <textarea id="isd-paste" rows="10" spellcheck="false"
          style="width:100%;background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:10px;color:#e2e8f0;font-size:12px;font-family:monospace" placeholder='Paste an ISD notification payload (the whole { "id": ..., "payload": { ... } } block)'></textarea>
        <div class="mt" style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
          <button class="btn-xs btn-xs-view" onclick="beautifyJson('isd-paste')">✨ Beautify</button>
          <button class="btn btn-success" id="isd-paste-btn" onclick="saveIsdPaste()">💾 Save pasted as golden</button>
          <span id="isd-paste-status" style="font-size:12px"></span>
        </div>
        <div id="isd-paste-result" style="margin-top:10px"></div>
      </div>
    </div>
  </div>

  <!-- COMPARE -->
  <div class="page" id="page-compare">
    <h2>🔍 Compare</h2>
    <p class="subtitle">Diff notifications against a golden source, or directly diff two JSON documents.</p>

    <div class="flow-pills" style="margin-bottom:16px;align-items:center">
      <span style="font-size:11px;color:#64748b;margin-right:2px">Golden compare:</span>
      <div class="flow-pill active pill-put" id="cmp-gs-db"   onclick="setCompareGolden('db')">🗄 DB</div>
      <div class="flow-pill pill-other"      id="cmp-gs-isd"  onclick="setCompareGolden('isd')">📄 ISD</div>
      <div class="flow-pill pill-pick"       id="cmp-gs-kowl" onclick="setCompareGolden('kowl')">🧬 Kowl</div>
      <span style="display:inline-block;width:1px;height:22px;background:#2d3148;margin:0 10px"></span>
      <div class="flow-pill pill-audit"      id="cmp-gs-json" onclick="setCompareGolden('json')">🧩 Direct JSON</div>
    </div>

    <!-- ISD golden can validate either DB-fetched or Kowl-fetched live data -->
    <div id="cmp-isd-data" class="flow-pills" style="margin-bottom:16px;align-items:center;display:none">
      <span style="font-size:11px;color:#64748b;margin-right:2px">ISD golden vs live data from:</span>
      <div class="flow-pill active pill-put" id="cmp-isd-db"   onclick="setIsdDataSource('db')">🗄 DB</div>
      <div class="flow-pill pill-pick"       id="cmp-isd-kowl" onclick="setIsdDataSource('kowl')">🧬 Kowl</div>
    </div>

    <div class="tabs" id="cmp-tabs-row">
      <div class="tab active" id="cmp-tab-time"  onclick="switchCmpTab('time')">📅 By Time Range</div>
      <div class="tab"        id="cmp-tab-extid" onclick="switchCmpTab('extid')">🔖 By Request ID</div>
    </div>

    <div id="cmp-src-notif">
    <div class="card">
      <div class="flow-sub-row" style="margin-bottom:14px">
        <div>
          <label>Flow Type</label>
          <div class="flow-pills" id="cmp-flow-pills">
            <div class="flow-pill pill-put"   data-flow="PUT"   onclick="selectFlowType('cmp','PUT')">PUT</div>
            <div class="flow-pill pill-pick"  data-flow="PICK"  onclick="selectFlowType('cmp','PICK')">PICK</div>
            <div class="flow-pill pill-audit" data-flow="AUDIT" onclick="selectFlowType('cmp','AUDIT')">AUDIT</div>
            <div class="flow-pill pill-other" data-flow="OTHER" onclick="selectFlowType('cmp','OTHER')">Other</div>
          </div>
        </div>
        <div class="sub-input-wrap">
          <label>Subscriber ID</label>
          <input type="number" id="cmp-subscriber" placeholder="auto-filled">
        </div>
      </div>

      <!-- Time panel -->
      <div id="cmp-panel-time" style="margin-top:12px">
        <label>Since</label>
        <div style="display:flex;gap:8px;align-items:center">
          <input type="datetime-local" id="cmp-since" style="flex:1">
          <button class="btn btn-ghost" style="padding:7px 12px;font-size:12px" onclick="document.getElementById('cmp-since').value=''">✕ Clear</button>
        </div>
      </div>

      <!-- Ext ID panel -->
      <div id="cmp-panel-extid" style="margin-top:12px;display:none">
        <label>External Request ID (externalServiceRequestId)</label>
        <input type="text" id="cmp-extid" placeholder="e.g. rohit_ext_09">
      </div>

      <div style="margin-top:14px;display:flex;align-items:center;gap:16px;flex-wrap:wrap">
        <button class="btn btn-primary" onclick="doCompare()" id="cmp-btn">🔍 Compare</button>
        <label style="margin:0;display:flex;align-items:center;gap:10px;cursor:pointer">
          <div class="toggle-wrap" id="cmp-mode-wrap" onclick="toggleMode('cmp')">
            <div class="toggle-knob" id="cmp-mode-knob"></div>
          </div>
          <span id="cmp-mode-label" style="font-size:13px;color:#94a3b8">Full Compare</span>
        </label>
        <span id="cmp-mode-hint" style="font-size:11px;color:#4b5563">Compares keys, values, and types</span>
      </div>
    </div>
    <div id="cmp-summary" style="display:none">
      <div class="summary-strip">
        <div class="summary-card"><div class="num num-total" id="cmp-total">0</div><div class="lbl">Total</div></div>
        <div class="summary-card"><div class="num num-pass" id="cmp-pass">0</div><div class="lbl">Pass ✅</div></div>
        <div class="summary-card"><div class="num num-fail" id="cmp-fail">0</div><div class="lbl">Fail ❌</div></div>
        <div class="summary-card"><div class="num num-warn" id="cmp-nogolden">0</div><div class="lbl">No Golden ⚠️</div></div>
      </div>
    </div>
    <div class="card" id="cmp-results-card" style="display:none">
      <div class="card-title">Results</div>
      <table class="results-table">
        <thead><tr>
          <th>DB ID</th><th>Time</th><th>Flow Request ID</th><th>Notification Key</th><th>Status</th><th>Diffs</th>
        </tr></thead>
        <tbody id="cmp-results-body"></tbody>
      </table>
    </div>
    </div><!-- /cmp-src-notif -->

    <!-- Direct JSON compare (any JSON — incl. Erlang-related schema) -->
    <div id="cmp-src-json" style="display:none">
      <div class="card">
        <div class="card-title">🧩 Compare two JSON documents directly</div>
        <p style="font-size:13px;color:#64748b;margin-bottom:14px">Paste or upload an <b>Expected (A)</b> and an <b>Actual (B)</b> JSON. Works for any JSON — notification payloads, Erlang-related schema, configs, anything.</p>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
          <div>
            <div style="display:flex;justify-content:space-between;align-items:center">
              <label style="margin:0">Expected / Baseline (A)</label>
              <span>
                <button class="btn-xs btn-xs-view" onclick="beautifyJson('json-a')" title="Pretty-print">✨ Beautify</button>
                <button class="btn-xs btn-xs-view" onclick="minifyJson('json-a')" title="Collapse to one line">／ Minify</button>
              </span>
            </div>
            <textarea id="json-a" rows="14" spellcheck="false"
              style="width:100%;margin-top:5px;background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:10px;color:#e2e8f0;font-size:12px;font-family:monospace" placeholder='{ "paste expected JSON here": true }'></textarea>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-top:6px">
              <input type="file" id="json-a-file" accept=".json,application/json,text/plain" style="font-size:12px;color:#94a3b8" onchange="loadJsonFile('json-a', this)">
              <span id="json-a-status" style="font-size:11px"></span>
            </div>
          </div>
          <div>
            <div style="display:flex;justify-content:space-between;align-items:center">
              <label style="margin:0">Actual / Candidate (B)</label>
              <span>
                <button class="btn-xs btn-xs-view" onclick="beautifyJson('json-b')" title="Pretty-print">✨ Beautify</button>
                <button class="btn-xs btn-xs-view" onclick="minifyJson('json-b')" title="Collapse to one line">／ Minify</button>
              </span>
            </div>
            <textarea id="json-b" rows="14" spellcheck="false"
              style="width:100%;margin-top:5px;background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:10px;color:#e2e8f0;font-size:12px;font-family:monospace" placeholder='{ "paste actual JSON here": true }'></textarea>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-top:6px">
              <input type="file" id="json-b-file" accept=".json,application/json,text/plain" style="font-size:12px;color:#94a3b8" onchange="loadJsonFile('json-b', this)">
              <span id="json-b-status" style="font-size:11px"></span>
            </div>
          </div>
        </div>
        <div style="margin-top:14px;display:flex;align-items:center;gap:16px;flex-wrap:wrap">
          <button class="btn btn-primary" id="json-cmp-btn" onclick="doJsonCompare()">🧩 Compare JSON</button>
          <div class="flow-pills">
            <div class="flow-pill active pill-put" id="json-mode-full"   onclick="setJsonMode('full')">Full</div>
            <div class="flow-pill pill-pick"       id="json-mode-schema" onclick="setJsonMode('schema')">Schema only</div>
          </div>
          <label style="margin:0;display:flex;align-items:center;gap:8px;cursor:pointer;font-size:12px;color:#94a3b8">
            <input type="checkbox" id="json-ignore-dyn"> ignore dynamic fields (ids, timestamps…)
          </label>
          <span id="json-cmp-err" style="font-size:12px;color:#fca5a5"></span>
        </div>
      </div>
      <div class="summary-strip" id="json-summary" style="display:none">
        <div class="summary-card"><div class="num" id="json-verdict" style="font-size:18px">—</div><div class="lbl">Result</div></div>
        <div class="summary-card"><div class="num" id="json-diffs" style="color:#fca5a5">0</div><div class="lbl">Differences</div></div>
      </div>
      <div class="card" id="json-result-card" style="display:none">
        <div class="card-title">Diff</div>
        <table class="results-table">
          <thead><tr><th>Pair</th><th>Source</th><th></th><th>Key</th><th>Status</th><th>Diffs</th></tr></thead>
          <tbody id="json-result-body"></tbody>
        </table>
      </div>
    </div>

    <!-- COMPARE: Kowl golden (fetch topics, diff vs kowl baseline) -->
    <div id="cmp-src-kowl" style="display:none">
      <div class="card">
        <div class="card-title">Compare live Kowl topics against the stored kowl golden baseline</div>
        <div class="flow-sub-row">
          <div style="flex:1;min-width:220px">
            <label>Target Kowl host:port</label>
            <input type="text" id="tc-cmp-host" placeholder="172.29.32.39:9003">
          </div>
          <div class="sub-input-wrap">
            <label>Recent N</label>
            <input type="number" id="tc-cmp-count" placeholder="50">
          </div>
          <div class="sub-input-wrap" style="width:auto">
            <label>Mode</label>
            <div class="flow-pills">
              <div class="flow-pill active pill-put" id="tc-mode-full"   onclick="setTopicMode('full')">Full</div>
              <div class="flow-pill pill-pick"       id="tc-mode-schema" onclick="setTopicMode('schema')">Schema only</div>
            </div>
          </div>
        </div>
        <div class="mt">
          <button class="btn btn-primary" onclick="compareTopics()" id="tc-cmp-btn">🔍 Compare</button>
        </div>
      </div>
      <div class="summary-strip" id="tc-summary" style="display:none">
        <div class="summary-card"><div class="num" id="tc-total" style="color:#93c5fd">0</div><div class="lbl">Total</div></div>
        <div class="summary-card"><div class="num" id="tc-pass"  style="color:#86efac">0</div><div class="lbl">Pass</div></div>
        <div class="summary-card"><div class="num" id="tc-fail"  style="color:#fca5a5">0</div><div class="lbl">Fail</div></div>
        <div class="summary-card"><div class="num" id="tc-nob"   style="color:#fcd34d">0</div><div class="lbl">No Baseline</div></div>
      </div>
      <div class="card" id="tc-cmp-result" style="display:none">
        <div class="card-title">Results</div>
        <table class="results-table">
          <thead><tr><th>Offset</th><th>Topic</th><th>Entity</th><th>Key</th><th>Status</th><th>Diffs</th></tr></thead>
          <tbody id="tc-results-body"></tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- WATCH -->
  <div class="page" id="page-watch">
    <h2>👁 Watch Mode (Live)</h2>
    <p class="subtitle">Polls DB every N seconds. Start watching, then trigger your flow.</p>
    <div class="card">
      <div class="card-title">Settings</div>

      <div class="tabs" id="watch-fetch-tabs" style="margin-bottom:14px;border-bottom:1px solid #2d3148">
        <div class="tab active" id="watch-fetch-tab-time"  onclick="switchWatchFetchTab('time')">📅 By Time</div>
        <div class="tab"        id="watch-fetch-tab-extid" onclick="switchWatchFetchTab('extid')">🔖 By Request ID</div>
      </div>

      <div style="margin-bottom:14px">
        <label>Compare against golden source</label>
        <div class="flow-pills">
          <div class="flow-pill active pill-put" id="watch-gs-db"   onclick="setWatchGolden('db')">🗄 DB</div>
          <div class="flow-pill pill-other"      id="watch-gs-isd"  onclick="setWatchGolden('isd')">📄 ISD</div>
          <div class="flow-pill pill-pick"       id="watch-gs-kowl" onclick="setWatchGolden('kowl')">🧬 Kowl</div>
        </div>
        <div id="watch-isd-data" class="flow-pills" style="margin-top:8px;display:none">
          <span style="font-size:11px;color:#64748b;margin-right:2px">ISD golden vs live data from:</span>
          <div class="flow-pill active pill-put" id="watch-isd-db"   onclick="setWatchIsdData('db')">🗄 DB</div>
          <div class="flow-pill pill-pick"       id="watch-isd-kowl" onclick="setWatchIsdData('kowl')">🧬 Kowl</div>
        </div>
      </div>

      <div style="margin-bottom:14px" id="watch-flow-block">
        <label>Flow Type</label>
        <div class="flow-pills" id="watch-flow-pills">
          <div class="flow-pill pill-put"   data-flow="PUT"   onclick="selectFlowType('watch','PUT')">PUT</div>
          <div class="flow-pill pill-pick"  data-flow="PICK"  onclick="selectFlowType('watch','PICK')">PICK</div>
          <div class="flow-pill pill-audit" data-flow="AUDIT" onclick="selectFlowType('watch','AUDIT')">AUDIT</div>
          <div class="flow-pill pill-other" data-flow="OTHER" onclick="selectFlowType('watch','OTHER')">Other</div>
        </div>
      </div>
      <div id="watch-kowl-note" style="display:none;margin-bottom:14px;font-size:12px;color:#64748b">
        🧬 Topic-based — watches the configured Kowl topics (<span id="watch-kowl-topics">…</span>). No subscriber needed.
      </div>
      <div class="flex-row">
        <div class="sub-input-wrap" id="watch-sub-wrap">
          <label>Subscriber ID</label>
          <input type="number" id="watch-subscriber" placeholder="auto-filled">
        </div>
        <div>
          <label>Poll Interval (seconds)</label>
          <input type="number" id="watch-interval" value="3" min="1">
        </div>
        <button class="btn btn-success" onclick="startWatch()" id="watch-start-btn">▶ Start</button>
        <button class="btn btn-danger" onclick="stopWatch()" id="watch-stop-btn" disabled>⏹ Stop</button>
      </div>

      <div id="watch-fetch-panel-extid" style="margin-top:12px;display:none">
        <label>External Request ID (externalServiceRequestId)</label>
        <input type="text" id="watch-extid" placeholder="e.g. rohit_ext_09">
      </div>

      <div style="margin-top:14px">
        <label style="margin:0;display:flex;align-items:center;gap:10px;cursor:pointer;width:fit-content">
          <div class="toggle-wrap" id="watch-mode-wrap" onclick="toggleMode('watch')" title="Toggle comparison mode">
            <div class="toggle-knob" id="watch-mode-knob"></div>
          </div>
          <span id="watch-mode-label" style="font-size:13px;color:#94a3b8">Full Compare</span>
        </label>
        <div id="watch-mode-hint" style="font-size:11px;color:#4b5563;margin-top:4px">Compares keys, values, and types</div>
      </div>
      <div style="margin-top:12px;display:flex;align-items:center;gap:8px;font-size:12px;color:#64748b" id="watch-status-row">
        <span id="watch-status-dot"></span>
        <span id="watch-status-text">Idle</span>
      </div>
    </div>
    <div id="watch-summary" style="display:none">
      <div class="summary-strip">
        <div class="summary-card"><div class="num num-total" id="w-total">0</div><div class="lbl">Total</div></div>
        <div class="summary-card"><div class="num num-pass"  id="w-pass">0</div><div class="lbl">Pass ✅</div></div>
        <div class="summary-card"><div class="num num-fail"  id="w-fail">0</div><div class="lbl">Fail ❌</div></div>
        <div class="summary-card"><div class="num num-warn"  id="w-nogolden">0</div><div class="lbl">No Golden ⚠️</div></div>
      </div>
    </div>
    <div class="card">
      <div class="card-title">Live Log</div>
      <div class="log-box" id="watch-log"></div>
    </div>
    <div class="card" id="watch-results-card" style="display:none">
      <div class="card-title">Results</div>
      <table class="results-table">
        <thead><tr>
          <th>DB ID</th><th>Time</th><th>Flow Request ID</th><th>Notification Key</th><th>Status</th><th>Diffs</th>
        </tr></thead>
        <tbody id="watch-results-body"></tbody>
      </table>
    </div>
  </div>

  <!-- CONFIG -->
  <div class="page" id="page-config">
    <h2>⚙️ Config</h2>
    <p class="subtitle">Non-secret settings are saved to <code>config.json</code>. Secrets are kept in memory only and must be entered each time the app starts.</p>

    <!-- Secrets banner -->
    <div id="cfg-secrets-banner" style="display:none;background:#450a0a;border:1px solid #7f1d1d;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#fca5a5">
      ⚠️ <strong>Secrets required</strong> — Enter DB password and SSH key path below before connecting.
    </div>

    <!-- SECRETS SECTION -->
    <div class="card" style="border-color:#4f46e5">
      <div class="card-title" style="color:#a5b4fc">🔒 Secrets</div>

      <!-- Auto-loaded banner -->
      <div id="cfg-secrets-loaded-banner" style="display:none;background:#0f2a1a;border:1px solid #166534;border-radius:6px;padding:10px 14px;margin-bottom:14px;font-size:13px;color:#86efac;display:flex;align-items:center;justify-content:space-between">
        <span>✅ Secrets auto-loaded from saved file</span>
        <button class="btn-xs btn-xs-del" onclick="clearSavedSecrets()">🗑 Remove saved file</button>
      </div>

      <div class="flex-row">

        <!-- SSH Key -->
        <div style="flex:1">
          <label style="display:flex;align-items:center;gap:6px;margin-bottom:8px">
            <span style="background:#1e2235;border-radius:4px;padding:2px 7px;font-size:11px;color:#818cf8;font-weight:700;letter-spacing:.5px">SSH KEY</span>
            <span style="color:#4b5563;font-size:11px">path to private key file</span>
          </label>
          <div style="display:flex;align-items:stretch;border:1px solid #2d3148;border-radius:8px;overflow:hidden;background:#0a0d14;transition:border-color .2s" onfocusin="this.style.borderColor='#6366f1'" onfocusout="this.style.borderColor='#2d3148'">
            <div style="padding:0 12px;display:flex;align-items:center;background:#111827;border-right:1px solid #2d3148;color:#4b5563;font-size:16px;flex-shrink:0">🗝️</div>
            <input type="password" id="cfg-ssh-key" placeholder="e.g. ~/.ssh/id_rsa"
              style="flex:1;background:transparent;border:none;padding:10px 12px;color:#e2e8f0;font-size:13px;font-family:monospace;outline:none;min-width:0">
            <button type="button" id="cfg-ssh-key-toggle" onclick="toggleVisible('cfg-ssh-key','cfg-ssh-key-toggle')"
              style="padding:0 14px;background:none;border:none;border-left:1px solid #2d3148;color:#4b5563;cursor:pointer;font-size:12px;font-weight:600;white-space:nowrap;transition:color .15s;outline:none"
              onmouseover="this.style.color='#a5b4fc'" onmouseout="this.style.color='#4b5563'">👁 Show</button>
          </div>
        </div>

        <!-- DB Password -->
        <div style="flex:1">
          <label style="display:flex;align-items:center;gap:6px;margin-bottom:8px">
            <span style="background:#1e2235;border-radius:4px;padding:2px 7px;font-size:11px;color:#f59e0b;font-weight:700;letter-spacing:.5px">DB PASS</span>
            <span style="color:#4b5563;font-size:11px">postgres password</span>
          </label>
          <div style="display:flex;align-items:stretch;border:1px solid #2d3148;border-radius:8px;overflow:hidden;background:#0a0d14;transition:border-color .2s" onfocusin="this.style.borderColor='#6366f1'" onfocusout="this.style.borderColor='#2d3148'">
            <div style="padding:0 12px;display:flex;align-items:center;background:#111827;border-right:1px solid #2d3148;color:#4b5563;font-size:16px;flex-shrink:0">🔑</div>
            <input type="password" id="cfg-db-pass" placeholder="Enter password"
              style="flex:1;background:transparent;border:none;padding:10px 12px;color:#e2e8f0;font-size:13px;font-family:monospace;outline:none;min-width:0">
            <button type="button" id="cfg-db-pass-toggle" onclick="toggleVisible('cfg-db-pass','cfg-db-pass-toggle')"
              style="padding:0 14px;background:none;border:none;border-left:1px solid #2d3148;color:#4b5563;cursor:pointer;font-size:12px;font-weight:600;white-space:nowrap;transition:color .15s;outline:none"
              onmouseover="this.style.color='#a5b4fc'" onmouseout="this.style.color='#4b5563'">👁 Show</button>
          </div>
        </div>

      </div>

      <!-- Save to disk toggle -->
      <label for="cfg-save-disk" style="margin-top:14px;display:flex;align-items:center;gap:10px;cursor:pointer">
        <input type="checkbox" id="cfg-save-disk" style="width:16px;height:16px;accent-color:#4f46e5;cursor:pointer">
        <span style="font-size:13px;color:#94a3b8">Save to disk <span style="color:#4b5563;font-size:12px">(stores in <code style="color:#818cf8">.secrets</code> file — skip re-entry on restart)</span></span>
      </label>

      <div class="mt" style="display:flex;align-items:center;gap:12px;flex-wrap:wrap">
        <button class="btn btn-primary" onclick="saveSecrets()" id="cfg-secrets-btn">🔒 Set Secrets</button>
        <span id="cfg-secrets-status" style="font-size:13px"></span>
      </div>
    </div>

    <!-- SAVED CONFIG SECTION -->
    <div class="card">
      <div class="card-title">🔐 SSH Tunnel</div>
      <div class="flex-row">
        <div><label>SSH Host</label><input type="text" id="cfg-ssh-host"></div>
        <div><label>SSH Port</label><input type="number" id="cfg-ssh-port"></div>
      </div>
      <div class="flex-row">
        <div><label>SSH User</label><input type="text" id="cfg-ssh-user"></div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">🗄 PostgreSQL</div>
      <div class="flex-row">
        <div><label>DB Host (internal, via tunnel)</label><input type="text" id="cfg-db-host"></div>
        <div><label>DB Port</label><input type="number" id="cfg-db-port"></div>
      </div>
      <div class="flex-row">
        <div><label>Database Name</label><input type="text" id="cfg-db-name"></div>
        <div><label>Table</label><input type="text" id="cfg-db-table"></div>
      </div>
      <div class="flex-row">
        <div><label>Username</label><input type="text" id="cfg-db-user"></div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">📋 Subscriber IDs <span style="font-size:11px;font-weight:400;color:#64748b;margin-left:8px">At least one required</span></div>
      <div class="flex-row">
        <div><label>PUT Subscriber ID</label><input type="number" id="cfg-sub-put" placeholder="e.g. 157"></div>
        <div><label>PICK Subscriber ID</label><input type="number" id="cfg-sub-pick" placeholder="e.g. 158"></div>
      </div>
      <div class="flex-row">
        <div><label>AUDIT Subscriber ID</label><input type="number" id="cfg-sub-audit" placeholder="e.g. 159"></div>
        <div><label>Other Subscriber ID</label><input type="number" id="cfg-sub-other" placeholder="e.g. 160"></div>
      </div>
      <div id="cfg-sub-error" style="color:#fca5a5;font-size:12px;margin-top:6px;display:none">⚠️ At least one subscriber ID must be set</div>
    </div>

    <div class="card">
      <div class="card-title">🧬 Topic Compare (Kowl / Kafka UI)</div>
      <div class="flex-row">
        <div><label>Baseline setup — Kowl host:port</label><input type="text" id="cfg-topic-host" placeholder="172.29.32.39:9003"></div>
        <div><label>Target setup — Kowl host:port</label><input type="text" id="cfg-topic-host-b" placeholder="172.29.32.39:9003"></div>
      </div>
      <div class="flex-row">
        <div><label>Recent messages per topic</label><input type="number" id="cfg-topic-count" placeholder="50"></div>
      </div>
      <label>Topics (one per line — <code>LABEL = topic.name</code>)</label>
      <textarea id="cfg-topics" rows="4" style="width:100%;background:#0f1117;border:1px solid #2d3148;border-radius:6px;padding:8px 12px;color:#e2e8f0;font-size:12px;font-family:monospace;outline:none" placeholder="PUT = stpfunction-sbscloud.put_information.events"></textarea>
    </div>

    <div class="card">
      <div class="card-title">📁 Project</div>
      <div class="flex-row">
        <div><label>Current project (golden data is categorized under this name)</label>
          <input type="text" id="cfg-project" placeholder="e.g. SBSCloud_2026" list="cfg-project-list">
          <datalist id="cfg-project-list"></datalist>
        </div>
      </div>
      <p style="font-size:11px;color:#64748b;margin-top:6px">Goldens are stored under <code>golden/{project}/{FLOW}/…</code>. Leave blank to use the shared root.</p>
    </div>

    <div class="card">
      <div class="card-title">🔧 Defaults</div>
      <div class="flex-row">
        <div><label>Watch Poll Interval (seconds)</label><input type="number" id="cfg-poll"></div>
      </div>
    </div>

    <div style="display:flex;gap:12px;align-items:center">
      <button type="button" class="btn btn-ghost" onclick="saveConfig()" id="cfg-save-btn">💾 Save Config</button>
      <button type="button" class="btn btn-ghost" onclick="testConnection()" id="cfg-test-btn">🔌 Test Connection</button>
      <span id="cfg-status" style="font-size:13px"></span>
    </div>
  </div>

  <!-- GOLDENS -->
  <div class="page" id="page-goldens">
    <h2>🗂 Golden Snapshots</h2>
    <p class="subtitle">Saved expected payloads. Click View to inspect, Delete to remove.</p>
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px">
        <div class="card-title" style="margin:0">Snapshots</div>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <button class="btn btn-ghost" id="g-select-btn" onclick="toggleGoldenSelect()">☑ Select</button>
          <button class="btn btn-danger" id="g-del-sel-btn" onclick="deleteSelectedGoldens()" style="display:none">🗑 Delete Selected (<span id="g-sel-count">0</span>)</button>
          <button class="btn btn-danger" onclick="deleteAllGoldens()">🗑 Delete All</button>
          <button class="btn btn-ghost" onclick="loadGoldens()">↻ Refresh</button>
        </div>
      </div>
      <div id="goldens-list"><div class="no-results">Loading...</div></div>
    </div>
  </div>

</main>

<!-- Modal -->
<div class="modal-overlay" id="modal">
  <div class="modal">
    <div class="modal-header">
      <h3 id="modal-title">Golden Payload</h3>
      <button class="modal-close" onclick="closeModal()">×</button>
    </div>
    <div class="modal-body">
      <pre id="modal-body"></pre>
    </div>
  </div>
</div>

<script>
// ── Navigation ──────────────────────────────────────────────────────────────
function showPage(name) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('page-' + name).classList.add('active');
  document.querySelectorAll('.nav-item').forEach(n => {
    if (n.textContent.toLowerCase().includes(name === 'dashboard' ? 'dashboard'
        : name === 'capture' ? 'capture' : name === 'compare' ? 'compare'
        : name === 'watch' ? 'watch' : name === 'config' ? 'config' : 'golden'))
      n.classList.add('active');
  });
  if (name === 'goldens' || name === 'dashboard') loadGoldens();
  if (name === 'config') loadConfig();
  if (name === 'capture') refreshCaptureProject();
  if (name === 'dashboard') { loadReports(); checkAllureStatus(); }
}

let allureStatusChecked = false;
async function checkAllureStatus() {
  if (allureStatusChecked) return;        // one-time per session
  allureStatusChecked = true;
  try {
    const s = await (await fetch('/api/allure/status')).json();
    const warn = document.getElementById('allure-cli-warn');
    if (warn) warn.style.display = s.html_capable ? 'none' : 'block';
  } catch (e) {}
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function datetimeLocalToISO(val) {
  if (!val) return null;
  return val.replace('T', ' ') + ':00';
}

function statusBadge(s) {
  const map = {PASS:'pass', FAIL:'fail', 'NO GOLDEN':'warn', 'NO BASELINE':'warn', ERROR:'error'};
  const icon = {PASS:'✅', FAIL:'❌', 'NO GOLDEN':'⚠️', 'NO BASELINE':'⚠️', ERROR:'🔥'};
  return `<span class="badge badge-${map[s]||'info'}">${icon[s]||''} ${s}</span>`;
}

function renderResultRow(r, tbodyId) {
  const tbody = document.getElementById(tbodyId);
  const rowId = 'row-' + r.db_id + '-' + tbodyId;
  const tr = document.createElement('tr');
  tr.className = 'expandable';
  tr.innerHTML = `
    <td>${r.db_id}</td>
    <td style="white-space:nowrap;font-size:11px">${r.create_time}</td>
    <td style="font-family:monospace;font-size:11px;color:#64748b">${r.ext_id || '—'}</td>
    <td style="font-family:monospace;font-size:12px">${r.key}</td>
    <td>${statusBadge(r.status)}</td>
    <td>${r.findings.length}</td>
  `;
  tbody.appendChild(tr);

  const diffBlock = r.findings.length > 0 ? `
        <div style="font-size:11px;font-weight:700;color:#64748b;margin:0 0 6px">
          DIFFERENCES (${r.findings.length})
        </div>
        ${r.findings.map(f => `
          <div class="diff-row diff-row-fail">
            <span class="diff-type">${f.type}</span>
            <span class="diff-path">${f.path}</span>
            <span class="diff-detail-text">${f.detail}</span>
          </div>`).join('')}` : '';

  const jsonBlock = r.payload ? `
        <div style="font-size:11px;font-weight:700;color:#64748b;margin:${diffBlock ? '12px' : '0'} 0 6px">
          PAYLOAD JSON <span style="font-weight:400;color:#475569">— <span style="color:#86efac">green = matches golden</span>, <span style="color:#fca5a5">red = schema mismatch</span></span>
        </div>
        <pre class="payload-json">${colorJsonLines(r.payload, r.findings)}</pre>` : '';

  if (diffBlock || jsonBlock) {
    const detail = document.createElement('tr');
    detail.innerHTML = `<td colspan="6">
      <div class="diff-detail" id="${rowId}-detail">
        ${diffBlock}${jsonBlock}
      </div>
    </td>`;
    tbody.appendChild(detail);
    tr.onclick = () => {
      const d = document.getElementById(rowId + '-detail');
      d.style.display = d.style.display === 'block' ? 'none' : 'block';
    };
  }
}

function escapeHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// Convert a DeepDiff path (root['a']['b'][0]) into a dot-path (a.b.0).
function parseDiffPaths(findings) {
  const set = new Set();
  (findings || []).forEach(f => {
    const segs = (f.path || '').match(/\\[['"]?([^\\]'"]+)['"]?\\]/g) || [];
    const path = segs.map(s => s.replace(/^\\[['"]?/, '').replace(/['"]?\\]$/, '')).join('.');
    if (path) set.add(path);
  });
  return set;
}

// A line is "bad" only if it IS a changed node or sits INSIDE an added/removed
// subtree — never just because it's an ancestor on the way to a deep change.
function pathIsBad(path, diffPaths) {
  if (!path) return false;
  if (diffPaths.has(path)) return true;
  for (const d of diffPaths) if (path.startsWith(d + '.')) return true;
  return false;
}

// Render payload JSON line-by-line: green = matches golden, red = exact mismatch.
function colorJsonLines(payload, findings) {
  const diffPaths = parseDiffPaths(findings);
  const lines = [];
  function walk(node, path, indent, keyLabel, comma) {
    const pad = '  '.repeat(indent);
    const bad = pathIsBad(path, diffPaths);
    const prefix = keyLabel !== null ? '"' + keyLabel + '": ' : '';
    if (node !== null && typeof node === 'object') {
      const isArr = Array.isArray(node);
      lines.push({t: pad + prefix + (isArr ? '[' : '{'), bad});
      const entries = isArr ? node.map((v, i) => [i, v]) : Object.entries(node);
      entries.forEach((kv, i) => {
        const childPath = path ? path + '.' + kv[0] : String(kv[0]);
        walk(kv[1], childPath, indent + 1, isArr ? null : kv[0], i < entries.length - 1);
      });
      lines.push({t: pad + (isArr ? ']' : '}') + (comma ? ',' : ''), bad});
    } else {
      lines.push({t: pad + prefix + JSON.stringify(node) + (comma ? ',' : ''), bad});
    }
  }
  walk(payload, '', 0, null, false);
  return lines.map(l =>
    '<span class="' + (l.bad ? 'jl-bad' : 'jl-ok') + '">' + escapeHtml(l.t) + '</span>'
  ).join('\\n');
}

function updateWatchCounters(results) {
  const pass = results.filter(r=>r.status==='PASS').length;
  const fail = results.filter(r=>r.status==='FAIL').length;
  const nog  = results.filter(r=>r.status==='NO GOLDEN').length;
  document.getElementById('w-total').textContent = results.length;
  document.getElementById('w-pass').textContent  = pass;
  document.getElementById('w-fail').textContent  = fail;
  document.getElementById('w-nogolden').textContent = nog;
  document.getElementById('watch-summary').style.display = 'block';
  document.getElementById('watch-results-card').style.display = 'block';
}

// ── Full Run (live, all flows) ────────────────────────────────────────────────
let fullRunResults = [];
let fullRunSSE = null;
let fullRunGolden = 'db';
let fullRunIsdData = 'db';

function setFullRunGolden(src) {
  fullRunGolden = src;
  ['db','isd','kowl'].forEach(s =>
    document.getElementById('fullrun-gs-' + s).classList.toggle('active', s === src));
  document.getElementById('fullrun-isd-data').style.display = src === 'isd' ? 'flex' : 'none';
}
function setFullRunIsdData(src) {
  fullRunIsdData = src;
  document.getElementById('fullrun-isd-db').classList.toggle('active', src === 'db');
  document.getElementById('fullrun-isd-kowl').classList.toggle('active', src === 'kowl');
}

// Generic CSS-donut painter
function paintDonut(donutId, pctId, pass, fail, other) {
  const total = pass + fail + other;
  if (!total) return;
  const pPass = pass / total * 100, pFail = pPass + fail / total * 100;
  document.getElementById(donutId).style.background =
    `conic-gradient(#22c55e 0 ${pPass}%, #ef4444 ${pPass}% ${pFail}%, #eab308 ${pFail}% 100%)`;
  document.getElementById(pctId).textContent = Math.round(pass / total * 100) + '%';
}

function updateFullRunCounters(results) {
  const pass = results.filter(r=>r.status==='PASS').length;
  const fail = results.filter(r=>r.status==='FAIL').length;
  const nog  = results.filter(r=>r.status!=='PASS'&&r.status!=='FAIL').length;
  document.getElementById('fr-total').textContent = results.length;
  document.getElementById('fr-pass').textContent  = pass;
  document.getElementById('fr-fail').textContent  = fail;
  document.getElementById('fr-nogolden').textContent = nog;
  document.getElementById('fr-leg-pass').textContent = pass;
  document.getElementById('fr-leg-fail').textContent = fail;
  document.getElementById('fr-leg-other').textContent = nog;
  paintDonut('fr-donut', 'fr-donut-pct', pass, fail, nog);
  document.getElementById('fullrun-summary').style.display = 'block';
  document.getElementById('fullrun-chart-card').style.display = 'block';
  document.getElementById('fullrun-results-card').style.display = 'block';
}

async function startFullRun() {
  const interval = document.getElementById('fullrun-interval').value;
  const res = await fetch('/api/full-run/start', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ interval, mode: modeState.fullrun, golden_source: fullRunGolden,
                           data_source: fullRunGolden === 'isd' ? fullRunIsdData : undefined })
  });
  const data = await res.json();
  if (!data.ok) { alert(data.error); return; }

  fullRunResults = [];
  document.getElementById('fullrun-results-body').innerHTML = '';
  document.getElementById('fullrun-log').innerHTML = '';
  document.getElementById('fullrun-log-card').style.display = 'block';

  document.getElementById('fullrun-start-btn').disabled = true;
  document.getElementById('fullrun-stop-btn').disabled  = false;
  setFullRunModeLocked(true);
  document.getElementById('fullrun-status-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('fullrun-status-text').textContent = 'Watching all flows...';

  fullRunSSE = new EventSource('/api/full-run/stream');
  fullRunSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;

    const log = document.getElementById('fullrun-log');
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;

    if (item.result) {
      const r = item.result;
      if (r.flow) r.create_time = r.flow + ' · ' + r.create_time;
      fullRunResults.push(r);
      renderResultRow(r, 'fullrun-results-body');
      updateFullRunCounters(fullRunResults);
    }

    if (item.type === 'done') {
      fullRunSSE.close();
      document.getElementById('fullrun-start-btn').disabled = false;
      document.getElementById('fullrun-stop-btn').disabled  = true;
      setFullRunModeLocked(false);
      document.getElementById('fullrun-status-dot').innerHTML = '';
      document.getElementById('fullrun-status-text').textContent = 'Idle';
      if (item.report) {
        const dl = document.getElementById('fullrun-report-dl');
        dl.href = '/api/report/' + encodeURIComponent(item.report) + '?download=1';
        dl.style.display = 'inline-flex';
      }
      showAllure(item.allure_zip, item.allure_html);
      loadReports();  // new report now appears under Past Reports
    }
  };
}

function showAllure(zip, html) {
  const card = document.getElementById('fullrun-allure-card');
  const body = document.getElementById('fullrun-allure-body');
  if (!zip && !html) { card.style.display = 'none'; return; }
  card.style.display = 'block';
  let h = '';
  if (html) {
    h += `<div style="margin-bottom:8px"><a class="btn btn-success" href="/api/allure-html/${html.replace('-html','')}/" target="_blank">📊 Open Allure Report</a></div>`;
  }
  if (zip) {
    h += `<div style="margin-bottom:8px"><a class="btn btn-primary" href="/api/allure/${encodeURIComponent(zip)}" download>⬇ Download allure-results (.zip)</a></div>`;
    h += `<div style="font-size:12px;color:#64748b">No Allure HTML on the server (allure CLI/Java not installed). Unzip the file and view it with:
            <pre class="payload-json" style="margin-top:6px">unzip ${zip} -d allure-results\nallure serve allure-results</pre></div>`;
  }
  body.innerHTML = h;
}

async function stopFullRun() {
  await fetch('/api/full-run/stop', {method:'POST'});
  document.getElementById('fullrun-status-text').textContent = 'Stopping — saving report...';
  // The thread emits a 'done' event after building the report; SSE handler finalizes UI.
}

// ── Dashboard ─────────────────────────────────────────────────────────────────
async function loadGoldens() {
  const el = document.getElementById('goldens-list');
  if (!el) return;
  const keys = await (await fetch('/api/goldens')).json();
  if (!keys.length) {
    el.innerHTML = '<div class="no-results">No golden snapshots yet. Use Capture to create them.</div>';
    return;
  }
  el.innerHTML =
    `<div style="font-size:11px;color:#64748b;margin-bottom:12px">${keys.length} snapshot(s) — click a folder to expand, then “View” to see the JSON</div>` +
    renderGoldenNode(buildGoldenTree(keys), true, '');
  if (goldenSelectMode) el.classList.add('select-on');
  updateGoldenSelCount();
}

// Build a nested tree from relative paths like EL_Columbus/db/PUT/PUT__created__SUCCESS
function buildGoldenTree(keys) {
  const root = {};
  keys.forEach(k => {
    const parts = k.split('/');
    let node = root;
    parts.forEach((p, i) => {
      if (i === parts.length - 1) {
        (node.__leaves = node.__leaves || []).push({name: p, key: k});
      } else {
        node[p] = node[p] || {};
        node = node[p];
      }
    });
  });
  return root;
}

function countLeaves(node) {
  let c = (node.__leaves || []).length;
  for (const k in node) if (k !== '__leaves') c += countLeaves(node[k]);
  return c;
}

const FOLDER_ICON = {db: '🗄', kowl: '🧬', isd: '📄', PUT: '📦', PICK: '🛒', AUDIT: '🔎', SR: '🔁'};

function renderGoldenNode(node, topLevel, prefix) {
  let html = '';
  Object.keys(node).filter(k => k !== '__leaves').sort().forEach(name => {
    const child = node[name];
    const icon = FOLDER_ICON[name] || '📁';
    const childPrefix = prefix ? prefix + '/' + name : name;
    html += `<details class="tree-group"${topLevel ? ' open' : ''}>
      <summary>${icon} <b>${name}</b> <span class="tree-count">${countLeaves(child)}</span>
        <span class="folder-del" title="Delete this folder"
              onclick="event.preventDefault();event.stopPropagation();deleteGoldenFolder('${childPrefix}',${countLeaves(child)})">🗑</span>
      </summary>
      <div class="tree-children">${renderGoldenNode(child, false, childPrefix)}</div>
    </details>`;
  });
  (node.__leaves || []).sort((a, b) => a.name.localeCompare(b.name)).forEach(leaf => {
    const id = 'g-' + btoa(unescape(encodeURIComponent(leaf.key))).replace(/[^a-zA-Z0-9]/g, '');
    html += `<div class="tree-leaf">
      <div class="tree-leaf-row">
        <span class="tree-leaf-name">
          <input type="checkbox" class="g-check" value="${leaf.key}" onchange="updateGoldenSelCount()">
          📄 ${leaf.name}
        </span>
        <div class="golden-actions">
          <button class="btn-xs btn-xs-view" onclick="toggleGoldenJson('${id}','${leaf.key}',this)">View</button>
          <button class="btn-xs btn-xs-del"  onclick="deleteGolden('${leaf.key}')">Delete</button>
        </div>
      </div>
      <div class="tree-json" id="${id}" style="display:none"></div>
    </div>`;
  });
  return html;
}

let goldenSelectMode = false;
function toggleGoldenSelect() {
  goldenSelectMode = !goldenSelectMode;
  document.getElementById('goldens-list').classList.toggle('select-on', goldenSelectMode);
  document.getElementById('g-select-btn').classList.toggle('btn-primary', goldenSelectMode);
  document.getElementById('g-del-sel-btn').style.display = goldenSelectMode ? 'inline-flex' : 'none';
  updateGoldenSelCount();
}

function selectedGoldenKeys() {
  return Array.from(document.querySelectorAll('#goldens-list .g-check:checked')).map(c => c.value);
}
function updateGoldenSelCount() {
  const el = document.getElementById('g-sel-count');
  if (el) el.textContent = selectedGoldenKeys().length;
}

async function postGoldenDelete(body, confirmMsg) {
  if (confirmMsg && !confirm(confirmMsg)) return;
  const res = await fetch('/api/goldens/delete', {
    method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
  });
  const data = await res.json();
  loadGoldens();
  return data;
}

function deleteSelectedGoldens() {
  const keys = selectedGoldenKeys();
  if (!keys.length) { alert('No snapshots selected.'); return; }
  postGoldenDelete({keys}, `Delete ${keys.length} selected snapshot(s)?`);
}

function deleteAllGoldens() {
  postGoldenDelete({all: true}, '⚠️ Delete ALL golden snapshots? This cannot be undone.');
}

function deleteGoldenFolder(prefix, count) {
  postGoldenDelete({prefix}, `Delete folder "${prefix}" and its ${count} snapshot(s)?`);
}

async function toggleGoldenJson(id, key, btn) {
  const box = document.getElementById(id);
  if (box.style.display === 'block') { box.style.display = 'none'; btn.textContent = 'View'; return; }
  box.style.display = 'block';
  btn.textContent = 'Hide';
  if (!box.dataset.loaded) {
    box.innerHTML = '<div style="padding:8px;color:#64748b;font-size:12px">Loading…</div>';
    try {
      const data = await (await fetch(goldenUrl(key))).json();
      box.innerHTML = '<pre class="payload-json">' + escapeHtml(JSON.stringify(data, null, 2)) + '</pre>';
      box.dataset.loaded = '1';
    } catch (e) {
      box.innerHTML = '<div style="padding:8px;color:#fca5a5;font-size:12px">Failed to load.</div>';
    }
  }
}

function goldenUrl(key) {
  // encode each path segment separately so slashes are preserved in the URL
  return '/api/golden/' + key.split('/').map(encodeURIComponent).join('/');
}

async function viewGolden(key) {
  const res = await fetch(goldenUrl(key));
  const data = await res.json();
  document.getElementById('modal-title').textContent = key;
  document.getElementById('modal-body').textContent = JSON.stringify(data, null, 2);
  document.getElementById('modal').classList.add('open');
}

async function deleteGolden(key) {
  if (!confirm('Delete golden snapshot: ' + key + '?')) return;
  await fetch(goldenUrl(key), {method:'DELETE'});
  loadGoldens();
}

function closeModal() {
  document.getElementById('modal').classList.remove('open');
}

// ── Capture ───────────────────────────────────────────────────────────────────
function switchCapTab(tab) {
  document.getElementById('cap-tab-time').classList.toggle('active', tab === 'time');
  document.getElementById('cap-tab-live').classList.toggle('active', tab === 'live');
  document.getElementById('cap-panel-time').style.display = tab === 'time' ? 'block' : 'none';
  document.getElementById('cap-panel-live').style.display = tab === 'live' ? 'block' : 'none';
}

function switchCapSource(src) {
  ['db','kowl','isd'].forEach(s => {
    document.getElementById('cap-src-' + s).style.display = s === src ? 'block' : 'none';
    document.getElementById('cap-src-tab-' + s).classList.toggle('active', s === src);
  });
  if (src === 'kowl') {
    initTopics();
    const lbl = document.getElementById('cap-kowl-project');
    if (lbl) refreshCaptureProject().then(() => {
      const p = document.getElementById('cap-project-label').textContent;
      lbl.textContent = p;
    });
  }
}

async function refreshCaptureProject() {
  try {
    const cfg = await (await fetch('/api/config')).json();
    const p = cfg.project || '(none)';
    document.getElementById('cap-project-label').textContent = p;
    document.getElementById('cap-isd-project').textContent   = p;
  } catch (e) {}
}

async function uploadISD() {
  const btn = document.getElementById('cap-isd-btn');
  const fileEl = document.getElementById('cap-isd-file');
  if (!fileEl.files.length) { alert('Choose an ISD PDF first.'); return; }
  const fd = new FormData();
  fd.append('file', fileEl.files[0]);
  btn.disabled = true; btn.textContent = '⏳ Reading ISD...';
  try {
    const res = await fetch('/api/golden/from-isd', {method:'POST', body: fd});
    const data = await res.json();
    const card = document.getElementById('cap-isd-result');
    const body = document.getElementById('cap-isd-result-body');
    card.style.display = 'block';
    if (data.error) {
      body.innerHTML = '<div style="color:#fca5a5">❌ ' + data.error + '</div>';
    } else {
      const unparse = data.blocks_unparseable || 0;
      body.innerHTML =
        `<div style="font-size:12px;color:#86efac;margin-bottom:6px">✅ Read ${data.pages} page(s); saved ${data.keys} golden(s) under project "${data.project || '(none)'}".</div>` +
        `<div style="font-size:11px;color:#64748b;margin-bottom:10px">JSON blocks found: ${data.blocks_seen} · parsed: ${data.blocks_parsed}${unparse ? ` · <span style="color:#fcd34d">unparseable: ${unparse}</span>` : ''}</div>` +
        (data.saved.length
          ? data.saved.map(s => `<div class="golden-item"><span class="golden-name">${s.key}</span></div>`).join('')
          : '<div style="color:#fcd34d;font-size:12px">No payloads auto-extracted.</div>') +
        (unparse ? `<div style="font-size:11px;color:#fcd34d;margin-top:8px">⚠️ ${unparse} payload block(s) couldn't be parsed (the PDF's JSON is malformed — smart quotes / wrapped tokens). Paste those below to capture them.</div>` : '');
      loadGoldens();
    }
  } catch (e) { alert('ISD upload error: ' + e); }
  btn.disabled = false; btn.textContent = '📄 Read ISD & Capture Golden';
}

async function saveIsdPaste() {
  const btn = document.getElementById('isd-paste-btn');
  const status = document.getElementById('isd-paste-status');
  const text = document.getElementById('isd-paste').value.trim();
  if (!text) { status.textContent = 'Paste a payload first.'; status.style.color = '#fca5a5'; return; }
  btn.disabled = true; btn.textContent = '⏳ Saving...'; status.textContent = '';
  try {
    const res = await fetch('/api/golden/from-json', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({text})
    });
    const data = await res.json();
    const result = document.getElementById('isd-paste-result');
    if (data.error) {
      status.textContent = '❌ ' + data.error; status.style.color = '#fca5a5';
    } else {
      status.textContent = `✅ Saved ${data.keys} golden(s) from ${data.objects} object(s)`;
      status.style.color = '#86efac';
      result.innerHTML = data.saved.map(s => `<div class="golden-item"><span class="golden-name">${s.key}</span></div>`).join('');
      loadGoldens();
    }
  } catch (e) { status.textContent = 'Error: ' + e; status.style.color = '#fca5a5'; }
  btn.disabled = false; btn.textContent = '💾 Save pasted as golden';
}

let capFetchMode     = 'time';
let capLiveFetchMode = 'time';
let watchFetchMode   = 'time';

function switchCapFetchTab(tab) {
  capFetchMode = tab;
  document.getElementById('cap-fetch-tab-time').classList.toggle('active',  tab === 'time');
  document.getElementById('cap-fetch-tab-extid').classList.toggle('active', tab === 'extid');
  document.getElementById('cap-fetch-panel-time').style.display  = tab === 'time'  ? 'block' : 'none';
  document.getElementById('cap-fetch-panel-extid').style.display = tab === 'extid' ? 'block' : 'none';
}

function switchCapLiveFetchTab(tab) {
  capLiveFetchMode = tab;
  document.getElementById('cap-live-fetch-tab-time').classList.toggle('active',  tab === 'time');
  document.getElementById('cap-live-fetch-tab-extid').classList.toggle('active', tab === 'extid');
  document.getElementById('cap-live-fetch-panel-extid').style.display = tab === 'extid' ? 'block' : 'none';
}

function switchWatchFetchTab(tab) {
  watchFetchMode = tab;
  document.getElementById('watch-fetch-tab-time').classList.toggle('active',  tab === 'time');
  document.getElementById('watch-fetch-tab-extid').classList.toggle('active', tab === 'extid');
  document.getElementById('watch-fetch-panel-extid').style.display = tab === 'extid' ? 'block' : 'none';
}

async function doCapture() {
  const btn = document.getElementById('cap-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Connecting...';

  const subscriber = document.getElementById('cap-subscriber').value;
  const since  = capFetchMode === 'time'  ? datetimeLocalToISO(document.getElementById('cap-since').value) : null;
  const ext_id = capFetchMode === 'extid' ? document.getElementById('cap-extid').value.trim() : null;

  if (capFetchMode === 'extid' && !ext_id) {
    alert('Please enter an External Request ID.');
    btn.disabled = false; btn.innerHTML = '📸 Capture'; return;
  }

  try {
    const res = await fetch('/api/capture', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({subscriber, since, ext_id})
    });
    const data = await res.json();
    const el   = document.getElementById('cap-result');
    const body = document.getElementById('cap-result-body');
    el.style.display = 'block';
    if (data.ok) {
      body.innerHTML = `
        <p style="color:#86efac;margin-bottom:10px">✅ Captured ${data.saved.length} golden snapshot(s) from ${data.total_fetched} notifications.</p>
        ${data.saved.map(k=>`<div style="font-family:monospace;font-size:12px;color:#a5b4fc;padding:2px 0">${k}</div>`).join('')}
      `;
    } else {
      body.innerHTML = `<p style="color:#fca5a5">❌ ${data.error}</p>`;
    }
  } catch(e) {
    alert('Error: ' + e.message);
  }
  btn.disabled = false;
  btn.innerHTML = '📸 Capture';
}

// ── Live Capture ──────────────────────────────────────────────────────────────
let liveCapSSE = null;

async function startLiveCapture() {
  const subscriber = document.getElementById('cap-live-subscriber').value;
  const interval   = document.getElementById('cap-live-interval').value;
  const ext_id     = capLiveFetchMode === 'extid' ? document.getElementById('cap-live-extid').value.trim() : null;

  if (capLiveFetchMode === 'extid' && !ext_id) {
    alert('Please enter an External Request ID.'); return;
  }

  const res = await fetch('/api/capture/live/start', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({subscriber, interval, ext_id})
  });
  const data = await res.json();
  if (!data.ok) { alert(data.error); return; }

  document.getElementById('cap-live-log').innerHTML = '';
  document.getElementById('cap-live-result').style.display = 'none';
  document.getElementById('cap-live-start-btn').disabled = true;
  document.getElementById('cap-live-stop-btn').disabled  = false;
  document.getElementById('cap-live-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('cap-live-status').textContent = 'Polling — trigger your flow now...';

  liveCapSSE = new EventSource('/api/capture/live/stream');
  liveCapSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;

    const log  = document.getElementById('cap-live-log');
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;

    if (item.type === 'done') {
      liveCapSSE.close();
      document.getElementById('cap-live-start-btn').disabled = false;
      document.getElementById('cap-live-stop-btn').disabled  = true;
      document.getElementById('cap-live-dot').innerHTML = '';
      document.getElementById('cap-live-status').textContent = 'Done.';
      if (item.saved && item.saved.length > 0) {
        const el = document.getElementById('cap-live-result');
        el.style.display = 'block';
        document.getElementById('cap-live-result-body').innerHTML =
          `<p style="color:#86efac;margin-bottom:10px">✅ ${item.saved.length} golden snapshot(s) saved.</p>` +
          item.saved.map(k=>`<div style="font-family:monospace;font-size:12px;color:#a5b4fc;padding:2px 0">${k}</div>`).join('');
      }
    }
  };
}

async function stopLiveCapture() {
  await fetch('/api/capture/live/stop', {method:'POST'});
  if (liveCapSSE) { liveCapSSE.close(); liveCapSSE = null; }
  document.getElementById('cap-live-start-btn').disabled = false;
  document.getElementById('cap-live-stop-btn').disabled  = true;
  document.getElementById('cap-live-dot').innerHTML = '';
  document.getElementById('cap-live-status').textContent = 'Stopped.';
}

// ── Compare ───────────────────────────────────────────────────────────────────
let cmpFetchMode = 'time';  // 'time' or 'extid'
let cmpGoldenSource = 'db'; // 'db' | 'isd' | 'kowl'

let isdDataSource = 'db';   // when golden=isd: validate DB-data or Kowl-data
let topicGoldenSource = 'kowl';  // what golden the kowl panel compares against

// Pick comparison mode: db/isd golden, kowl golden, or standalone direct-JSON
function setCompareGolden(src) {
  ['db','isd','kowl','json'].forEach(s =>
    document.getElementById('cmp-gs-' + s).classList.toggle('active', s === src));
  document.getElementById('cmp-isd-data').style.display = src === 'isd' ? 'flex' : 'none';
  if (src === 'kowl') {
    cmpGoldenSource = 'kowl'; topicGoldenSource = 'kowl';
    showComparePanel('kowl');
  } else if (src === 'json') {
    showComparePanel('json');
  } else if (src === 'db') {
    cmpGoldenSource = 'db'; showComparePanel('notif');
  } else if (src === 'isd') {
    cmpGoldenSource = 'isd';                 // golden source = isd
    setIsdDataSource(isdDataSource);         // data origin DB or Kowl
  }
}

// For ISD golden: choose whether live data comes from DB or Kowl
function setIsdDataSource(src) {
  isdDataSource = src;
  document.getElementById('cmp-isd-db').classList.toggle('active', src === 'db');
  document.getElementById('cmp-isd-kowl').classList.toggle('active', src === 'kowl');
  if (src === 'kowl') {
    topicGoldenSource = 'isd';               // kowl panel compares against ISD golden
    showComparePanel('kowl');
    initTopics();
  } else {
    showComparePanel('notif');               // DB-fetched notifications vs ISD golden
  }
}

// Toggle which compare panel is visible
function showComparePanel(which) {
  const isKowl = which === 'kowl', isJson = which === 'json';
  document.getElementById('cmp-tabs-row').style.display = (isKowl || isJson) ? 'none' : 'flex';
  document.getElementById('cmp-src-kowl').style.display = isKowl ? 'block' : 'none';
  document.getElementById('cmp-src-json').style.display = isJson ? 'block' : 'none';
  document.getElementById('cmp-src-notif').style.display = (isKowl || isJson) ? 'none' : 'block';
  if (which === 'notif') switchCmpTab(cmpFetchMode === 'extid' ? 'extid' : 'time');
  if (isKowl) initTopics();
}

function switchCmpTab(tab) {
  document.getElementById('cmp-tab-time').classList.toggle('active',  tab === 'time');
  document.getElementById('cmp-tab-extid').classList.toggle('active', tab === 'extid');
  cmpFetchMode = tab;
  document.getElementById('cmp-panel-time').style.display  = tab === 'time'  ? 'block' : 'none';
  document.getElementById('cmp-panel-extid').style.display = tab === 'extid' ? 'block' : 'none';
}

// ── Direct JSON compare ─────────────────────────────────────────────────────────
let jsonMode = 'full';
function setJsonMode(m) {
  jsonMode = m;
  document.getElementById('json-mode-full').classList.toggle('active',   m === 'full');
  document.getElementById('json-mode-schema').classList.toggle('active', m === 'schema');
}

function loadJsonFile(targetId, input) {
  if (!input.files.length) return;
  const reader = new FileReader();
  reader.onload = e => { document.getElementById(targetId).value = e.target.result; beautifyJson(targetId); };
  reader.readAsText(input.files[0]);
}

function jsonStatus(id, msg, ok) {
  const el = document.getElementById(id + '-status');
  if (el) { el.textContent = msg; el.style.color = ok ? '#86efac' : '#fca5a5'; }
}

function beautifyJson(id) {
  const ta = document.getElementById(id);
  const raw = ta.value.trim();
  if (!raw) return;
  try {
    ta.value = JSON.stringify(JSON.parse(raw), null, 2);
    jsonStatus(id, '✓ valid JSON, beautified', true);
  } catch (e) {
    jsonStatus(id, '✗ invalid JSON: ' + e.message, false);
  }
}

function minifyJson(id) {
  const ta = document.getElementById(id);
  const raw = ta.value.trim();
  if (!raw) return;
  try {
    ta.value = JSON.stringify(JSON.parse(raw));
    jsonStatus(id, '✓ minified', true);
  } catch (e) {
    jsonStatus(id, '✗ invalid JSON: ' + e.message, false);
  }
}

async function doJsonCompare() {
  const btn = document.getElementById('json-cmp-btn');
  const err = document.getElementById('json-cmp-err');
  err.textContent = '';
  const a = document.getElementById('json-a').value.trim();
  const b = document.getElementById('json-b').value.trim();
  if (!a || !b) { err.textContent = 'Paste or upload JSON in both A and B.'; return; }
  btn.disabled = true; btn.textContent = '⏳ Comparing...';
  try {
    const res = await fetch('/api/compare/json', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({a, b, mode: jsonMode,
                            ignore_dynamic: document.getElementById('json-ignore-dyn').checked})
    });
    const data = await res.json();
    if (data.error) { err.textContent = data.error; }
    else { renderJsonCompare(data); }
  } catch (e) { err.textContent = 'Compare error: ' + e; }
  btn.disabled = false; btn.textContent = '🧩 Compare JSON';
}

function renderJsonCompare(data) {
  document.getElementById('json-summary').style.display = 'flex';
  const v = document.getElementById('json-verdict');
  v.textContent = data.status === 'PASS' ? '✅ MATCH' : '❌ MISMATCH';
  v.style.color = data.status === 'PASS' ? '#86efac' : '#fca5a5';
  document.getElementById('json-diffs').textContent = data.count;
  document.getElementById('json-result-card').style.display = 'block';
  const body = document.getElementById('json-result-body');
  body.innerHTML = '';
  renderResultRow({
    db_id: 'A↔B', create_time: 'direct', ext_id: '',
    key: jsonMode === 'schema' ? 'schema compare' : 'full compare',
    status: data.status, findings: data.findings, payload: data.payload
  }, 'json-result-body');
}

async function doCompare() {
  const btn = document.getElementById('cmp-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Comparing...';

  const subscriber = document.getElementById('cmp-subscriber').value;
  const since  = cmpFetchMode === 'time'  ? datetimeLocalToISO(document.getElementById('cmp-since').value) : null;
  const ext_id = cmpFetchMode === 'extid' ? document.getElementById('cmp-extid').value.trim() : null;

  if (cmpFetchMode === 'time' && !since) {
    alert('Please set a Since time or use By Request ID mode.');
    btn.disabled = false;
    btn.innerHTML = '🔍 Compare';
    return;
  }
  if (cmpFetchMode === 'extid' && !ext_id) {
    alert('Please enter an External Request ID.');
    btn.disabled = false;
    btn.innerHTML = '🔍 Compare';
    return;
  }

  try {
    const res = await fetch('/api/compare', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({subscriber, since, ext_id, mode: modeState.cmp, golden_source: cmpGoldenSource})
    });
    const data = await res.json();

    if (!data.ok) { alert('Error: ' + data.error); return; }

    const pass = data.results.filter(r=>r.status==='PASS').length;
    const fail = data.results.filter(r=>r.status==='FAIL').length;
    const nog  = data.results.filter(r=>r.status==='NO GOLDEN').length;

    document.getElementById('cmp-total').textContent = data.total;
    document.getElementById('cmp-pass').textContent  = pass;
    document.getElementById('cmp-fail').textContent  = fail;
    document.getElementById('cmp-nogolden').textContent = nog;
    document.getElementById('cmp-summary').style.display = 'block';
    document.getElementById('cmp-results-card').style.display = 'block';

    const tbody = document.getElementById('cmp-results-body');
    tbody.innerHTML = '';
    if (data.results.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" class="no-results">No notifications found for this time range.</td></tr>';
    } else {
      data.results.forEach(r => renderResultRow(r, 'cmp-results-body'));
    }
  } catch(e) {
    alert('Error: ' + e.message);
  }

  btn.disabled = false;
  btn.innerHTML = '🔍 Compare';
}

// ── Watch ─────────────────────────────────────────────────────────────────────
let watchResults = [];
let watchSSE = null;

let watchGolden = 'db';
let watchIsdData = 'db';
function watchDataOrigin() {
  if (watchGolden === 'kowl') return 'kowl';
  if (watchGolden === 'isd' && watchIsdData === 'kowl') return 'kowl';
  return 'db';
}
// Kowl watching is topic-based — hide DB-only Flow/Subscriber/fetch controls.
function updateWatchControls() {
  const kowl = watchDataOrigin() === 'kowl';
  document.getElementById('watch-fetch-tabs').style.display = kowl ? 'none' : 'flex';
  document.getElementById('watch-flow-block').style.display = kowl ? 'none' : 'block';
  document.getElementById('watch-sub-wrap').style.display   = kowl ? 'none' : 'block';
  document.getElementById('watch-kowl-note').style.display  = kowl ? 'block' : 'none';
  if (kowl && !document.getElementById('watch-fetch-panel-extid').style.display)
    document.getElementById('watch-fetch-panel-extid').style.display = 'none';
  if (kowl) {
    const names = (topicCfg.topics || []).map(t => t.label).join(', ') || 'none configured';
    document.getElementById('watch-kowl-topics').textContent = names;
  }
}
function setWatchGolden(src) {
  watchGolden = src;
  ['db','isd','kowl'].forEach(s =>
    document.getElementById('watch-gs-' + s).classList.toggle('active', s === src));
  document.getElementById('watch-isd-data').style.display = src === 'isd' ? 'flex' : 'none';
  updateWatchControls();
}
function setWatchIsdData(src) {
  watchIsdData = src;
  document.getElementById('watch-isd-db').classList.toggle('active', src === 'db');
  document.getElementById('watch-isd-kowl').classList.toggle('active', src === 'kowl');
  updateWatchControls();
}

async function startWatch() {
  const subscriber = document.getElementById('watch-subscriber').value;
  const interval   = document.getElementById('watch-interval').value;

  const res = await fetch('/api/watch/start', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({
      subscriber, interval, mode: modeState.watch, golden_source: watchGolden,
      data_source: watchGolden === 'isd' ? watchIsdData : undefined,
      ext_id: watchFetchMode === 'extid' ? document.getElementById('watch-extid').value.trim() : null
    })
  });
  const data = await res.json();
  if (!data.ok) { alert(data.error); return; }

  watchResults = [];
  document.getElementById('watch-results-body').innerHTML = '';
  document.getElementById('watch-log').innerHTML = '';

  document.getElementById('watch-start-btn').disabled = true;
  document.getElementById('watch-stop-btn').disabled  = false;
  setWatchModeLocked(true);
  document.getElementById('watch-status-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('watch-status-text').textContent = 'Watching...';

  watchSSE = new EventSource('/api/watch/stream');
  watchSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;

    const log = document.getElementById('watch-log');
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;

    if (item.result) {
      watchResults.push(item.result);
      renderResultRow(item.result, 'watch-results-body');
      updateWatchCounters(watchResults);
    }

    if (item.type === 'done') {
      watchSSE.close();
      document.getElementById('watch-start-btn').disabled = false;
      document.getElementById('watch-stop-btn').disabled  = true;
      setWatchModeLocked(false);
      document.getElementById('watch-status-dot').innerHTML = '';
      document.getElementById('watch-status-text').textContent = 'Idle';
    }
  };
}

async function stopWatch() {
  await fetch('/api/watch/stop', {method:'POST'});
  if (watchSSE) { watchSSE.close(); watchSSE = null; }
  document.getElementById('watch-start-btn').disabled = false;
  document.getElementById('watch-stop-btn').disabled  = true;
  setWatchModeLocked(false);
  document.getElementById('watch-status-dot').innerHTML = '';
  document.getElementById('watch-status-text').textContent = 'Stopped';
}

// ── Mode Toggle ──────────────────────────────────────────────────────────────
const modeState = { cmp: 'full', watch: 'full', fullrun: 'full' };
let watchModeLocked = false;  // mode can't change mid-run — the watch thread is pinned to its start-time mode
let fullRunModeLocked = false;

function setWatchModeLocked(locked) {
  watchModeLocked = locked;
  const wrap = document.getElementById('watch-mode-wrap');
  const hint = document.getElementById('watch-mode-hint');
  wrap.classList.toggle('disabled', locked);
  if (locked) {
    hint.textContent = 'Locked while watching — stop the run to change comparison mode';
  } else {
    // restore the hint for the current mode
    hint.textContent = modeState.watch === 'schema'
      ? 'Only checks for missing or extra keys — ignores value changes'
      : 'Compares keys, values, and types';
  }
}

function setFullRunModeLocked(locked) {
  fullRunModeLocked = locked;
  const wrap = document.getElementById('fullrun-mode-wrap');
  const hint = document.getElementById('fullrun-mode-hint');
  wrap.classList.toggle('disabled', locked);
  if (locked) {
    hint.textContent = 'Locked while running — stop the Full Run to change comparison mode';
  } else {
    hint.textContent = modeState.fullrun === 'schema'
      ? 'Only checks for missing or extra keys — ignores value changes'
      : 'Compares keys, values, and types';
  }
}

function toggleMode(prefix) {
  if (prefix === 'watch' && watchModeLocked) return;     // ignore clicks during a live run
  if (prefix === 'fullrun' && fullRunModeLocked) return; // ignore clicks during a full run
  const isSchema = modeState[prefix] === 'full';  // about to flip to schema
  modeState[prefix] = isSchema ? 'schema' : 'full';

  const wrap  = document.getElementById(prefix + '-mode-wrap');
  const label = document.getElementById(prefix + '-mode-label');
  const hint  = document.getElementById(prefix + '-mode-hint');

  if (isSchema) {
    wrap.classList.add('on');
    label.textContent = 'Schema Only';
    label.style.color = '#818cf8';
    hint.textContent  = 'Only checks for missing or extra keys — ignores value changes';
  } else {
    wrap.classList.remove('on');
    label.textContent = 'Full Compare';
    label.style.color = '#94a3b8';
    hint.textContent  = 'Compares keys, values, and types';
  }
}

// ── Subscriber ID map (populated from config) ────────────────────────────────
let subscriberIds = {PUT: null, PICK: null, AUDIT: null, OTHER: null};

function selectFlowType(prefix, type) {
  // Highlight the chosen pill
  const pillsEl = document.getElementById(prefix + '-flow-pills');
  if (pillsEl) {
    pillsEl.querySelectorAll('.flow-pill').forEach(p => {
      p.classList.toggle('active', p.dataset.flow === type);
    });
  }
  // Auto-fill subscriber ID if configured
  const inp = document.getElementById(prefix + '-subscriber');
  const val = subscriberIds[type];
  if (inp && val) inp.value = val;
}

// ── Config ───────────────────────────────────────────────────────────────────
async function loadConfig() {
  const res = await fetch('/api/config');
  const cfg = await res.json();
  // non-secret fields from disk
  document.getElementById('cfg-ssh-host').value  = cfg.ssh_host  || '';
  document.getElementById('cfg-ssh-port').value  = cfg.ssh_port  || 22;
  document.getElementById('cfg-ssh-user').value  = cfg.ssh_user  || '';
  document.getElementById('cfg-db-host').value   = cfg.db_host   || '';
  document.getElementById('cfg-db-port').value   = cfg.db_port   || 5432;
  document.getElementById('cfg-db-name').value   = cfg.db_name   || '';
  document.getElementById('cfg-db-table').value  = cfg.db_table  || '';
  document.getElementById('cfg-db-user').value   = cfg.db_user   || '';
  document.getElementById('cfg-poll').value      = cfg.poll_interval || 3;
  document.getElementById('cfg-project').value   = cfg.project || '';
  // project autocomplete from existing golden project folders
  try {
    const pj = await (await fetch('/api/projects')).json();
    document.getElementById('cfg-project-list').innerHTML =
      (pj.projects || []).map(p => `<option value="${p}">`).join('');
  } catch (e) {}
  // topic compare settings
  document.getElementById('cfg-topic-host').value   = cfg.topic_host   || '';
  document.getElementById('cfg-topic-host-b').value = cfg.topic_host_b || '';
  document.getElementById('cfg-topic-count').value  = cfg.topic_count  || 50;
  document.getElementById('cfg-topics').value =
    (cfg.topics || []).map(t => `${t.label} = ${t.topic}`).join('\\n');
  topicCfg = {
    host:   cfg.topic_host   || '',
    host_b: cfg.topic_host_b || '',
    count:  cfg.topic_count  || 50,
    topics: cfg.topics || [],
  };
  // per-flow subscriber IDs
  document.getElementById('cfg-sub-put').value   = cfg.subscriber_put   || '';
  document.getElementById('cfg-sub-pick').value  = cfg.subscriber_pick  || '';
  document.getElementById('cfg-sub-audit').value = cfg.subscriber_audit || '';
  document.getElementById('cfg-sub-other').value = cfg.subscriber_other || '';
  subscriberIds = {
    PUT:   cfg.subscriber_put   || null,
    PICK:  cfg.subscriber_pick  || null,
    AUDIT: cfg.subscriber_audit || null,
    OTHER: cfg.subscriber_other || null,
  };
  // secrets — never pre-filled, always blank on load
  document.getElementById('cfg-ssh-key').value  = '';
  document.getElementById('cfg-db-pass').value  = '';
  // show banner if secrets not yet set
  document.getElementById('cfg-secrets-banner').style.display = cfg.secrets_ready ? 'none' : 'block';
  // check if secrets were auto-loaded from .secrets file
  checkSavedSecretsStatus();
}

function toggleVisible(inputId, btnId) {
  const inp  = document.getElementById(inputId);
  const btn  = typeof btnId === 'string' ? document.getElementById(btnId) : btnId;
  const show = inp.type === 'password';
  inp.type = show ? 'text' : 'password';
  btn.textContent = show ? '🙈 Hide' : '👁 Show';
}

async function checkSavedSecretsStatus() {
  const res  = await fetch('/api/secrets/saved');
  const data = await res.json();
  const banner = document.getElementById('cfg-secrets-loaded-banner');
  if (banner) banner.style.display = data.saved ? 'flex' : 'none';
}

async function saveSecrets() {
  const btn      = document.getElementById('cfg-secrets-btn');
  const status   = document.getElementById('cfg-secrets-status');
  const ssh_key  = document.getElementById('cfg-ssh-key').value.trim();
  const db_pass  = document.getElementById('cfg-db-pass').value;
  const saveDisk = document.getElementById('cfg-save-disk').checked;

  if (!ssh_key && !db_pass) {
    status.textContent = '❌ Enter a password and/or SSH key';
    status.style.color = '#fca5a5';
    return;
  }
  btn.disabled = true;
  const res  = await fetch('/api/secrets', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ssh_key, db_pass, save_to_disk: saveDisk})
  });
  const data = await res.json();
  if (data.ok) {
    const msg = saveDisk ? '✅ Secrets set & saved to disk' : '✅ Secrets set for this session';
    status.textContent = msg;
    status.style.color = '#86efac';
    document.getElementById('cfg-secrets-banner').style.display = 'none';
    document.getElementById('cfg-ssh-key').value  = '';
    document.getElementById('cfg-db-pass').value  = '';
    document.getElementById('cfg-save-disk').checked = false;
    checkSavedSecretsStatus();
  } else {
    status.textContent = '❌ Failed';
    status.style.color = '#fca5a5';
  }
  btn.disabled = false;
  setTimeout(() => status.textContent = '', 4000);
}

async function clearSavedSecrets() {
  await fetch('/api/secrets/clear', {method:'POST'});
  document.getElementById('cfg-secrets-loaded-banner').style.display = 'none';
}

// Returns true if saved successfully, false if validation failed or error.
// Pass silent=true to suppress the status message (used by testConnection).
async function saveConfig(silent = false) {
  const btn    = document.getElementById('cfg-save-btn');
  const status = document.getElementById('cfg-status');
  const errEl  = document.getElementById('cfg-sub-error');

  const subPut   = document.getElementById('cfg-sub-put').value.trim();
  const subPick  = document.getElementById('cfg-sub-pick').value.trim();
  const subAudit = document.getElementById('cfg-sub-audit').value.trim();
  const subOther = document.getElementById('cfg-sub-other').value.trim();

  // At least one subscriber ID required
  if (!subPut && !subPick && !subAudit && !subOther) {
    errEl.style.display = 'block';
    if (!silent) {
      status.textContent = '❌ At least one subscriber ID is required';
      status.style.color = '#fca5a5';
    }
    return false;
  }
  errEl.style.display = 'none';

  btn.disabled = true;
  const payload = {
    ssh_host:          document.getElementById('cfg-ssh-host').value,
    ssh_port:          document.getElementById('cfg-ssh-port').value,
    ssh_user:          document.getElementById('cfg-ssh-user').value,
    db_host:           document.getElementById('cfg-db-host').value,
    db_port:           document.getElementById('cfg-db-port').value,
    db_name:           document.getElementById('cfg-db-name').value,
    db_table:          document.getElementById('cfg-db-table').value,
    db_user:           document.getElementById('cfg-db-user').value,
    subscriber_put:    subPut   || null,
    subscriber_pick:   subPick  || null,
    subscriber_audit:  subAudit || null,
    subscriber_other:  subOther || null,
    poll_interval:     document.getElementById('cfg-poll').value,
    project:           document.getElementById('cfg-project').value.trim(),
    topic_host:        document.getElementById('cfg-topic-host').value.trim(),
    topic_host_b:      document.getElementById('cfg-topic-host-b').value.trim(),
    topic_count:       document.getElementById('cfg-topic-count').value || 50,
    topics:            parseTopicsTextarea(document.getElementById('cfg-topics').value),
  };
  const res = await fetch('/api/config', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  btn.disabled = false;
  if (!silent) {
    status.textContent = data.ok ? '✅ Saved!' : '❌ ' + data.error;
    status.style.color = data.ok ? '#86efac' : '#fca5a5';
    setTimeout(() => status.textContent = '', 3000);
  }
  if (data.ok) {
    subscriberIds = {
      PUT:   subPut   ? parseInt(subPut)   : null,
      PICK:  subPick  ? parseInt(subPick)  : null,
      AUDIT: subAudit ? parseInt(subAudit) : null,
      OTHER: subOther ? parseInt(subOther) : null,
    };
    document.getElementById('watch-interval').value = payload.poll_interval;
  }
  return data.ok;
}

async function testConnection() {
  const btn    = document.getElementById('cfg-test-btn');
  const status = document.getElementById('cfg-status');
  // Save first (with validation) — if save fails, abort
  const saved = await saveConfig(true);
  if (!saved) return;
  btn.disabled = true;
  status.textContent = '🔄 Testing...';
  status.style.color = '#93c5fd';
  const res  = await fetch('/api/config/test', {method:'POST'});
  const data = await res.json();
  status.textContent = data.msg;
  status.style.color = data.ok ? '#86efac' : '#fca5a5';
  btn.disabled = false;
}

// ── Init ─────────────────────────────────────────────────────────────────────
loadGoldens();
loadConfig();
loadReports();
checkAllureStatus();   // dashboard is the default page — warn early if allure CLI is missing
// Set default "since" to 1 hour ago
const now = new Date(); now.setHours(now.getHours() - 1);
const iso = now.toISOString().slice(0,16);
document.getElementById('cmp-since').value = iso;
// cap-since intentionally left blank (fetch last 100 by default)

// ── Topic Compare ──────────────────────────────────────────────────────────────
let topicCfg  = {host:'', host_b:'', count:50, topics:[]};
let topicMode = 'full';
let topicsInited = false;

function parseTopicsTextarea(text) {
  return (text || '').split('\\n').map(l => l.trim()).filter(Boolean).map(line => {
    const i = line.indexOf('=');
    if (i === -1) return {label: 'TOPIC', topic: line};
    return {label: line.slice(0, i).trim() || 'TOPIC', topic: line.slice(i + 1).trim()};
  }).filter(t => t.topic);
}

function setTopicMode(m) {
  topicMode = m;
  document.getElementById('tc-mode-full').classList.toggle('active',   m === 'full');
  document.getElementById('tc-mode-schema').classList.toggle('active', m === 'schema');
}

async function initTopics() {
  // pull latest config so hosts/topics reflect the Config tab
  try {
    const cfg = await (await fetch('/api/config')).json();
    topicCfg = {host: cfg.topic_host || '', host_b: cfg.topic_host_b || '',
                count: cfg.topic_count || 50, topics: cfg.topics || []};
  } catch (e) {}
  if (!document.getElementById('tc-cap-host').value) document.getElementById('tc-cap-host').value = topicCfg.host;
  if (!document.getElementById('tc-cap-count').value) document.getElementById('tc-cap-count').value = topicCfg.count;
  if (!document.getElementById('tc-cmp-host').value) document.getElementById('tc-cmp-host').value = topicCfg.host_b || topicCfg.host;
  if (!document.getElementById('tc-cmp-count').value) document.getElementById('tc-cmp-count').value = topicCfg.count;
  document.getElementById('tc-cap-topics').innerHTML =
    topicCfg.topics.length
      ? topicCfg.topics.map(t => `<div style="padding:3px 0">• <b>${t.label}</b> — ${t.topic}</div>`).join('')
      : '<span style="color:#fca5a5">No topics configured. Add them on the Config tab.</span>';
  loadTopicBaselines();
  topicsInited = true;
}

async function loadTopicBaselines() {
  const res  = await fetch('/api/topics/baselines');
  const keys = await res.json();
  document.getElementById('tc-baseline-count').textContent = keys.length;
  const el = document.getElementById('tc-baseline-list');
  if (!keys.length) { el.innerHTML = '<div class="no-results">No baseline captured yet.</div>'; return; }
  el.innerHTML = keys.map(k => `
    <div class="golden-item">
      <span class="golden-name">${k}</span>
      <div class="golden-actions">
        <button class="btn-xs btn-xs-view" onclick="viewTopicBaseline('${k}')">View</button>
        <button class="btn-xs btn-xs-del"  onclick="deleteTopicBaseline('${k}')">Delete</button>
      </div>
    </div>`).join('');
}

async function viewTopicBaseline(key) {
  const res  = await fetch('/api/topics/baseline/' + encodeURIComponent(key));
  const data = await res.json();
  document.getElementById('modal-title').textContent = key;
  document.getElementById('modal-body').textContent  = JSON.stringify(data, null, 2);
  document.getElementById('modal').classList.add('open');
}

async function deleteTopicBaseline(key) {
  if (!confirm('Delete baseline ' + key + '?')) return;
  await fetch('/api/topics/baseline/' + encodeURIComponent(key), {method:'DELETE'});
  loadTopicBaselines();
}

let kowlCapSSE = null;
async function startKowlCapture() {
  const host = document.getElementById('tc-cap-host').value.trim();
  const interval = document.getElementById('kc-interval').value;
  if (!host) { alert('Enter the Kowl host:port above'); return; }
  if (!topicCfg.topics.length) { alert('No topics configured. Add them on the Config tab.'); return; }
  const res = await fetch('/api/kowl-capture/start', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({host, interval})
  });
  const data = await res.json();
  if (!data.ok) { alert(data.error); return; }
  document.getElementById('kc-start-btn').disabled = true;
  document.getElementById('kc-stop-btn').disabled  = false;
  document.getElementById('kc-dot').innerHTML = '<span class="pulse"></span>';
  document.getElementById('kc-status').textContent = 'Capturing — run your flow...';
  const log = document.getElementById('kc-log');
  log.style.display = 'block'; log.innerHTML = '';
  kowlCapSSE = new EventSource('/api/kowl-capture/stream');
  kowlCapSSE.onmessage = (e) => {
    const item = JSON.parse(e.data);
    if (item.type === 'ping') return;
    const line = document.createElement('div');
    line.className = 'log-line log-' + item.type;
    line.textContent = new Date().toLocaleTimeString() + '  ' + item.msg;
    log.appendChild(line); log.scrollTop = log.scrollHeight;
    if (item.type === 'done') {
      kowlCapSSE.close();
      document.getElementById('kc-start-btn').disabled = false;
      document.getElementById('kc-stop-btn').disabled  = true;
      document.getElementById('kc-dot').innerHTML = '';
      document.getElementById('kc-status').textContent = 'Idle';
      loadTopicBaselines();
    }
  };
}
async function stopKowlCapture() {
  await fetch('/api/kowl-capture/stop', {method:'POST'});
  document.getElementById('kc-status').textContent = 'Stopping...';
}

async function captureTopics() {
  const btn  = document.getElementById('tc-cap-btn');
  const host = document.getElementById('tc-cap-host').value.trim();
  const count = parseInt(document.getElementById('tc-cap-count').value) || 50;
  if (!host) { alert('Enter the baseline Kowl host:port'); return; }
  if (!topicCfg.topics.length) { alert('No topics configured. Add them on the Config tab.'); return; }
  btn.disabled = true; btn.textContent = '⏳ Capturing...';
  try {
    const res  = await fetch('/api/topics/capture', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({host, count, topics: topicCfg.topics})
    });
    const data = await res.json();
    if (data.error) { alert('Capture failed: ' + data.error); }
    else {
      const card = document.getElementById('tc-cap-result');
      card.style.display = 'block';
      document.getElementById('tc-cap-result-body').innerHTML =
        `<div style="font-size:12px;color:#86efac;margin-bottom:10px">✅ Stored ${data.keys} key(s) from ${data.messages} message(s).</div>` +
        data.saved.map(s => `<div class="golden-item"><span class="golden-name">${s.key}</span><span style="color:#64748b;font-size:11px">${s.count} msg</span></div>`).join('');
      loadTopicBaselines();
    }
  } catch (e) { alert('Capture error: ' + e); }
  btn.disabled = false; btn.textContent = '📥 Capture Baseline';
}

async function compareTopics() {
  const btn  = document.getElementById('tc-cmp-btn');
  const host = document.getElementById('tc-cmp-host').value.trim();
  const count = parseInt(document.getElementById('tc-cmp-count').value) || 50;
  if (!host) { alert('Enter the target Kowl host:port'); return; }
  if (!topicCfg.topics.length) { alert('No topics configured. Add them on the Config tab.'); return; }
  btn.disabled = true; btn.textContent = '⏳ Comparing...';
  try {
    const res  = await fetch('/api/topics/compare', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({host, count, mode: topicMode, topics: topicCfg.topics,
                            golden_source: topicGoldenSource})
    });
    const data = await res.json();
    if (data.error) { alert('Compare failed: ' + data.error); }
    else { renderTopicResults(data.results || []); }
  } catch (e) { alert('Compare error: ' + e); }
  btn.disabled = false; btn.textContent = '🔍 Compare';
}

function renderTopicResults(results) {
  const body = document.getElementById('tc-results-body');
  body.innerHTML = '';
  document.getElementById('tc-cmp-result').style.display = 'block';
  document.getElementById('tc-summary').style.display = 'flex';
  document.getElementById('tc-total').textContent = results.length;
  document.getElementById('tc-pass').textContent  = results.filter(r => r.status === 'PASS').length;
  document.getElementById('tc-fail').textContent  = results.filter(r => r.status === 'FAIL').length;
  document.getElementById('tc-nob').textContent   = results.filter(r => r.status !== 'PASS' && r.status !== 'FAIL').length;
  if (!results.length) {
    body.innerHTML = '<tr><td colspan="6"><div class="no-results">No messages returned from the target topics.</div></td></tr>';
    return;
  }
  results.forEach(r => renderResultRow(r, 'tc-results-body'));
}

// ── Dashboard: Run All + Reports ───────────────────────────────────────────────
let runAllMode = 'full';
let runAllGolden = 'db';
function setRunAllMode(m) {
  runAllMode = m;
  document.getElementById('runall-mode-full').classList.toggle('active',   m === 'full');
  document.getElementById('runall-mode-schema').classList.toggle('active', m === 'schema');
}
function setRunAllGolden(src) {
  runAllGolden = src;
  ['db','isd','kowl'].forEach(s =>
    document.getElementById('runall-gs-' + s).classList.toggle('active', s === src));
  // 'since' only applies to DB-fetched flows, not the kowl topic sweep
  document.getElementById('runall-since-wrap').style.display = src === 'kowl' ? 'none' : 'block';
}

async function runAll() {
  const btn = document.getElementById('runall-btn');
  const since = datetimeLocalToISO(document.getElementById('runall-since').value);
  btn.disabled = true; btn.textContent = '⏳ Running…';
  try {
    const res = await fetch('/api/run-all', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({since, mode: runAllMode, source: runAllGolden})
    });
    const data = await res.json();
    if (data.error) { alert('Run All failed: ' + data.error); }
    else { renderRunAll(data); loadReports(); }
  } catch (e) { alert('Run All error: ' + e); }
  btn.disabled = false; btn.textContent = '🚀 Run All';
}

function drawDonut(pass, fail, other) {
  const total = pass + fail + other;
  const card = document.getElementById('runall-chart-card');
  card.style.display = total ? 'block' : 'none';
  if (!total) return;
  const pPass = pass / total * 100;
  const pFail = pPass + fail / total * 100;
  document.getElementById('ra-donut').style.background =
    `conic-gradient(#22c55e 0 ${pPass}%, #ef4444 ${pPass}% ${pFail}%, #eab308 ${pFail}% 100%)`;
  document.getElementById('ra-donut-pct').textContent = Math.round(pass / total * 100) + '%';
  document.getElementById('ra-leg-pass').textContent  = pass;
  document.getElementById('ra-leg-fail').textContent  = fail;
  document.getElementById('ra-leg-other').textContent = other;
}

function renderRunAll(data) {
  const results = data.results || [];
  document.getElementById('runall-summary').style.display = 'flex';
  document.getElementById('runall-result').style.display  = 'block';
  const np = results.filter(r=>r.status==='PASS').length;
  const nf = results.filter(r=>r.status==='FAIL').length;
  const no = results.length - np - nf;
  document.getElementById('ra-total').textContent = results.length;
  document.getElementById('ra-pass').textContent  = np;
  document.getElementById('ra-fail').textContent  = nf;
  document.getElementById('ra-other').textContent = no;
  drawDonut(np, nf, no);

  const pf = data.per_flow || {};
  document.getElementById('runall-perflow').innerHTML = Object.keys(pf).length
    ? Object.entries(pf).map(([flow,s]) =>
        `<span class="badge badge-info" style="margin-right:8px">${flow}: ${s.pass}/${s.total} pass${s.fail?`, ${s.fail} fail`:''}</span>`).join('')
    : '<span style="color:#fcd34d;font-size:12px">No subscriber IDs configured — set them on the Config tab.</span>';

  if (data.report) {
    const dl = document.getElementById('runall-download');
    dl.href = '/api/report/' + encodeURIComponent(data.report) + '?download=1';
  }

  const body = document.getElementById('runall-body');
  body.innerHTML = '';
  if (!results.length) {
    body.innerHTML = '<tr><td colspan="6"><div class="no-results">No notifications found for the configured flows.</div></td></tr>';
    return;
  }
  results.forEach(r => {
    if (r.flow) r.create_time = r.flow + ' · ' + r.create_time;
    renderResultRow(r, 'runall-body');
  });
}

async function loadReports() {
  try {
    const reports = await (await fetch('/api/reports')).json();
    const el = document.getElementById('dash-reports');
    if (!el) return;
    el.innerHTML = reports.length
      ? reports.map(rep => {
          const n = rep.name;
          const hasMeta = (rep.total !== undefined);
          const kindIcon = rep.kind === 'full_run' ? '▶' : (rep.kind === 'run_all' ? '📅' : '🗒');
          const title = hasMeta
            ? `<b>${rep.project || '(none)'}</b> · ${rep.created || ''}`
            : `${n} <span style="color:#475569">· ${rep.created || ''}</span>`;
          const counts = hasMeta
            ? `<span style="margin-left:10px;font-size:11px">
                 <span style="color:#86efac">✅ ${rep.pass}</span>
                 <span style="color:#fca5a5;margin-left:6px">❌ ${rep.fail}</span>
                 <span style="color:#94a3b8;margin-left:6px">/ ${rep.total}</span>
                 ${rep.mode ? `<span class="badge badge-info" style="margin-left:8px">${rep.mode}</span>` : ''}
               </span>`
            : '';
          const allureBtns =
            (rep.allure_html ? `<a class="btn-xs btn-xs-view" style="background:#14532d;color:#86efac" href="/api/allure-html/${rep.allure_html.replace('-html','')}/" target="_blank" title="Open Allure HTML report">📊 Allure</a>` : '') +
            (rep.allure_zip ? `<a class="btn-xs btn-xs-view" href="/api/allure/${encodeURIComponent(rep.allure_zip)}" download title="Download allure-results (.zip)">📦 .zip</a>` : '');
          return `
          <div class="golden-item">
            <span class="golden-name"><input type="checkbox" class="rep-check" value="${n}" onchange="updateReportSelCount()">${kindIcon} ${title} ${counts}</span>
            <div class="golden-actions">
              <a class="btn-xs btn-xs-view" href="/api/report/${encodeURIComponent(n)}" target="_blank">Open</a>
              <a class="btn-xs btn-xs-view" href="/api/report/${encodeURIComponent(n)}?download=1" download>Download</a>
              ${allureBtns}
              <button class="btn-xs btn-xs-del" onclick="deleteReport('${n}')">Delete</button>
            </div>
          </div>`;
        }).join('')
      : 'No reports yet.';
    updateReportSelCount();
  } catch (e) {}
}

function selectedReportNames() {
  return Array.from(document.querySelectorAll('#dash-reports .rep-check:checked')).map(c => c.value);
}
function updateReportSelCount() {
  const el = document.getElementById('rep-sel-count');
  if (el) el.textContent = selectedReportNames().length;
}
async function deleteReport(name) {
  if (!confirm('Delete report ' + name + '?')) return;
  await fetch('/api/report/' + encodeURIComponent(name), {method: 'DELETE'});
  loadReports();
}
async function deleteSelectedReports() {
  const names = selectedReportNames();
  if (!names.length) { alert('No reports selected.'); return; }
  if (!confirm(`Delete ${names.length} selected report(s)?`)) return;
  await fetch('/api/reports/delete', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({names})});
  loadReports();
}
async function deleteAllReports() {
  if (!confirm('⚠️ Delete ALL reports? This cannot be undone.')) return;
  await fetch('/api/reports/delete', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({all: true})});
  loadReports();
}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("🔔 Notification Comparator UI")
    print("   Open: http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False, threaded=True)

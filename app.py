#!/usr/bin/env python3
"""
Notification Comparator — Flask Web UI
Run: python app.py
Then open: http://localhost:5050
"""

import json
import queue
import threading
import time
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
TOPIC_DIR   = Path(__file__).parent / "topic_baseline"   # stored Setup-A topic messages
GOLDEN_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)
TOPIC_DIR.mkdir(exist_ok=True)

app = Flask(__name__)

# ─── GLOBALS FOR WATCH MODE ───────────────────────────────────────────────────

watch_state = {
    "running": False,
    "results": [],
    "log_queue": queue.Queue(),
    "thread": None,
    "mode": "full",
}

capture_state = {
    "running": False,
    "seen":    set(),
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

def golden_path(key):
    """Resolve path: golden/{FLOW_TYPE}/{key}.json, fallback to golden/{key}.json"""
    flow_type = key.split("__")[0].upper()
    subdir = GOLDEN_DIR / flow_type
    if subdir.is_dir():
        return subdir / f"{key}.json"
    return GOLDEN_DIR / f"{key}.json"

def save_golden(key, payload):
    path = golden_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str))

def load_golden(key):
    path = golden_path(key)
    # also check root golden dir as fallback for old files
    fallback = GOLDEN_DIR / f"{key}.json"
    if path.exists():
        return json.loads(path.read_text())
    elif fallback.exists():
        return json.loads(fallback.read_text())
    return None

def list_goldens():
    """List all goldens as {flow_type}/{key} for organised display."""
    results = []
    # subdir goldens: golden/PUT/PUT__created__*.json etc.
    for p in sorted(GOLDEN_DIR.rglob("*.json")):
        rel = p.relative_to(GOLDEN_DIR)
        results.append(str(rel.with_suffix("")))  # e.g. PUT/PUT__created__order_information
    return results

def extract_ext_id(raw_payload):
    """Extract externalServiceRequestId before stripping — used for grouping only."""
    try:
        data = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
        data = normalize(data)
        nd = data.get("notification_data") or data
        return nd.get("externalServiceRequestId", "")
    except Exception:
        return ""

def process_rows(rows, mode="full"):
    results = []
    for row in rows:
        try:
            ext_id  = extract_ext_id(row["payload"])
            payload = clean_payload(row["payload"])
            key     = notif_key(payload)
            golden  = load_golden(key)
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

def topic_baseline_path(key):
    return TOPIC_DIR / f"{key}.json"

def save_topic_baseline(key, payload):
    topic_baseline_path(key).write_text(json.dumps(payload, indent=2, default=str))

def load_topic_baseline(key):
    p = topic_baseline_path(key)
    return json.loads(p.read_text()) if p.exists() else None

def list_topic_baselines():
    return sorted(p.stem for p in TOPIC_DIR.glob("*.json"))

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

def compare_topics(host, topics, count, mode="full"):
    """Fetch target setup topics and diff each message against stored baselines."""
    results = []
    for spec in topics:
        label, topic = spec["label"], spec["topic"]
        for msg in fetch_topic_messages(host, topic, count):
            env = message_envelope(msg)
            if env is None:
                continue
            try:
                key      = topic_notif_key(env, label, topic)
                ext_id   = env.get("entity_id") or ""
                payload  = clean_topic_payload(env)
                baseline = load_topic_baseline(key)
                row_id   = f"p{msg.get('partitionID','?')}@{msg.get('offset','?')}"
                if baseline is None:
                    results.append({"db_id": row_id, "create_time": topic_short(topic),
                                    "key": key, "ext_id": ext_id, "status": "NO BASELINE",
                                    "findings": [], "payload": payload})
                else:
                    diff = DeepDiff(baseline, payload, ignore_order=True, verbose_level=2)
                    findings = diff_to_list(diff, mode=mode)
                    results.append({"db_id": row_id, "create_time": topic_short(topic),
                                    "key": key, "ext_id": ext_id,
                                    "status": "PASS" if not findings else "FAIL",
                                    "findings": findings, "payload": payload})
            except Exception as e:
                results.append({"db_id": "?", "create_time": topic_short(topic),
                                "key": "ERROR", "ext_id": "", "status": "ERROR",
                                "findings": [{"type": "exception", "path": "", "detail": str(e)}]})
    return results

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
                results = process_rows([row], mode=watch_state.get("mode", "full"))
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
    subscriber = int(data.get("subscriber", 158))
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
    subscriber = int(data.get("subscriber", 158))
    mode       = data.get("mode", "full")
    since      = data.get("since") or None
    ext_id     = data.get("ext_id") or None

    if not since and not ext_id:
        return jsonify({"ok": False, "error": "Provide either a time range (since) or an External Request ID"}), 400
    try:
        tunnel = open_tunnel()
        conn = connect_db(tunnel)
        cur = conn.cursor()
        rows = fetch_notifications(cur, subscriber, since=since, ext_id=ext_id)
        results = process_rows(rows, mode=mode)
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
    subscriber = int(data.get("subscriber", 158))
    interval   = int(data.get("interval", 3))
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

@app.route("/api/watch/start", methods=["POST"])
def api_watch_start():
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    # Only block if a watch thread is actually still alive — a stale "running"
    # flag from a crashed/finished thread must not wedge restarts.
    t_old = watch_state.get("thread")
    if watch_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"ok": False, "error": "Already running"}), 400
    data = request.json
    subscriber = int(data.get("subscriber", 158))
    interval   = int(data.get("interval", 3))
    watch_state["mode"]   = data.get("mode", "full")
    watch_state["ext_id"] = data.get("ext_id") or None
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
    return jsonify({"ok": True})

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
    host  = (data.get("host") or cfg.get("topic_host_b") or cfg.get("topic_host") or "").strip()
    count = int(data.get("count") or cfg.get("topic_count") or 50)
    mode  = data.get("mode", "full")
    if not host:
        return jsonify({"error": "No Kowl host configured."}), 400
    if not list_topic_baselines():
        return jsonify({"error": "No baseline stored yet. Capture a baseline first."}), 400
    try:
        results = compare_topics(host, _topics_from_request(data), count, mode=mode)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"results": results})

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
  .payload-json { background: #0a0d13; border: 1px solid #1e2235; border-radius: 6px;
                  padding: 12px; margin: 0; max-height: 420px; overflow: auto;
                  font-family: monospace; font-size: 11px; line-height: 1.5;
                  color: #94a3b8; white-space: pre; }
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
  <div class="nav-item" onclick="showPage('topics')">
    <span class="nav-icon">🧬</span><span class="nav-label">Topic Compare</span>
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
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
      <div class="card" style="cursor:pointer" onclick="showPage('capture')">
        <div class="card-title">📸 Capture Golden</div>
        <p style="font-size:13px;color:#64748b">Run a flow and save its notifications as the expected baseline. Future runs will be compared against these.</p>
      </div>
      <div class="card" style="cursor:pointer" onclick="showPage('compare')">
        <div class="card-title">🔍 Compare</div>
        <p style="font-size:13px;color:#64748b">Fetch notifications from a time range and diff them against golden snapshots. Get a pass/fail report.</p>
      </div>
      <div class="card" style="cursor:pointer" onclick="showPage('watch')">
        <div class="card-title">👁 Watch (Live)</div>
        <p style="font-size:13px;color:#64748b">Start watching, trigger your flow, see comparisons arrive in real-time as notifications land in the DB.</p>
      </div>
      <div class="card" style="cursor:pointer" onclick="showPage('goldens')">
        <div class="card-title">🗂 Golden Snapshots</div>
        <p style="font-size:13px;color:#64748b">Browse, inspect, and manage your saved golden payloads.</p>
      </div>
    </div>
    <div class="card mt" id="dash-goldens-summary">
      <div class="card-title">📊 Current Goldens</div>
      <div id="dash-golden-list" style="color:#64748b;font-size:13px">Loading...</div>
    </div>
  </div>

  <!-- CAPTURE -->
  <div class="page" id="page-capture">
    <h2>📸 Capture Golden Snapshots</h2>
    <p class="subtitle">Save notifications as the expected baseline for future comparisons.</p>

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
  </div>

  <!-- COMPARE -->
  <div class="page" id="page-compare">
    <h2>🔍 Compare Notifications</h2>
    <p class="subtitle">Diff notifications against golden snapshots.</p>

    <div class="tabs">
      <div class="tab active" id="cmp-tab-time"  onclick="switchCmpTab('time')">📅 By Time Range</div>
      <div class="tab"        id="cmp-tab-extid" onclick="switchCmpTab('extid')">🔖 By Request ID</div>
    </div>

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
  </div>

  <!-- WATCH -->
  <div class="page" id="page-watch">
    <h2>👁 Watch Mode (Live)</h2>
    <p class="subtitle">Polls DB every N seconds. Start watching, then trigger your flow.</p>
    <div class="card">
      <div class="card-title">Settings</div>

      <div class="tabs" style="margin-bottom:14px;border-bottom:1px solid #2d3148">
        <div class="tab active" id="watch-fetch-tab-time"  onclick="switchWatchFetchTab('time')">📅 By Time</div>
        <div class="tab"        id="watch-fetch-tab-extid" onclick="switchWatchFetchTab('extid')">🔖 By Request ID</div>
      </div>

      <div style="margin-bottom:14px">
        <label>Flow Type</label>
        <div class="flow-pills" id="watch-flow-pills">
          <div class="flow-pill pill-put"   data-flow="PUT"   onclick="selectFlowType('watch','PUT')">PUT</div>
          <div class="flow-pill pill-pick"  data-flow="PICK"  onclick="selectFlowType('watch','PICK')">PICK</div>
          <div class="flow-pill pill-audit" data-flow="AUDIT" onclick="selectFlowType('watch','AUDIT')">AUDIT</div>
          <div class="flow-pill pill-other" data-flow="OTHER" onclick="selectFlowType('watch','OTHER')">Other</div>
        </div>
      </div>
      <div class="flex-row">
        <div class="sub-input-wrap">
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

  <!-- TOPIC COMPARE -->
  <div class="page" id="page-topics">
    <h2>🧬 Topic Compare</h2>
    <p class="subtitle">Pull notifications from the Kowl topic viewer, store one setup as a baseline, then diff another setup against it.</p>

    <div class="tabs">
      <div class="tab active" id="tc-tab-capture" onclick="switchTopicTab('capture')">📥 Capture Baseline</div>
      <div class="tab"        id="tc-tab-compare" onclick="switchTopicTab('compare')">🔍 Compare Setup</div>
    </div>

    <!-- Capture baseline -->
    <div id="tc-panel-capture">
      <div class="card">
        <div class="card-title">Capture baseline from Setup A topics</div>
        <div class="flow-sub-row">
          <div style="flex:1;min-width:220px">
            <label>Baseline Kowl host:port</label>
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
          <button class="btn btn-success" onclick="captureTopics()" id="tc-cap-btn">📥 Capture Baseline</button>
        </div>
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

    <!-- Compare -->
    <div id="tc-panel-compare" style="display:none">
      <div class="card">
        <div class="card-title">Compare Setup B against stored baseline</div>
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

  <!-- GOLDENS -->
  <div class="page" id="page-goldens">
    <h2>🗂 Golden Snapshots</h2>
    <p class="subtitle">Saved expected payloads. Click View to inspect, Delete to remove.</p>
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
        <div class="card-title" style="margin:0">Snapshots</div>
        <button class="btn btn-ghost" onclick="loadGoldens()">↻ Refresh</button>
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
        : name === 'watch' ? 'watch' : name === 'config' ? 'config'
        : name === 'topics' ? 'topic' : 'golden'))
      n.classList.add('active');
  });
  if (name === 'goldens' || name === 'dashboard') loadGoldens();
  if (name === 'config') loadConfig();
  if (name === 'topics') initTopics();
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
          <div class="diff-row">
            <span class="diff-type">${f.type}</span>
            <span class="diff-path">${f.path}</span>
            <span class="diff-detail-text">${f.detail}</span>
          </div>`).join('')}` : '';

  const jsonBlock = r.payload ? `
        <div style="font-size:11px;font-weight:700;color:#64748b;margin:${diffBlock ? '12px' : '0'} 0 6px">
          PAYLOAD JSON
        </div>
        <pre class="payload-json">${escapeHtml(JSON.stringify(r.payload, null, 2))}</pre>` : '';

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

// ── Dashboard ─────────────────────────────────────────────────────────────────
async function loadGoldens() {
  const res = await fetch('/api/goldens');
  const keys = await res.json();
  const el = document.getElementById('goldens-list');
  const dash = document.getElementById('dash-golden-list');

  if (keys.length === 0) {
    el.innerHTML = '<div class="no-results">No golden snapshots yet. Use Capture to create them.</div>';
    if (dash) dash.textContent = 'No goldens captured yet.';
    return;
  }

  el.innerHTML = keys.map(k => `
    <div class="golden-item">
      <span class="golden-name">${k}</span>
      <div class="golden-actions">
        <button class="btn-xs btn-xs-view" onclick="viewGolden('${k}')">View</button>
        <button class="btn-xs btn-xs-del"  onclick="deleteGolden('${k}')">Delete</button>
      </div>
    </div>`).join('');

  if (dash) {
    dash.innerHTML = keys.map(k => `
      <div style="padding:4px 0;font-size:12px;font-family:monospace;color:#a5b4fc">${k}</div>`
    ).join('') + `<div style="margin-top:8px;font-size:11px;color:#4b5563">${keys.length} snapshot(s)</div>`;
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

function switchCmpTab(tab) {
  cmpFetchMode = tab;
  document.getElementById('cmp-tab-time').classList.toggle('active',  tab === 'time');
  document.getElementById('cmp-tab-extid').classList.toggle('active', tab === 'extid');
  document.getElementById('cmp-panel-time').style.display  = tab === 'time'  ? 'block' : 'none';
  document.getElementById('cmp-panel-extid').style.display = tab === 'extid' ? 'block' : 'none';
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
      body: JSON.stringify({subscriber, since, ext_id, mode: modeState.cmp})
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

async function startWatch() {
  const subscriber = document.getElementById('watch-subscriber').value;
  const interval   = document.getElementById('watch-interval').value;

  const res = await fetch('/api/watch/start', {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({
      subscriber, interval, mode: modeState.watch,
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
const modeState = { cmp: 'full', watch: 'full' };
let watchModeLocked = false;  // mode can't change mid-run — the watch thread is pinned to its start-time mode

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

function toggleMode(prefix) {
  if (prefix === 'watch' && watchModeLocked) return;  // ignore clicks during a live run
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

function switchTopicTab(tab) {
  document.getElementById('tc-tab-capture').classList.toggle('active', tab === 'capture');
  document.getElementById('tc-tab-compare').classList.toggle('active', tab === 'compare');
  document.getElementById('tc-panel-capture').style.display = tab === 'capture' ? 'block' : 'none';
  document.getElementById('tc-panel-compare').style.display = tab === 'compare' ? 'block' : 'none';
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
      body: JSON.stringify({host, count, mode: topicMode, topics: topicCfg.topics})
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
  document.getElementById('tc-nob').textContent   = results.filter(r => r.status === 'NO BASELINE').length;
  if (!results.length) {
    body.innerHTML = '<tr><td colspan="6"><div class="no-results">No messages returned from the target topics.</div></td></tr>';
    return;
  }
  results.forEach(r => renderResultRow(r, 'tc-results-body'));
}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("🔔 Notification Comparator UI")
    print("   Open: http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False, threaded=True)

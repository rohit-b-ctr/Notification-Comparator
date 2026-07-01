"""Kowl/Kafka topic capture, baselines, and comparison."""
import json
import time
from pathlib import Path

from deepdiff import DeepDiff  # type: ignore[import]

from core.config import *
from core.diffing import *
from core.golden import load_golden, save_golden, current_project, golden_path, golden_root
from core.state import kowl_capture_state

# websocket-client is only needed for Kowl topic streaming; keep it optional.
try:
    import websocket  # websocket-client  # type: ignore[import]
except ImportError:
    websocket = None

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


"""All HTTP routes, exposed as a Flask Blueprint."""
import json
import queue
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path

from deepdiff import DeepDiff  # type: ignore[import]

from flask import Blueprint, Response, jsonify, request, current_app  # type: ignore[import]

from core.config import *
from core.diffing import *
from core.db import *
from core.golden import *
from core.reports import *
from core.allure import *
from core.kowl import *
from core.isd import *
from core.live import capture_live_thread, watch_thread_fn, full_watch_thread_fn
from core.state import (
    watch_state, full_watch_state, capture_state, kowl_capture_state,
)

bp = Blueprint("api", __name__)

@bp.route("/")
def index():
    return current_app.send_static_file("index.html")

@bp.route("/api/config", methods=["GET"])
def api_get_config():
    cfg = load_config()  # disk only — no secrets
    cfg["secrets_ready"] = secrets_ready()
    return jsonify(cfg)

@bp.route("/api/config", methods=["POST"])
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

@bp.route("/api/secrets", methods=["POST"])
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

@bp.route("/api/secrets/saved", methods=["GET"])
def api_secrets_saved_status():
    return jsonify({"saved": SECRETS_PATH.exists(), "secrets_ready": secrets_ready()})

@bp.route("/api/secrets/clear", methods=["POST"])
def api_clear_saved_secrets():
    clear_saved_secrets()
    return jsonify({"ok": True})

@bp.route("/api/config/test", methods=["POST"])
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

@bp.route("/api/goldens")
def api_goldens():
    return jsonify(list_goldens())

@bp.route("/api/capture", methods=["POST"])
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

@bp.route("/api/compare", methods=["POST"])
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

@bp.route("/api/capture/live/start", methods=["POST"])
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

@bp.route("/api/capture/live/stop", methods=["POST"])
def api_capture_live_stop():
    capture_state["running"] = False
    return jsonify({"ok": True, "saved": list(capture_state.get("saved", {}).keys())})

@bp.route("/api/capture/live/stream")
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

@bp.route("/api/kowl-capture/start", methods=["POST"])
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

@bp.route("/api/kowl-capture/stop", methods=["POST"])
def api_kowl_capture_stop():
    kowl_capture_state["running"] = False
    return jsonify({"ok": True, "saved": sorted(kowl_capture_state.get("saved", {}).keys())})

@bp.route("/api/kowl-capture/stream")
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

@bp.route("/api/watch/start", methods=["POST"])
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

@bp.route("/api/watch/stop", methods=["POST"])
def api_watch_stop():
    watch_state["running"] = False
    return jsonify({"ok": True})

@bp.route("/api/watch/stream")
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

@bp.route("/api/full-run/start", methods=["POST"])
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

@bp.route("/api/full-run/stop", methods=["POST"])
def api_full_run_stop():
    full_watch_state["running"] = False
    return jsonify({"ok": True})

@bp.route("/api/full-run/stream")
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

@bp.route("/api/golden/<path:key>")
def api_get_golden(key):
    # key may be "PUT/PUT__complete__PROCESSED" or just "PUT__complete__PROCESSED"
    path = GOLDEN_DIR / f"{key}.json"
    if not path.exists():
        return jsonify({"error": "not found"}), 404
    return jsonify(json.loads(path.read_text()))

@bp.route("/api/golden/<path:key>", methods=["DELETE"])
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

@bp.route("/api/goldens/delete", methods=["POST"])
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

@bp.route("/api/topics/baselines")
def api_topic_baselines():
    return jsonify(list_topic_baselines())

@bp.route("/api/topics/baseline/<path:key>", methods=["DELETE"])
def api_delete_topic_baseline(key):
    p = topic_baseline_path(key)
    if p.exists():
        p.unlink()
    return jsonify({"ok": True})

@bp.route("/api/topics/baseline/<path:key>")
def api_get_topic_baseline(key):
    data = load_topic_baseline(key)
    if data is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(data)

@bp.route("/api/topics/capture", methods=["POST"])
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

@bp.route("/api/topics/compare", methods=["POST"])
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

@bp.route("/api/compare/json", methods=["POST"])
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

@bp.route("/api/compare/xml", methods=["POST"])
def api_compare_xml():
    """Diff two arbitrary XML documents pasted/uploaded by the user.

    XML is parsed to a dict (via xmltodict) and then run through the very same
    DeepDiff pipeline as the JSON comparator, so 'full'/'schema' modes,
    ignore-dynamic, and the mark-value-diffs-as-pass UI all work identically.
    """
    data = request.get_json(force=True) or {}
    mode = data.get("mode", "full")
    raw_a, raw_b = data.get("a"), data.get("b")

    try:
        obj_a = xml_to_obj(raw_a)
        obj_b = xml_to_obj(raw_b)
    except ValueError as e:
        # Make it clear which side failed when possible.
        return jsonify({"error": f"XML parse error: {e}"}), 400

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

@bp.route("/api/projects")
def api_projects():
    return jsonify({"current": current_project(), "projects": list_projects()})

@bp.route("/api/golden/from-isd", methods=["POST"])
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

@bp.route("/api/golden/from-json", methods=["POST"])
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

@bp.route("/api/run-all", methods=["POST"])
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

@bp.route("/api/reports")
def api_reports():
    return jsonify(list_reports_meta())

@bp.route("/api/report/<path:name>")
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

@bp.route("/api/allure/status")
def api_allure_status():
    """Report whether the allure CLI (and a JRE) are available for HTML generation."""
    allure = shutil.which("allure")
    java = shutil.which("java")
    return jsonify({
        "cli": bool(allure),
        "java": bool(java),
        "html_capable": bool(allure and java),
    })

@bp.route("/api/allure/<path:name>")
def api_get_allure_zip(name):
    """Download the allure-results .zip produced by a Full Run."""
    p = REPORTS_DIR / name
    if not p.exists() or p.suffix != ".zip":
        return jsonify({"error": "not found"}), 404
    return Response(p.read_bytes(), mimetype="application/zip",
                    headers={"Content-Disposition": f'attachment; filename="{name}"'})

@bp.route("/api/allure-html/<run_id>/")
@bp.route("/api/allure-html/<run_id>/<path:sub>")
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

@bp.route("/api/report/<path:name>", methods=["DELETE"])
def api_delete_report(name):
    return jsonify({"ok": _delete_report(name)})

@bp.route("/api/reports/delete", methods=["POST"])
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

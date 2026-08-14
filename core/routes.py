"""All HTTP routes, exposed as a Flask Blueprint."""
import json
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
    topic_capture_state, topic_compare_state, Broadcaster,
)

bp = Blueprint("api", __name__)

@bp.route("/")
def index():
    return current_app.send_static_file("index.html")

@bp.route("/api/runtime/status")
def api_runtime_status():
    """Lets the frontend re-attach to still-running jobs after a page refresh
    instead of showing them as stopped (the backend threads keep running
    independently of the browser tab)."""
    return jsonify({
        "capture":        bool(capture_state["running"]),
        "kowl_capture":   bool(kowl_capture_state["running"]),
        "watch":          bool(watch_state["running"]),
        "full_watch":     bool(full_watch_state["running"]),
        "topic_capture":  bool(topic_capture_state["running"]),
        "topic_compare":  bool(topic_compare_state["running"]),
    })

@bp.route("/api/config", methods=["GET"])
def api_get_config():
    cfg = load_config()  # disk only — no secrets
    cfg["secrets_ready"] = secrets_ready()
    # DB passwords are decrypted in-memory at startup — surfaced here (not
    # persisted, and stripped again by save_config()'s SECRET_FIELDS filter if
    # this response gets POSTed straight back) so the Config form doesn't blank
    # them out on every reload/refresh.
    cfg["db_pass"]   = RUNTIME_SECRETS.get("db_pass", "")
    cfg["db_pass_b"] = RUNTIME_SECRETS.get("db_pass_b", "")
    return jsonify(cfg)

@bp.route("/api/config", methods=["POST"])
def api_save_config():
    data = request.json
    # Merge on top of the RAW on-disk JSON, not load_config() — load_config()
    # deliberately strips db_pass_enc/db_pass_b_enc (so the browser never sees
    # them), but that stripped copy must never be written back to disk, or
    # every plain config save (any Save button, not just DB) silently erases
    # the encrypted DB passwords. The incoming payload never carries those
    # keys either, so they pass through untouched here.
    current = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else dict(DEFAULT_CONFIG)
    merged  = {**current, **{k: v for k, v in data.items() if k not in SECRET_FIELDS}}
    for key in ("poll_interval", "topic_count"):
        try:
            merged[key] = int(merged[key])
        except (ValueError, KeyError, TypeError):
            pass
    save_config(merged)
    return jsonify({"ok": True})

def _export_subset(fields):
    cfg = load_config()
    return {k: cfg.get(k) for k in fields}

def _import_subset(fields, data):
    if not isinstance(data, dict):
        return False, "Invalid config file — expected a JSON object"
    # Same reasoning as api_save_config(): merge on the raw disk JSON, not
    # load_config(), so this write never drops the encrypted DB password blobs.
    current = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else dict(DEFAULT_CONFIG)
    for k in fields:
        if k in data:
            current[k] = data[k]
    for key in ("poll_interval", "topic_count"):
        if key in fields:
            try:
                current[key] = int(current[key])
            except (ValueError, KeyError, TypeError):
                pass
    save_config(current)
    return True, None

@bp.route("/api/config/export/db")
def api_export_db_config():
    """DB/SSH-side config (hosts, patterns, project, ssh_key) — for download/backup.
    Includes the encrypted db_pass*_enc blobs as ciphertext (still only
    decryptable on a machine holding this app's .config_key)."""
    data = _export_subset(DB_CONFIG_FIELDS)
    raw = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
    for f in DB_CONFIG_ENC_FIELDS:
        if raw.get(f):
            data[f] = raw[f]
    return jsonify(data)

@bp.route("/api/config/import/db", methods=["POST"])
def api_import_db_config():
    """Merge an uploaded DB config backup into the current config — only the
    DB/SSH fields (+ encrypted password blobs, if present) are touched, Kowl
    settings are left as-is."""
    data = request.json
    ok, err = _import_subset(DB_CONFIG_FIELDS, data)
    if not ok:
        return jsonify({"ok": False, "error": err}), 400
    if isinstance(data, dict) and any(f in data for f in DB_CONFIG_ENC_FIELDS):
        current = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
        for f in DB_CONFIG_ENC_FIELDS:
            if f in data:
                current[f] = data[f]
        save_config(current)
        load_saved_secrets()  # decrypt the newly-imported blobs into RUNTIME_SECRETS
    return jsonify({"ok": True})

@bp.route("/api/config/export/kowl")
def api_export_kowl_config():
    """Kowl/Kafka topic config only — for download/backup."""
    return jsonify(_export_subset(KOWL_CONFIG_FIELDS))

@bp.route("/api/config/import/kowl", methods=["POST"])
def api_import_kowl_config():
    """Merge an uploaded Kowl config backup into the current config — only the
    Kowl fields are touched, DB/SSH settings are left as-is."""
    ok, err = _import_subset(KOWL_CONFIG_FIELDS, request.json)
    if not ok:
        return jsonify({"ok": False, "error": err}), 400
    return jsonify({"ok": True})

@bp.route("/api/secrets", methods=["POST"])
def api_set_secrets():
    """Store DB passwords in memory. Optionally persist (encrypted) to config.json.
    ssh_key is not a secret — it's a plain field saved via the main /api/config."""
    data = request.json
    db_pass   = data.get("db_pass", "")
    db_pass_b = data.get("db_pass_b", "")
    if db_pass:
        RUNTIME_SECRETS["db_pass"] = db_pass
    if db_pass_b:
        RUNTIME_SECRETS["db_pass_b"] = db_pass_b
    if data.get("save_to_disk") and RUNTIME_SECRETS.get("db_pass"):
        save_secrets_to_disk(RUNTIME_SECRETS["db_pass"], RUNTIME_SECRETS.get("db_pass_b", ""))
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
    target = bool((request.json or {}).get("target"))
    try:
        cfg = get_cfg()
        tunnel = open_tunnel(cfg, target=target)
        conn = connect_db(tunnel, cfg, target=target)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close(); conn.close(); tunnel.stop()
        which = "target" if target else "baseline"
        return jsonify({"ok": True, "msg": f"✅ PostgreSQL connection successful ({which})"})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})

@bp.route("/api/config/test-kowl", methods=["POST"])
def api_test_kowl():
    cfg    = load_config()
    target = bool((request.json or {}).get("target"))
    which  = "target" if target else "baseline"
    host   = (cfg.get("topic_host_b" if target else "topic_host") or "").strip()
    if not host:
        return jsonify({"ok": False, "msg": f"⚠️ No Kowl {which} host configured"})
    try:
        import requests  # type: ignore[import]
        base = host.rstrip("/")
        if not base.startswith(("http://", "https://")):
            base = "https://" + base
        resp = requests.get(f"{base}/api/topics", timeout=8, verify=False)
        if resp.status_code == 200:
            return jsonify({"ok": True, "msg": f"✅ Kowl reachable at {host} ({which})"})
        return jsonify({"ok": False, "msg": f"Kowl returned HTTP {resp.status_code}"})
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Kowl connection failed: {e}"})

@bp.route("/api/goldens")
def api_goldens():
    return jsonify(list_goldens())

@bp.route("/api/capture", methods=["POST"])
def api_capture():
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    data = request.json
    patterns = data.get("patterns")
    if patterns is None:  # back-compat with the old single-pattern payload
        patterns = [data.get("pattern")] if data.get("pattern") else []
    patterns = [p.strip() for p in patterns if p and p.strip()]
    if not patterns:
        return jsonify({"ok": False, "error": "Enter at least one pattern"}), 400
    since      = data.get("since")  or None
    ext_id     = data.get("ext_id") or None
    tunnel = None
    try:
        cfg = get_cfg()
        tunnel = open_tunnel()
        conn = connect_db(tunnel)
        cur = conn.cursor()
        saved, errors, total_fetched = {}, [], 0
        for pattern in patterns:
            label = label_for_pattern(cfg, pattern)
            sub_ids = resolve_subscriber_ids(cur, [pattern])
            if not sub_ids:
                errors.append(f"No subscriber found for pattern '{pattern}'")
                continue
            # No since/ext_id given → the "leave blank for last 100" default.
            fetch_limit = 100 if (not since and not ext_id) else 300
            rows = fetch_notifications(cur, sub_ids, since=since, ext_id=ext_id, limit=fetch_limit)
            total_fetched += len(rows)
            for row in rows:
                try:
                    payload = clean_payload(row["payload"])
                    key = notif_key(payload)
                    dedup_key = f"{label}/{key}"
                    if dedup_key not in saved:
                        save_golden(key, payload, label=label)
                        saved[dedup_key] = True
                except Exception:
                    pass  # silently skip malformed rows
        cur.close(); conn.close()
        return jsonify({"ok": True, "saved": list(saved.keys()), "total_fetched": total_fetched, "errors": errors})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if tunnel: tunnel.stop()

@bp.route("/api/compare", methods=["POST"])
def api_compare():
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    data     = request.json
    raw_patterns = data.get("patterns")
    if raw_patterns is None:  # back-compat with the old single-`pattern` callers
        raw_patterns = [data.get("pattern")]
    patterns = [p.strip() for p in raw_patterns if (p or "").strip()]
    if not patterns:
        return jsonify({"ok": False, "error": "Enter at least one pattern first"}), 400
    mode       = data.get("mode", "full")
    since      = data.get("since") or None
    ext_id     = data.get("ext_id") or None
    gsource    = data.get("golden_source") or "db"   # db | isd (kowl handled via /api/topics/compare)

    if not since and not ext_id:
        return jsonify({"ok": False, "error": "Provide either a time range (since) or an External Request ID"}), 400
    tunnel = None
    try:
        cfg = get_cfg()
        tunnel = open_tunnel(target=True)
        conn = connect_db(tunnel, target=True)
        cur = conn.cursor()
        all_results, missing = [], []
        for pattern in patterns:
            sub_ids = resolve_subscriber_ids(cur, [pattern])
            if not sub_ids:
                missing.append(pattern)
                continue
            label = label_for_pattern(cfg, pattern)
            rows = fetch_notifications(cur, sub_ids, since=since, ext_id=ext_id)
            results = process_rows(rows, mode=mode, source=gsource, label=label)
            for r in results:
                r["flow"] = label
            all_results.extend(results)
        cur.close(); conn.close()
        if not all_results and len(missing) == len(patterns):
            return jsonify({"ok": False, "error": f"No subscriber found for pattern(s): {', '.join(missing)}"}), 400
        all_results.sort(key=lambda r: r.get("db_id") or 0)
        all_results, skipped_repeats = dedupe_by_key(all_results)
        return jsonify({"ok": True, "results": all_results, "total": len(all_results),
                         "skipped_repeats": skipped_repeats, "missing_patterns": missing})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if tunnel: tunnel.stop()


# ─── SUBSCRIBER SNAPSHOT ROUTES (baseline subscriber row per pattern, vs target) ──

@bp.route("/api/subscriber/goldens")
def api_subscriber_goldens():
    return jsonify(list_subscriber_goldens())

@bp.route("/api/subscriber/capture", methods=["POST"])
def api_subscriber_capture():
    """Snapshot every subscriber row that actually exists in the baseline env
    (the whole `subscriber` table, not just patterns typed into the Config
    tab) and store one golden per pattern under
    golden/{project}/subscriber/{label}.json."""
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    cfg = get_cfg()
    # Config-tab patterns are only consulted for nicer folder names (e.g.
    # "Put_Success" instead of the raw pattern string) — capture itself is no
    # longer limited to them.
    label_by_pattern = {
        (e.get("pattern") or "").strip(): (e.get("label") or e.get("pattern") or "").strip()
        for e in cfg.get("patterns", []) if (e.get("pattern") or "").strip()
    }
    tunnel = None
    try:
        tunnel = open_tunnel(cfg, target=False)
        conn = connect_db(tunnel, cfg, target=False)
        cur = conn.cursor()
        rows = fetch_all_subscribers(cur)
        if not rows:
            return jsonify({"ok": False, "error": "No subscriber rows found in the baseline environment."}), 400
        rows_by_pattern = {}
        for row in rows:
            pattern = (row.get("pattern") or "").strip()
            if not pattern:
                continue
            rows_by_pattern.setdefault(pattern, []).append(row)
        saved, errors = [], []
        for pattern, prows in rows_by_pattern.items():
            label = label_by_pattern.get(pattern, pattern)
            # Always store a list, even for the (usual) single-row match — if
            # the row count for this pattern ever differs between capture and
            # compare (e.g. a duplicate subscriber row appears/disappears),
            # comparing a bare dict against a list would produce a wholesale
            # type mismatch instead of a meaningful field diff.
            save_subscriber_golden(label, prows)
            saved.append(label)
        cur.close(); conn.close()
        return jsonify({"ok": True, "saved": saved, "errors": errors})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if tunnel: tunnel.stop()

@bp.route("/api/subscriber/compare", methods=["POST"])
def api_subscriber_compare():
    """Diff every pattern captured by /api/subscriber/capture (i.e. every
    pattern that existed in the baseline env at capture time — not just the
    ones typed into the Config tab) against its target-env subscriber row."""
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    cfg = get_cfg()
    labels = list_subscriber_goldens()
    if not labels:
        return jsonify({"ok": False, "error": "No subscriber snapshots captured yet. Run Capture Golden → 👤 Subscriber first."}), 400
    tunnel = None
    try:
        tunnel = open_tunnel(cfg, target=True)
        conn = connect_db(tunnel, cfg, target=True)
        cur = conn.cursor()
        results = []
        for label in labels:
            golden = load_subscriber_golden(label)
            golden_rows = golden if isinstance(golden, list) else [golden]
            # The pattern lives inside the captured row itself, not in Config —
            # capture snapshots whatever patterns existed in the baseline env,
            # so that's the only reliable source for which pattern this golden is.
            pattern = (golden_rows[0].get("pattern") or "").strip() if golden_rows else ""
            if not pattern:
                results.append({"label": label, "pattern": "", "status": "NO GOLDEN", "findings": [], "fields": []})
                continue
            rows = fetch_subscriber_details(cur, [pattern])
            if not rows:
                # Pattern exists in the baseline golden but has no subscriber row in
                # the target env at all — surface it as its own status (not a field
                # diff FAIL) and show the baseline's subscriber details directly,
                # since there's nothing on the target side to diff against.
                missing_fields = [
                    {"path": p, "baseline": v, "target": "NOT FOUND", "status": "fail"}
                    for p, v in sorted(flatten_dict(normalize(golden_rows)).items())
                ]
                results.append({"label": label, "pattern": pattern, "status": "MISSING IN TARGET", "findings": [
                    {"type": "Missing Field", "path": "subscriber",
                     "detail": "No subscriber found in the target environment for this pattern."}
                ], "fields": missing_fields})
                continue
            # Always a list — see the matching comment in api_subscriber_capture.
            actual = rows
            # Round-trip actual through JSON first — golden was saved (and reloaded)
            # via json.dumps(default=str)/json.loads, so its datetimes are already
            # strings; the live psycopg2 row still has raw datetime objects. Without
            # this, every timestamp column reports a false "type changed" on every
            # compare regardless of whether the value actually drifted.
            actual = json.loads(json.dumps(actual, default=str))
            # Normalize both sides to a list before diffing — older goldens
            # captured before this fix are a bare dict (single-row shortcut);
            # comparing that against a list (or a row count that changed
            # between capture and compare) would otherwise report a wholesale
            # type mismatch instead of a meaningful field diff.
            actual_rows = actual if isinstance(actual, list) else [actual]
            # Derive the pass/fail verdict from the exact same field-by-field
            # comparison rendered in the table below, instead of running a
            # second, independent DeepDiff pass over strip_dynamic-ed data.
            # Two separate diff engines could (and did) disagree on edge
            # cases — e.g. a field that's null on one side and a populated
            # object on the other — showing red "fail" cells in the table
            # while the overall verdict still said PASS.
            fields = side_by_side_fields(golden_rows, actual_rows)
            findings = [
                {"type": "type changes" if f["status"] == "fail" else "values changed",
                 "path": f["path"], "detail": f"{f['baseline']!r} → {f['target']!r}"}
                for f in fields if f["status"] != "same"
            ]
            status = "FAIL" if any(f["status"] == "fail" for f in fields) else "PASS"
            results.append({"label": label, "pattern": pattern,
                             "status": status, "findings": findings, "fields": fields})
        cur.close(); conn.close()

        # Build a shareable/downloadable HTML report (separate "kind" from
        # Full Run / Topic Compare so it gets its own section, not mixed into
        # the main Past Reports list) with the same side-by-side field view.
        meta = {
            "Project": current_project() or "(none)",
            "Run at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Patterns": ", ".join(r["label"] for r in results) or "none",
        }
        report_name = save_report(build_subscriber_report(results, "Subscriber Compare Report", meta),
                                  prefix="subscriber_compare")
        items = [{"label": r["label"], "pattern": r["pattern"], "status": r["status"]} for r in results]
        save_report_meta(report_name, results, project=current_project(), kind="subscriber_compare", items=items)

        return jsonify({"ok": True, "results": results, "report": report_name})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        if tunnel: tunnel.stop()

@bp.route("/api/capture/live/start", methods=["POST"])
def api_capture_live_start():
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    t_old = capture_state.get("thread")
    if capture_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"ok": False, "error": "Already running"}), 400
    data = request.json
    patterns = data.get("patterns")
    if patterns is None:  # back-compat with the old single-pattern payload
        patterns = [data.get("pattern")] if data.get("pattern") else []
    patterns = [p.strip() for p in patterns if p and p.strip()]
    if not patterns:
        return jsonify({"ok": False, "error": "Enter at least one pattern"}), 400
    interval   = parse_int(data.get("interval"), 3)
    ext_id     = data.get("ext_id") or None
    t = threading.Thread(target=capture_live_thread, args=(patterns, interval, ext_id), daemon=True)
    capture_state["running"] = True
    capture_state["seen"]    = set()
    capture_state["saved"]   = {}
    capture_state["log_queue"] = Broadcaster()
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
        idx = 0
        bus = capture_state["log_queue"]
        while True:
            idx, item = bus.get_from(idx)
            if item is None:
                yield 'data: {"type":"ping"}\n\n'
                continue
            yield f"data: {json.dumps(item)}\n\n"
            if item.get("type") == "done":
                break
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
    kowl_capture_state["log_queue"] = Broadcaster()
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
        idx = 0
        bus = kowl_capture_state["log_queue"]
        while True:
            idx, item = bus.get_from(idx)
            if item is None:
                yield 'data: {"type":"ping"}\n\n'
                continue
            yield f"data: {json.dumps(item)}\n\n"
            if item.get("type") == "done":
                break
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
    pattern = ""
    if origin == "db":
        if not secrets_ready():
            return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
        pattern = (data.get("pattern") or "").strip()
        if not pattern:
            return jsonify({"ok": False, "error": "Enter a pattern first"}), 400
    watch_state["mode"]   = data.get("mode", "full")
    watch_state["ext_id"] = data.get("ext_id") or None
    watch_state["source"] = golden
    watch_state["data_source"] = origin
    watch_state["running"] = True  # set before start() so a fast stop() wins the race
    t = threading.Thread(target=watch_thread_fn, args=(pattern, interval), daemon=True)
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
        idx = 0
        bus = watch_state["log_queue"]
        while True:
            idx, item = bus.get_from(idx)
            if item is None:
                yield 'data: {"type":"ping"}\n\n'
                continue
            yield f"data: {json.dumps(item)}\n\n"
            if item.get("type") == "done":
                break
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
    interval = parse_int(data.get("interval"), 3)
    full_watch_state["mode"] = data.get("mode", "full")
    full_watch_state["source"] = golden
    full_watch_state["data_source"] = origin
    full_watch_state["log_queue"] = Broadcaster()
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
        idx = 0
        bus = full_watch_state["log_queue"]
        while True:
            idx, item = bus.get_from(idx)
            if item is None:
                yield 'data: {"type":"ping"}\n\n'
                continue
            yield f"data: {json.dumps(item)}\n\n"
            if item.get("type") == "done":
                break
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

@bp.route("/api/topics/capture/start", methods=["POST"])
def api_topics_capture_start():
    if topic_capture_state["running"]:
        return jsonify({"error": "Capture already running"}), 400
    data   = request.get_json(force=True) or {}
    cfg    = load_config()
    host   = (data.get("host") or cfg.get("topic_host") or "").strip()
    count  = int(data.get("count") or cfg.get("topic_count") or 50)
    prefix = (data.get("prefix") or cfg.get("topic_prefix") or "").strip()
    topics = apply_prefix(_topics_from_request(data), prefix)
    if not host:
        return jsonify({"error": "No Kowl host configured."}), 400
    if not topics:
        return jsonify({"error": "No topics configured."}), 400
    topic_capture_state["running"]   = True
    topic_capture_state["log_queue"] = Broadcaster()
    t = threading.Thread(target=capture_topics_thread,
                         args=(host, topics, count, topic_capture_state), daemon=True)
    topic_capture_state["thread"] = t
    t.start()
    return jsonify({"ok": True, "total": len(topics)})

@bp.route("/api/topics/capture/stop", methods=["POST"])
def api_topics_capture_stop():
    topic_capture_state["running"] = False
    return jsonify({"ok": True})

@bp.route("/api/topics/capture/stream")
def api_topics_capture_stream():
    def generate():
        idx = 0
        bus = topic_capture_state["log_queue"]
        while True:
            idx, item = bus.get_from(idx)
            if item is None:
                yield 'data: {"type":"ping"}\n\n'
                continue
            yield f"data: {json.dumps(item)}\n\n"
            if item.get("type") in ("done", "error"):
                break
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

def _compare_topics_thread(host, topics, count, mode, gsource, state):
    log = state["log_queue"]
    started_dt = datetime.now()
    try:
        results = compare_topics(host, topics, count, mode=mode, golden_source=gsource, state=state)
        report = _generate_topic_report(results, mode, gsource, started_dt)
        log.put({"type": "done", "results": results, **report})
    except Exception as e:
        log.put({"type": "error", "msg": str(e)})
    finally:
        state["running"] = False

def _generate_topic_report(results, mode, gsource, started_dt):
    """Build an HTML + Allure report for a topic compare run, mirroring Full Run."""
    per_flow = {}
    for r in results:
        f = r.get("flow", "OTHER")
        s = per_flow.setdefault(f, {"total": 0, "pass": 0, "fail": 0})
        s["total"] += 1
        if r.get("status") == "PASS":
            s["pass"] += 1
        elif r.get("status") == "FAIL":
            s["fail"] += 1
    stopped_dt = datetime.now()
    started = started_dt.strftime("%Y-%m-%d %H:%M:%S")
    stopped = stopped_dt.strftime("%Y-%m-%d %H:%M:%S")
    project = current_kowl_project()
    meta = {
        "Project": project or "(none)",
        "Golden source": gsource,
        "Mode": mode,
        "Started": started,
        "Stopped": stopped,
        "Flows": ", ".join(f"{k}({v['pass']}/{v['total']})" for k, v in per_flow.items()) or "none",
    }
    report_name = save_report(build_html_report(results, "Topic Compare Report", meta), prefix="topic_compare")
    allure = {"zip": None, "html": None}
    try:
        allure = generate_allure(results, meta, started_dt, stopped_dt)
    except Exception:
        pass
    save_report_meta(report_name, results, project=project, mode=mode,
                     per_flow=per_flow, created=stopped, kind="topic_compare",
                     allure_zip=allure.get("zip"), allure_html=allure.get("html"))
    return {"report": report_name, "allure_zip": allure.get("zip"), "allure_html": allure.get("html")}

@bp.route("/api/topics/debug")
def api_topics_debug():
    """Return first 5 raw WebSocket frames from Kowl for a topic — for diagnosing 0-message issues."""
    from core.kowl import _normalise_host, fetch_topic_messages
    import websocket, json as _json, ssl as _ssl, time as _time
    cfg  = get_cfg()
    host = (cfg.get("topic_host") or "").strip()
    topics = cfg.get("topics", [])
    topic  = request.args.get("topic") or (topics[0]["topic"] if topics else "")
    if not host or not topic:
        return jsonify({"error": "host or topic not configured"})
    _, ws_base, is_tls = _normalise_host(host)
    ws_url = f"{ws_base}/api/topics/{topic}/messages"
    req_body = {"topicName": topic, "startOffset": -1, "startTimestamp": 0,
                "partitionId": -1, "maxResults": 5, "filterInterpreterCode": ""}
    frames, error = [], None
    try:
        sslopt = {"cert_reqs": _ssl.CERT_NONE} if is_tls else {}
        ws = websocket.create_connection(ws_url, timeout=10, sslopt=sslopt,
                                         header={"Origin": f"http://{ws_base.split('://',1)[-1]}"})
        ws.send(_json.dumps(req_body))
        ws.settimeout(5)
        start = _time.time()
        while _time.time() - start < 10 and len(frames) < 8:
            try:
                raw = ws.recv()
                o = _json.loads(raw)
                frames.append(o)
                if o.get("type") in ("done", "error"):
                    break
            except Exception as e:
                error = str(e); break
        ws.close()
    except Exception as e:
        error = str(e)
    return jsonify({"ws_url": ws_url, "frames": frames, "error": error})

@bp.route("/api/topics/compare/stop", methods=["POST"])
def api_topics_compare_stop():
    topic_compare_state["running"] = False
    topic_compare_state["log_queue"].put({"type": "error", "msg": "Stopped by user."})
    return jsonify({"ok": True})

@bp.route("/api/topics/compare/start", methods=["POST"])
def api_topics_compare_start():
    t_old = topic_compare_state.get("thread")
    if topic_compare_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"error": "Compare already running"}), 400
    data    = request.get_json(force=True) or {}
    cfg     = load_config()
    host    = (data.get("host") or cfg.get("topic_host_b") or cfg.get("topic_host") or "").strip()
    count   = int(data.get("count") or cfg.get("topic_count") or 50)
    mode    = data.get("mode", "full")
    gsource = data.get("golden_source") or "kowl"
    prefix  = (data.get("prefix") or cfg.get("topic_prefix_b") or "").strip()
    topics  = apply_prefix(_topics_from_request(data), prefix)
    if not host:
        return jsonify({"error": "No Kowl host configured."}), 400
    if not topics:
        return jsonify({"error": "No topics configured."}), 400
    if gsource == "kowl" and not list_topic_baselines():
        return jsonify({"error": "No kowl baseline stored yet. Capture one first."}), 400
    topic_compare_state["running"]   = True
    topic_compare_state["log_queue"] = Broadcaster()
    t = threading.Thread(target=_compare_topics_thread,
                         args=(host, topics, count, mode, gsource, topic_compare_state), daemon=True)
    topic_compare_state["thread"] = t
    t.start()
    return jsonify({"ok": True, "total": len(topics)})

@bp.route("/api/topics/compare/stream")
def api_topics_compare_stream():
    def generate():
        idx = 0
        bus = topic_compare_state["log_queue"]
        while True:
            idx, item = bus.get_from(idx)
            if item is None:
                yield 'data: {"type":"ping"}\n\n'
                continue
            yield f"data: {json.dumps(item)}\n\n"
            if item.get("type") in ("done", "error"):
                break
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

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
        "status": status_from_findings(findings),
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
        "status": status_from_findings(findings),
        "findings": findings,
        "count": len(findings),
        "payload": b,
    })

@bp.route("/api/compare/text", methods=["POST"])
def api_compare_text():
    """Line-by-line diff of two arbitrary plain-text blobs pasted/uploaded by
    the user — for unstructured content (logs, request bodies, config files)
    that doesn't parse as JSON/XML."""
    data = request.get_json(force=True) or {}
    raw_a, raw_b = data.get("a") or "", data.get("b") or ""
    rows, added, removed, changed = text_diff_rows(raw_a, raw_b, ignore_whitespace=bool(data.get("ignore_whitespace")))
    return jsonify({
        "status": "PASS" if not (added or removed or changed) else "FAIL",
        "rows": rows,
        "added": added, "removed": removed, "changed": changed,
        "count": added + removed + changed,
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
    project_kind = request.form.get("project_kind", "db")
    if project_kind not in ("db", "kowl"):
        project_kind = "db"
    # Scope extraction to configured patterns/topics — see extract_topic_anchored_objects()
    # in core/isd.py — so unrelated example JSON in the doc is never captured,
    # and real configured notifications aren't lost among that noise. Each
    # entry carries its configured label through so same-looking placeholder
    # examples from different notifications don't collapse into one golden.
    cfg = load_config()
    # kind tags each match so resolve_golden_target() knows whether it came
    # from a Kowl topic or a DB pattern without guessing from the JSON's
    # shape — an ISD doc's "JSON Payload" is often just the flat notification
    # body (no Kowl envelope wrapper), which shape detection alone can't
    # distinguish from a DB notification.
    known = [(p.get("label", ""), p.get("pattern", ""), "db") for p in cfg.get("patterns", [])] + \
            [(t.get("label", ""), t.get("topic", ""), "kowl") for t in cfg.get("topics", [])]
    try:
        parsed = parse_isd_pdf(data, filename=f.filename, known=known)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    scoped = parsed.get("scoped", False)
    if scoped:
        saved = capture_isd_goldens_labeled(parsed["labeled_payloads"], project_kind=project_kind)
        parsed_ok = len(parsed["labeled_payloads"])
    else:
        saved = capture_isd_goldens(parsed["payloads"], project_kind=project_kind)
        parsed_ok = len(parsed["payloads"])
    attempts = parsed.get("attempts", 0)
    return jsonify({
        "project": current_kowl_project() if project_kind == "kowl" else current_project(),
        "project_kind": project_kind,
        "isd_project_hint": parsed.get("project", ""),
        "pages": parsed.get("pages", 0),
        "scoped": scoped,  # True if capture was limited to configured patterns/topics
        "blocks_seen": attempts,            # balanced {...} blocks found in the PDF
        "blocks_parsed": parsed_ok,         # of those, valid JSON after repair
        "blocks_unparseable": max(0, attempts - parsed_ok),
        "failed_blocks": parsed.get("failed_blocks", []),  # page + preview for each unparseable block
        "saved": saved,
        "keys": len(saved),
    })

@bp.route("/api/golden/from-json", methods=["POST"])
def api_golden_from_json():
    """Save golden(s) from pasted JSON — one payload, an array, or several
    concatenated objects. Used for ISD payloads the PDF parser can't extract,
    and also accepts a raw Kowl message/envelope pasted directly. Each object
    is filed as a real db or kowl golden (see capture_isd_goldens ->
    resolve_golden_target in core/isd.py) — no separate isd bucket — so it's
    found by the ordinary DB/Kowl compare, filling gaps only (never
    overwriting a golden a real live capture already produced)."""
    data = request.get_json(force=True) or {}
    raw = (data.get("text") or "").strip()
    if not raw:
        return jsonify({"error": "Paste one or more JSON payloads."}), 400
    project_kind = data.get("project_kind", "db")
    if project_kind not in ("db", "kowl"):
        project_kind = "db"
    # Try: whole thing as JSON (object or array), else scan for embedded objects.
    objs = []
    try:
        parsed = json.loads(raw)
        objs = parsed if isinstance(parsed, list) else [parsed]
    except Exception:
        found, _, _ = extract_json_objects(clean_isd_text(raw))
        objs = found
    if not objs:
        return jsonify({"error": "No valid JSON found. Use Beautify to spot the syntax error."}), 400

    saved = capture_isd_goldens(objs, project_kind=project_kind)
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
            prefix  = (cfg.get("topic_prefix_b") or "").strip()
            topics  = apply_prefix(cfg.get("topics", []), prefix)
            results = compare_topics(host, topics, count, mode=mode)
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
    if not target.is_relative_to(base) or not target.exists():
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
    """Bulk delete reports: by explicit names, or all — optionally scoped so
    Past Reports' "Delete All" and Subscriber Compare Reports' "Delete All"
    stay independent of each other instead of one wiping both sections.
    - kind: only delete reports whose meta "kind" equals this (e.g. "subscriber_compare")
    - exclude_kind: delete all reports EXCEPT that kind
    """
    data = request.get_json(force=True) or {}
    if data.get("all"):
        kind = data.get("kind")
        exclude_kind = data.get("exclude_kind")
        if kind or exclude_kind:
            metas = list_reports_meta()
            if kind:
                names = [m["name"] for m in metas if m.get("kind") == kind]
            else:
                names = [m["name"] for m in metas if m.get("kind") != exclude_kind]
        else:
            names = [p.name for p in REPORTS_DIR.glob("*.html")]
    else:
        names = data.get("names") or []
    deleted = sum(1 for n in names if _delete_report(n))
    return jsonify({"deleted": deleted})

# ─── HTML UI ──────────────────────────────────────────────────────────────────

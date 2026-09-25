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
    db_capture_state, db_compare_state,
    subscriber_capture_state, subscriber_compare_state,
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
        "db_capture":     bool(db_capture_state["running"]),
        "db_compare":     bool(db_compare_state["running"]),
        "subscriber_capture": bool(subscriber_capture_state["running"]),
        "subscriber_compare": bool(subscriber_compare_state["running"]),
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
    cfg["ssh_pass"]    = RUNTIME_SECRETS.get("ssh_pass", "")
    cfg["ssh_pass_b"]  = RUNTIME_SECRETS.get("ssh_pass_b", "")
    cfg["sudo_pass"]   = RUNTIME_SECRETS.get("sudo_pass", "")
    cfg["sudo_pass_b"] = RUNTIME_SECRETS.get("sudo_pass_b", "")
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
    """Store secrets in memory. Optionally persist (encrypted) to config.json.
    ssh_key is not a secret — it's a plain field saved via the main /api/config.
    Which fields matter depends on access_mode: cloud uses db_pass/db_pass_b;
    onprem uses ssh_pass/ssh_pass_b (+ optional sudo_pass/sudo_pass_b) — separate
    baseline/target values since local vs. prod machines commonly use different
    passwords."""
    data = request.json
    db_pass     = data.get("db_pass", "")
    db_pass_b   = data.get("db_pass_b", "")
    ssh_pass    = data.get("ssh_pass", "")
    ssh_pass_b  = data.get("ssh_pass_b", "")
    sudo_pass   = data.get("sudo_pass", "")
    sudo_pass_b = data.get("sudo_pass_b", "")
    if db_pass:
        RUNTIME_SECRETS["db_pass"] = db_pass
    if db_pass_b:
        RUNTIME_SECRETS["db_pass_b"] = db_pass_b
    if ssh_pass:
        RUNTIME_SECRETS["ssh_pass"] = ssh_pass
    if ssh_pass_b:
        RUNTIME_SECRETS["ssh_pass_b"] = ssh_pass_b
    if sudo_pass:
        RUNTIME_SECRETS["sudo_pass"] = sudo_pass
    if sudo_pass_b:
        RUNTIME_SECRETS["sudo_pass_b"] = sudo_pass_b
    if data.get("save_to_disk"):
        save_secrets_to_disk(
            db_pass=RUNTIME_SECRETS.get("db_pass", ""),
            db_pass_b=RUNTIME_SECRETS.get("db_pass_b", ""),
            ssh_pass=RUNTIME_SECRETS.get("ssh_pass", ""),
            ssh_pass_b=RUNTIME_SECRETS.get("ssh_pass_b", ""),
            sudo_pass=RUNTIME_SECRETS.get("sudo_pass", ""),
            sudo_pass_b=RUNTIME_SECRETS.get("sudo_pass_b", ""),
        )
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
    handle = None
    try:
        cfg = get_cfg()
        handle = open_connection(cfg, target=target)
        run_query(handle, "SELECT 1")
        which = "target" if target else "baseline"
        mode_label = "on-prem/sudo psql" if handle["mode"] == "onprem" else "PostgreSQL"
        return jsonify({"ok": True, "msg": f"✅ {mode_label} connection successful ({which})"})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)})
    finally:
        if handle:
            close_connection(handle)

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

def _db_capture_thread(cfg, patterns, since, ext_id, state):
    log = state["log_queue"]
    total = len(patterns)
    handle = None
    try:
        handle = open_connection(cfg)
        saved, errors, total_fetched = {}, [], 0
        for i, pattern in enumerate(patterns, 1):
            label = label_for_pattern(cfg, pattern)
            sub_ids = resolve_subscriber_ids(handle, [pattern])
            if not sub_ids:
                errors.append(f"No subscriber found for pattern '{pattern}'")
                log.put({"type": "progress", "pattern": pattern, "current": i, "total": total,
                         "msg": f"[{i}/{total}] {pattern}: no subscriber found — skipped"})
                continue
            # Show every subscriber_id this pattern fans out to up front — a
            # pattern with several fan-out rows is otherwise a black box
            # here: there's no way to tell from the log whether all of them
            # actually got queried, or just one.
            ids_str = ", ".join(str(s) for s in sub_ids)
            log.put({"type": "progress", "pattern": pattern, "current": i, "total": total,
                     "msg": f"[{i}/{total}] Fetching: {pattern} — subscriber_id(s): {ids_str}…"})
            # No since/ext_id given → the "leave blank for last 100" default.
            fetch_limit = 100 if (not since and not ext_id) else 300
            # prefer_recent=True — a Capture with a wide/old `since` (e.g.
            # "since Jan 1") can easily have far more than `limit` matching
            # rows; without this it fetched the OLDEST `limit` rows in that
            # window and never got anywhere near today's data, so a flow's
            # current notification shapes could never be captured no matter
            # how the since date was set. Capture wants "what does this flow
            # look like right now", not strict chronological coverage.
            rows = fetch_notifications(handle, sub_ids, since=since, ext_id=ext_id, limit=fetch_limit, prefer_recent=True)
            total_fetched += len(rows)
            if len(sub_ids) > 1:
                # Per-subscriber row counts confirm every fan-out subscriber
                # actually contributed rows (0 is a real, visible answer too
                # — a quiet sibling isn't silently indistinguishable from one
                # that was never queried at all).
                per_sub = {sid: 0 for sid in sub_ids}
                for row in rows:
                    sid = row.get("subscriber_id")
                    if sid in per_sub:
                        per_sub[sid] += 1
                breakdown = ", ".join(f"{sid}: {n} row(s)" for sid, n in per_sub.items())
                log.put({"type": "progress", "pattern": pattern, "current": i, "total": total,
                         "msg": f"[{i}/{total}] {pattern} — {breakdown}"})
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
        log.put({"type": "done", "saved": list(saved.keys()), "total_fetched": total_fetched, "errors": errors})
    except Exception as e:
        log.put({"type": "error", "msg": str(e)})
    finally:
        if handle: close_connection(handle)
        state["running"] = False

@bp.route("/api/capture/start", methods=["POST"])
def api_capture_start():
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    t_old = db_capture_state.get("thread")
    if db_capture_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"ok": False, "error": "Capture already running"}), 400
    data = request.json
    patterns = data.get("patterns")
    if patterns is None:  # back-compat with the old single-pattern payload
        patterns = [data.get("pattern")] if data.get("pattern") else []
    patterns = [p.strip() for p in patterns if p and p.strip()]
    if not patterns:
        return jsonify({"ok": False, "error": "Enter at least one pattern"}), 400
    since  = data.get("since")  or None
    ext_id = data.get("ext_id") or None
    cfg = get_cfg()
    db_capture_state["running"]   = True
    db_capture_state["log_queue"] = Broadcaster()
    t = threading.Thread(target=_db_capture_thread,
                         args=(cfg, patterns, since, ext_id, db_capture_state), daemon=True)
    db_capture_state["thread"] = t
    t.start()
    return jsonify({"ok": True, "total": len(patterns)})

@bp.route("/api/capture/stream")
def api_capture_stream():
    def generate():
        idx = 0
        bus = db_capture_state["log_queue"]
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

def _db_compare_thread(cfg, patterns, mode, since, ext_id, gsource, state):
    log = state["log_queue"]
    total = len(patterns)
    handle = None
    try:
        handle = open_connection(cfg, target=True)
        all_results, missing = [], []
        for i, pattern in enumerate(patterns, 1):
            log.put({"type": "progress", "pattern": pattern, "current": i, "total": total,
                     "msg": f"[{i}/{total}] Comparing: {pattern}…"})
            sub_ids = resolve_subscriber_ids(handle, [pattern])
            if not sub_ids:
                missing.append(pattern)
                continue
            label = label_for_pattern(cfg, pattern)
            rows = fetch_notifications(handle, sub_ids, since=since, ext_id=ext_id)
            results = process_rows(rows, mode=mode, source=gsource, label=label)
            for r in results:
                r["flow"] = label
            all_results.extend(results)
        if not all_results and len(missing) == len(patterns):
            log.put({"type": "error", "msg": f"No subscriber found for pattern(s): {', '.join(missing)}"})
            return
        all_results.sort(key=lambda r: r.get("db_id") or 0)
        all_results, skipped_repeats = dedupe_by_key(all_results)

        per_flow = {}
        for r in all_results:
            f = r.get("flow", "OTHER")
            s = per_flow.setdefault(f, {"total": 0, "pass": 0, "fail": 0})
            s["total"] += 1
            if r["status"] == "PASS":
                s["pass"] += 1
            elif r["status"] == "FAIL":
                s["fail"] += 1
        meta = {
            "Project": current_project() or "(none)",
            "Golden source": gsource,
            "Mode": mode,
            "Run at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Flows": ", ".join(f"{k}({v['pass']}/{v['total']})" for k, v in per_flow.items()) or "none",
        }
        report_name = save_report(build_html_report(all_results, "Notification Comparison Report", meta),
                                  prefix="compare")
        save_report_meta(report_name, all_results, project=current_project(), mode=mode,
                         per_flow=per_flow, kind="compare")

        log.put({"type": "done", "results": all_results, "total": len(all_results),
                 "skipped_repeats": skipped_repeats, "missing_patterns": missing, "report": report_name})
    except Exception as e:
        log.put({"type": "error", "msg": str(e)})
    finally:
        if handle: close_connection(handle)
        state["running"] = False

@bp.route("/api/compare/start", methods=["POST"])
def api_compare_start():
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    t_old = db_compare_state.get("thread")
    if db_compare_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"ok": False, "error": "Compare already running"}), 400
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
    cfg = get_cfg()
    db_compare_state["running"]   = True
    db_compare_state["log_queue"] = Broadcaster()
    t = threading.Thread(target=_db_compare_thread,
                         args=(cfg, patterns, mode, since, ext_id, gsource, db_compare_state), daemon=True)
    db_compare_state["thread"] = t
    t.start()
    return jsonify({"ok": True, "total": len(patterns)})

@bp.route("/api/compare/stream")
def api_compare_stream():
    def generate():
        idx = 0
        bus = db_compare_state["log_queue"]
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


# ─── SUBSCRIBER SNAPSHOT ROUTES (baseline subscriber row per pattern, vs target) ──

@bp.route("/api/subscriber/goldens")
def api_subscriber_goldens():
    return jsonify(list_subscriber_goldens())

def _subscriber_capture_thread(cfg, state):
    log = state["log_queue"]
    handle = None
    try:
        handle = open_connection(cfg, target=False)
        rows = fetch_all_subscribers(handle)
        if not rows:
            log.put({"type": "error", "msg": "No subscriber rows found in the baseline environment."})
            return
        rows_by_pattern = {}
        for row in rows:
            pattern = (row.get("pattern") or "").strip()
            if not pattern:
                continue
            rows_by_pattern.setdefault(pattern, []).append(row)
        saved, errors = [], []
        total = len(rows_by_pattern)
        # One golden file per raw pattern name — no Config-tab label
        # substitution. That mapping used to file e.g. `service-request-update`
        # away under a friendlier name like `Put_Success`, which silently
        # hid it from the flat capture list and made it look like the
        # pattern was never captured. Sorted by pattern (not DB-scan order)
        # purely so filenames come out in a stable order run to run.
        for i, pattern in enumerate(sorted(rows_by_pattern), 1):
            # A pattern can fan out to several subscriber rows (different
            # url/advance_filter routing rules) — store each row as its own
            # golden file instead of bundling them into one list, so every
            # row is independently visible/comparable. First row (by
            # subscriber_sort_key) keeps the plain pattern name; further
            # rows for the same pattern get a _1, _2, ... postfix.
            prows = sorted(rows_by_pattern[pattern], key=subscriber_sort_key)
            log.put({"type": "progress", "pattern": pattern, "current": i, "total": total,
                     "msg": f"[{i}/{total}] Capturing: {pattern}…"})
            for j, row in enumerate(prows):
                label = pattern if j == 0 else f"{pattern}_{j}"
                save_subscriber_golden(label, row)
                saved.append(label)
        log.put({"type": "done", "saved": saved, "errors": errors})
    except Exception as e:
        log.put({"type": "error", "msg": str(e)})
    finally:
        if handle: close_connection(handle)
        state["running"] = False

@bp.route("/api/subscriber/capture/start", methods=["POST"])
def api_subscriber_capture_start():
    """Snapshot every subscriber row that actually exists in the baseline env
    (the whole `subscriber` table, not just patterns typed into the Config
    tab) and store one golden per raw pattern name under
    golden/{project}/subscriber/{pattern}.json."""
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    t_old = subscriber_capture_state.get("thread")
    if subscriber_capture_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"ok": False, "error": "Capture already running"}), 400
    cfg = get_cfg()
    subscriber_capture_state["running"]   = True
    subscriber_capture_state["log_queue"] = Broadcaster()
    t = threading.Thread(target=_subscriber_capture_thread,
                         args=(cfg, subscriber_capture_state), daemon=True)
    subscriber_capture_state["thread"] = t
    t.start()
    return jsonify({"ok": True})

@bp.route("/api/subscriber/capture/stream")
def api_subscriber_capture_stream():
    def generate():
        idx = 0
        bus = subscriber_capture_state["log_queue"]
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

def _dup_index(label, pattern):
    """A golden labelled exactly `pattern` is row 0 of that pattern's fan-out
    group; `{pattern}_N` is row N — see _subscriber_capture_thread."""
    if label == pattern:
        return 0
    prefix = f"{pattern}_"
    if label.startswith(prefix):
        try:
            return int(label[len(prefix):])
        except ValueError:
            pass
    return 0

def _subscriber_compare_thread(cfg, labels, state):
    log = state["log_queue"]
    handle = None
    try:
        handle = open_connection(cfg, target=True)
        results = []
        total = len(labels)

        # Fetch every target-env subscriber row once up front (rather than
        # one query per pattern) — needed both to match each baseline row by
        # position and to spot target rows with no baseline golden at all.
        target_rows = json.loads(json.dumps(fetch_all_subscribers(handle), default=str))
        rows_by_target_pattern = {}
        for row in target_rows:
            tpattern = (row.get("pattern") or "").strip()
            if tpattern:
                rows_by_target_pattern.setdefault(tpattern, []).append(row)
        for tpattern in rows_by_target_pattern:
            rows_by_target_pattern[tpattern] = sorted(rows_by_target_pattern[tpattern], key=subscriber_sort_key)
        consumed = set()  # (pattern, index into rows_by_target_pattern[pattern]) already matched

        for i, label in enumerate(labels, 1):
            log.put({"type": "progress", "label": label, "current": i, "total": total,
                     "msg": f"[{i}/{total}] Comparing: {label}…"})
            golden = load_subscriber_golden(label)
            if isinstance(golden, list):
                # Legacy golden from before per-row files — a whole fan-out
                # group saved as one list. Diff it against the whole target
                # group at once (old behavior) rather than trying to split
                # it into per-row files after the fact.
                golden_rows = sorted(golden, key=subscriber_sort_key)
                pattern = (golden_rows[0].get("pattern") or "").strip() if golden_rows else ""
                if not pattern:
                    results.append({"label": label, "pattern": "", "status": "NO GOLDEN", "findings": [], "fields": []})
                    continue
                actual_rows = rows_by_target_pattern.get(pattern, [])
                if not actual_rows:
                    missing_fields = [
                        {"path": p, "baseline": v, "target": "NOT FOUND", "status": "fail"}
                        for p, v in sorted(flatten_dict(normalize(golden_rows)).items())
                    ]
                    results.append({"label": label, "pattern": pattern, "status": "MISSING IN TARGET", "findings": [
                        {"type": "Missing Field", "path": "subscriber",
                         "detail": "No subscriber found in the target environment for this pattern."}
                    ], "fields": missing_fields})
                    continue
                consumed.update((pattern, idx) for idx in range(len(actual_rows)))
                fields = side_by_side_fields(golden_rows, actual_rows)
            else:
                golden_row = golden or {}
                pattern = (golden_row.get("pattern") or "").strip()
                if not pattern:
                    results.append({"label": label, "pattern": "", "status": "NO GOLDEN", "findings": [], "fields": []})
                    continue
                idx = _dup_index(label, pattern)
                target_list = rows_by_target_pattern.get(pattern, [])
                if idx >= len(target_list):
                    # This exact row (by position within the pattern's
                    # fan-out group) has no counterpart in the target env —
                    # either the whole pattern is gone, or the target simply
                    # has fewer rows for it than the baseline did.
                    missing_fields = [
                        {"path": p, "baseline": v, "target": "NOT FOUND", "status": "fail"}
                        for p, v in sorted(flatten_dict(normalize(golden_row)).items())
                    ]
                    results.append({"label": label, "pattern": pattern, "status": "MISSING IN TARGET", "findings": [
                        {"type": "Missing Field", "path": "subscriber",
                         "detail": "No matching subscriber row found in the target environment."}
                    ], "fields": missing_fields})
                    continue
                consumed.add((pattern, idx))
                fields = side_by_side_fields(golden_row, target_list[idx])

            # Derive the pass/fail verdict from the exact same field-by-field
            # comparison rendered in the table below, instead of running a
            # second, independent DeepDiff pass over strip_dynamic-ed data.
            # Two separate diff engines could (and did) disagree on edge
            # cases — e.g. a field that's null on one side and a populated
            # object on the other — showing red "fail" cells in the table
            # while the overall verdict still said PASS.
            findings = [
                {"type": "type changes" if f["status"] == "fail" else "values changed",
                 "path": f["path"], "detail": f"{f['baseline']!r} → {f['target']!r}"}
                for f in fields if f["status"] != "same"
            ]
            status = "FAIL" if any(f["status"] == "fail" for f in fields) else "PASS"
            results.append({"label": label, "pattern": pattern,
                             "status": status, "findings": findings, "fields": fields})

        # Reverse check: a subscriber row can exist in the target env with no
        # baseline counterpart at all — a whole new pattern, or an extra
        # fan-out row added to an already-known pattern after the last
        # capture. The loop above only ever walks baseline goldens, so
        # without this it goes completely unreported.
        log.put({"type": "progress", "label": "target-only check", "current": total, "total": total,
                 "msg": "Checking for rows present only in the target…"})
        for pattern in sorted(rows_by_target_pattern):
            for idx, trow in enumerate(rows_by_target_pattern[pattern]):
                if (pattern, idx) in consumed:
                    continue
                label = pattern if idx == 0 else f"{pattern}_{idx}"
                extra_fields = [
                    {"path": p, "baseline": "NOT FOUND", "target": v, "status": "fail"}
                    for p, v in sorted(flatten_dict(normalize(trow)).items())
                ]
                results.append({"label": label, "pattern": pattern, "status": "MISSING IN BASELINE", "findings": [
                    {"type": "Extra Field", "path": "subscriber",
                     "detail": "Subscriber row exists in the target environment but no baseline snapshot was captured for it."}
                ], "fields": extra_fields})

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

        log.put({"type": "done", "results": results, "report": report_name})
    except Exception as e:
        log.put({"type": "error", "msg": str(e)})
    finally:
        if handle: close_connection(handle)
        state["running"] = False

@bp.route("/api/subscriber/compare/start", methods=["POST"])
def api_subscriber_compare_start():
    """Diff every pattern captured by /api/subscriber/capture (i.e. every
    pattern that existed in the baseline env at capture time — not just the
    ones typed into the Config tab) against its target-env subscriber row."""
    if not secrets_ready():
        return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
    t_old = subscriber_compare_state.get("thread")
    if subscriber_compare_state["running"] and t_old is not None and t_old.is_alive():
        return jsonify({"ok": False, "error": "Compare already running"}), 400
    cfg = get_cfg()
    labels = list_subscriber_goldens()
    if not labels:
        return jsonify({"ok": False, "error": "No subscriber snapshots captured yet. Run Capture Golden → 👤 Subscriber first."}), 400
    subscriber_compare_state["running"]   = True
    subscriber_compare_state["log_queue"] = Broadcaster()
    t = threading.Thread(target=_subscriber_compare_thread,
                         args=(cfg, labels, subscriber_compare_state), daemon=True)
    subscriber_compare_state["thread"] = t
    t.start()
    return jsonify({"ok": True, "total": len(labels)})

@bp.route("/api/subscriber/compare/stream")
def api_subscriber_compare_stream():
    def generate():
        idx = 0
        bus = subscriber_compare_state["log_queue"]
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
    all_patterns = bool(data.get("all_patterns"))
    pattern = ""
    if origin == "db":
        if not secrets_ready():
            return jsonify({"ok": False, "error": "⚠️ Enter DB password and SSH key path in Config first"}), 400
        if all_patterns:
            if not list_subscriber_goldens():
                return jsonify({"ok": False, "error": "No subscriber snapshots captured yet. Run Capture Golden → 👤 Subscriber, then Compare Subscribers, first."}), 400
        else:
            pattern = (data.get("pattern") or "").strip()
            if not pattern:
                return jsonify({"ok": False, "error": "Enter a pattern first"}), 400
    watch_state["mode"]   = data.get("mode", "full")
    watch_state["ext_id"] = data.get("ext_id") or None
    watch_state["source"] = golden
    watch_state["data_source"] = origin
    # Reset the event log — unlike every other live/capture flow, this one
    # was never re-created on start, so a fresh /api/watch/stream connection
    # replayed the *entire* history of every past watch run from idx 0,
    # including old "done" events. The new run's SSE stream would hit one
    # of those stale "done" markers almost immediately and close itself,
    # making the UI think the brand-new run had already finished while the
    # backend thread was still very much alive — which is also why the next
    # Start attempt then failed with "Already running".
    watch_state["log_queue"] = Broadcaster()
    watch_state["running"] = True  # set before start() so a fast stop() wins the race
    t = threading.Thread(target=watch_thread_fn, args=(pattern, interval, all_patterns), daemon=True)
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
            # "error" ends the run just as much as "done" does — the watch
            # thread's except block logs an error and dies (running=False)
            # without ever putting a "done" event, so breaking only on
            # "done" left this generator (and the browser tab) waiting
            # forever on a thread that had already exited: the UI kept
            # showing "Watching..." with no further notifications ever
            # arriving, since nothing was left running to fetch them.
            if item.get("type") in ("done", "error"):
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
            # See the identical fix on /api/watch/stream — an "error" ends
            # the run just as much as "done" does; breaking only on "done"
            # left the stream (and the UI) waiting forever on a thread that
            # had already died.
            if item.get("type") in ("done", "error"):
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

    diff = DeepDiff(a, b, verbose_level=2, threshold_to_diff_deeper=0)
    findings = diff_to_list(diff, mode=mode)
    return jsonify({
        "status": status_from_findings(findings),
        "findings": findings,
        "count": len(findings),
        "fields": side_by_side_fields(a, b),
        "golden": a,
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

    diff = DeepDiff(a, b, verbose_level=2, threshold_to_diff_deeper=0)
    findings = diff_to_list(diff, mode=mode)
    return jsonify({
        "status": status_from_findings(findings),
        "findings": findings,
        "count": len(findings),
        "fields": side_by_side_fields(a, b),
        "golden": a,
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
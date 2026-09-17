"""Background worker threads: live capture, watch, and full-run compare."""
import time
from datetime import datetime

from core.config import *
from core.diffing import *
from core.db import (
    open_connection, close_connection, fetch_notifications, resolve_subscriber_ids, db_now,
    fetch_all_subscribers,
)
from core.golden import (
    process_rows, run_all_db_flows, current_project, save_golden, label_for_pattern,
    list_subscriber_goldens, load_subscriber_golden,
)
from core.kowl import kowl_watch_loop
from core.reports import build_html_report, save_report, save_report_meta
from core.allure import generate_allure, build_allure_results
from core.state import watch_state, full_watch_state, capture_state

def _connect_msg(cfg, target=False):
    """Log-friendly description of how we're connecting — differs by access_mode
    (SSH tunnel + key vs. plain SSH login + sudo psql), so the live log reads
    correctly in either mode instead of always saying "Opening SSH tunnel"."""
    host = (cfg.get("ssh_host_b") or cfg["ssh_host"]) if target else cfg["ssh_host"]
    if cfg.get("access_mode") == "onprem":
        return f"Connecting to {host} via SSH (password)..."
    return f"Opening SSH tunnel to {host}..."

def capture_live_thread(patterns, interval, ext_id=None):
    if isinstance(patterns, str):
        patterns = [patterns]
    cfg = get_cfg()
    log = capture_state["log_queue"]
    handle = None

    try:
        log.put({"type": "info", "msg": _connect_msg(cfg)})
        handle = open_connection(cfg)
        since  = db_now(handle)  # DB's own clock — see db_now() for why this matters
        # Map subscriber_id -> configured pattern label, so a golden is filed
        # under the pattern's own label folder rather than whatever internal
        # "type" field happens to be in the payload (e.g. a
        # service-request-cancel-success pattern that carries type=PUT must
        # not land in the PUT folder).
        sub_to_label = {}
        for pattern in patterns:
            label = label_for_pattern(cfg, pattern)
            for sid in resolve_subscriber_ids(handle, [pattern]):
                sub_to_label[sid] = label
        sub_ids = list(sub_to_label.keys())
        if not sub_ids:
            log.put({"type": "error", "msg": f"No subscriber found for pattern(s): {', '.join(patterns)}."})
            close_connection(handle)
            handle = None
            return
        mode_msg = f"ext_id={ext_id}" if ext_id else "polling by time"
        log.put({"type": "info", "msg": f"Connected. Watching {len(patterns)} pattern(s) ({mode_msg}) — trigger your flow now..."})

        while capture_state["running"]:
            rows = fetch_notifications(handle, sub_ids, since=since, ext_id=ext_id)
            new  = [r for r in rows if r["id"] not in capture_state["seen"]]
            for row in new:
                capture_state["seen"].add(row["id"])
                label = sub_to_label.get(row.get("subscriber_id"), "OTHER")
                try:
                    payload = clean_payload(row["payload"])
                    key     = notif_key(payload)
                    dedup_key = f"{label}/{key}"
                    if dedup_key not in capture_state["saved"]:
                        save_golden(key, payload, label=label)
                        capture_state["saved"][dedup_key] = True
                        log.put({"type": "pass", "msg": f"📸 [{row['id']}] Saved golden: {label}/{key}"})
                    else:
                        log.put({"type": "info", "msg": f"⏭  [{row['id']}] Already captured: {label}/{key} (keeping first)"})
                except Exception as e:
                    log.put({"type": "error", "msg": f"⚠️  [{row['id']}] Error: {e}"})
            time.sleep(interval)

        close_connection(handle)
        handle = None
        saved = list(capture_state["saved"].keys())
        log.put({"type": "done", "msg": f"Stopped. {len(saved)} golden snapshot(s) saved.", "saved": saved})
    except Exception as e:
        log.put({"type": "error", "msg": f"Error: {e}"})
    finally:
        capture_state["running"] = False
        if handle:
            try:
                close_connection(handle)
            except Exception:
                pass

# ─── WATCH THREAD ─────────────────────────────────────────────────────────────

def _all_subscriber_patterns():
    """Every distinct `pattern` covered by a captured subscriber golden — not
    just the ones typed into the Config tab. A pattern with fan-out rows is
    saved as several goldens (pattern, pattern_1, pattern_2, ...) that all
    share the same `pattern` field, so goldens are deduped down to patterns
    here rather than watched once per golden file."""
    patterns = set()
    for label in list_subscriber_goldens():
        golden = load_subscriber_golden(label)
        row = golden[0] if isinstance(golden, list) else golden
        p = ((row or {}).get("pattern") or "").strip()
        if p:
            patterns.add(p)
    return sorted(patterns)

def watch_thread_fn(pattern, interval, all_patterns=False):
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
    # Once a notification key (type/state/status) has been compared once in this
    # run, every later message with the identical key gets the same schema
    # verdict — so skip re-comparing/re-logging it and just tally how many were
    # skipped, instead of flooding the log with the same diff over and over.
    # Keyed by (flow, key) rather than bare key in all_patterns mode, so two
    # different patterns that happen to produce the same notification key
    # (e.g. both a generic "unknown__UNKNOWN") don't shadow each other.
    seen_keys = {}
    log = watch_state["log_queue"]
    handle = None

    try:
        cfg = get_cfg()
        log.put({"type": "info", "msg": _connect_msg(cfg, target=True)})
        handle = open_connection(cfg, target=True)
        since = db_now(handle)  # DB's own clock — see db_now() for why this matters

        if all_patterns:
            patterns = _all_subscriber_patterns()
            if not patterns:
                log.put({"type": "error", "msg": "No subscriber snapshots captured yet. Run Capture Golden → 👤 Subscriber first."})
                close_connection(handle)
                handle = None
                return
            # One query for every subscriber row, not one round-trip per
            # pattern — with dozens of captured patterns over onprem SSH
            # (each resolve is its own remote `sudo -u postgres psql` call),
            # doing this per-pattern left the watch looking stuck at
            # "Connecting..." for a long time with zero visible progress.
            wanted = set(patterns)
            rows_by_pattern = {}
            for row in fetch_all_subscribers(handle):
                p = (row.get("pattern") or "").strip()
                if p in wanted:
                    rows_by_pattern.setdefault(p, []).append(row)
            # A pattern with fan-out rows (several subscribers listening for
            # the same pattern) is tagged one *display* flow per row —
            # pattern, then pattern_1, pattern_2, ... matching the subscriber
            # golden filenames — so it's visible which specific subscriber a
            # live notification matched. The *golden lookup* label stays the
            # base pattern's configured label for every row of that pattern,
            # not the per-row suffix: notification-payload goldens (Capture
            # Golden -> DB) are captured once per pattern, since every
            # subscriber for the same pattern forwards the same kind of
            # event — looking them up by the dup-suffixed label would never
            # find the golden captured under the plain pattern name.
            sub_to_flow, sub_to_golden_label, sub_ids = {}, {}, []
            for p, prows in rows_by_pattern.items():
                golden_label = label_for_pattern(cfg, p)
                for j, row in enumerate(sorted(prows, key=subscriber_sort_key)):
                    sub_to_flow[row["id"]] = p if j == 0 else f"{p}_{j}"
                    sub_to_golden_label[row["id"]] = golden_label
                    sub_ids.append(row["id"])
            if not sub_ids:
                log.put({"type": "error", "msg": "None of the captured subscriber patterns matched a subscriber row in the target env."})
                close_connection(handle)
                handle = None
                return
            log.put({"type": "info", "msg": f"Connected. Watching {len(sub_ids)} subscriber(s)..."})
        else:
            sub_ids = resolve_subscriber_ids(handle, [pattern])
            if not sub_ids:
                log.put({"type": "error", "msg": f"No subscriber found for pattern '{pattern}'."})
                close_connection(handle)
                handle = None
                return
            label = label_for_pattern(cfg, pattern)
            sub_to_flow = {sid: label for sid in sub_ids}
            sub_to_golden_label = {sid: label for sid in sub_ids}
            ext_id_msg = watch_state.get("ext_id")
            mode_msg = f"ext_id={ext_id_msg}" if ext_id_msg else "polling by time"
            log.put({"type": "info", "msg": f"Connected. Watching pattern '{pattern}' ({mode_msg})..."})

        ext_id = watch_state.get("ext_id")

        while watch_state["running"]:
            rows = fetch_notifications(handle, sub_ids, since=since, ext_id=ext_id)
            new = [r for r in rows if r["id"] not in seen]
            for row in new:
                seen.add(row["id"])
                row_label = sub_to_flow.get(row.get("subscriber_id"), "OTHER")
                golden_label = sub_to_golden_label.get(row.get("subscriber_id"), row_label)
                results = process_rows([row], mode=watch_state.get("mode", "full"),
                                       source=watch_state.get("source", "db"), label=golden_label)
                r = results[0]
                if all_patterns:
                    r["flow"] = row_label
                dedup_key = (row_label, r["key"]) if all_patterns else r["key"]
                if dedup_key in seen_keys:
                    seen_keys[dedup_key] += 1
                    continue
                seen_keys[dedup_key] = 1
                watch_state["results"].append(r)
                icon   = {"PASS": "✅", "FAIL": "❌", "NO GOLDEN": "⚠️", "ERROR": "🔥"}.get(r["status"], "?")
                # NOTE: use a distinct name — do NOT reassign `ext_id`, which is the
                # query filter for the next poll; clobbering it pins the watch to one flow.
                row_ext_id = r.get("ext_id", "")
                ext_str = f" [{row_ext_id}]" if row_ext_id else ""
                nfail = fail_count(r["findings"])
                nwarn = len(r["findings"]) - nfail
                counts = f"{nfail} diff(s)" + (f", {nwarn} warning(s)" if nwarn else "")
                flow_str = f"{row_label} " if all_patterns else ""
                log.put({"type": r["status"].lower().replace(" ", "_"), "msg": f"{icon} {flow_str}[{r['db_id']}]{ext_str} {r['key']} — {counts}", "result": r})
            time.sleep(interval)

        close_connection(handle)
        handle = None
        repeats = sum(c - 1 for c in seen_keys.values() if c > 1)
        if repeats:
            log.put({"type": "info", "msg": f"({repeats} additional notification(s) with an already-seen key were skipped)"})
        log.put({"type": "done", "msg": "Watch stopped."})
    except Exception as e:
        log.put({"type": "error", "msg": f"Error: {e}"})
    finally:
        watch_state["running"] = False
        if handle:
            try:
                close_connection(handle)
            except Exception:
                pass

# ─── ALLURE REPORT ────────────────────────────────────────────────────────────
# We can't render Allure HTML without the `allure` CLI + Java, so we always emit
# allure-results (the JSON the Allure CLI consumes) and zip it for download.
# If the CLI happens to be installed at runtime, we also generate the HTML report.


def full_watch_thread_fn(interval):
    """Live-watch every configured subscriber at once, time-bounded.
    On stop, build + save a collective report (+ metadata sidecar)."""
    full_watch_state["results"] = []
    seen = set()
    # Dedup by (flow, key): once a given flow's notification key has been
    # compared once, later messages with the same key get the same schema
    # verdict — skip re-comparing/re-logging them instead of flooding the log.
    seen_keys = {}
    started = datetime.now()
    full_watch_state["started_at"] = started.strftime("%Y-%m-%d %H:%M:%S")
    log = full_watch_state["log_queue"]
    mode = full_watch_state.get("mode", "full")
    source = full_watch_state.get("source", "db")
    handle = None

    try:
        if full_watch_state.get("data_source") == "kowl":
            # Live data origin: Kowl topics, compared vs kowl/isd golden.
            kowl_watch_loop(full_watch_state, interval)
        else:
            cfg = get_cfg()
            log.put({"type": "info", "msg": _connect_msg(cfg, target=True)})
            handle = open_connection(cfg, target=True)
            since = db_now(handle)  # DB's own clock — see db_now() for why this matters
            # Build subscriber_id -> flow map by resolving each configured pattern.
            sub_to_flow, sub_ids = {}, []
            for entry in cfg.get("patterns", []):
                pattern = (entry.get("pattern") or "").strip()
                if not pattern:
                    continue
                flow = (entry.get("label") or pattern).strip()
                for sid in resolve_subscriber_ids(handle, [pattern]):
                    sub_to_flow[sid] = flow
                    sub_ids.append(sid)
            if not sub_ids:
                log.put({"type": "error", "msg": "No patterns configured (or none matched a subscriber). Set them on the Config tab."})
                close_connection(handle)
                handle = None
                return

            flows_str = ", ".join(f"{f}={s}" for s, f in sub_to_flow.items())
            log.put({"type": "info", "msg": f"Connected. Full Run watching all flows ({flows_str}) — trigger your automation now..."})

            while full_watch_state["running"]:
                rows = fetch_notifications(handle, sub_ids, since=since)
                new = [r for r in rows if r["id"] not in seen]
                for row in new:
                    seen.add(row["id"])
                    row_label = sub_to_flow.get(row.get("subscriber_id"), "OTHER")
                    results = process_rows([row], mode=mode, source=source, label=row_label)
                    r = results[0]
                    r["flow"] = row_label
                    dedup_key = (row_label, r["key"])
                    if dedup_key in seen_keys:
                        seen_keys[dedup_key] += 1
                        continue
                    seen_keys[dedup_key] = 1
                    full_watch_state["results"].append(r)
                    icon = {"PASS": "✅", "FAIL": "❌", "NO GOLDEN": "⚠️", "ERROR": "🔥"}.get(r["status"], "?")
                    row_ext_id = r.get("ext_id", "")
                    ext_str = f" [{row_ext_id}]" if row_ext_id else ""
                    nfail = fail_count(r["findings"])
                    nwarn = len(r["findings"]) - nfail
                    counts = f"{nfail} diff(s)" + (f", {nwarn} warning(s)" if nwarn else "")
                    log.put({"type": r["status"].lower().replace(" ", "_"),
                             "msg": f"{icon} {r['flow']} [{r['db_id']}]{ext_str} {r['key']} — {counts}",
                             "result": r})
                time.sleep(interval)

            close_connection(handle)
            handle = None
            repeats = sum(c - 1 for c in seen_keys.values() if c > 1)
            if repeats:
                log.put({"type": "info", "msg": f"({repeats} additional notification(s) with an already-seen key were skipped)"})

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
        if handle:
            try:
                close_connection(handle)
            except Exception:
                pass

# ─── ROUTES ───────────────────────────────────────────────────────────────────
"""Background worker threads: live capture, watch, and full-run compare."""
import time
from datetime import datetime, timezone

from core.config import *
from core.diffing import *
from core.db import open_tunnel, connect_db, fetch_notifications
from core.golden import process_rows, run_all_db_flows, current_project, FLOW_SUBSCRIBER_KEYS, save_golden
from core.kowl import kowl_watch_loop
from core.reports import build_html_report, save_report, save_report_meta
from core.allure import generate_allure, build_allure_results
from core.state import watch_state, full_watch_state, capture_state

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


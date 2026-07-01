"""Golden snapshot storage, project listing, row processing, run-all-flows."""
import json
from pathlib import Path

from deepdiff import DeepDiff  # type: ignore[import]

from core.config import *
from core.diffing import *
from core.db import open_tunnel, connect_db, fetch_notifications

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


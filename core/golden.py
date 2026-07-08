"""Golden snapshot storage, project listing, row processing, run-all-flows."""
import json
from pathlib import Path

from deepdiff import DeepDiff  # type: ignore[import]

from core.config import *
from core.diffing import *
from core.db import open_tunnel, connect_db, fetch_notifications, resolve_subscriber_ids

def current_project():
    return (load_config().get("project") or "").strip()

def golden_root():
    """Root dir for goldens — golden/{project} when a project is set, else golden/."""
    proj = current_project()
    return (GOLDEN_DIR / proj) if proj else GOLDEN_DIR

# Capture sources — golden data is filed under golden/{project}/{source}/{FLOW}/...
GOLDEN_SOURCES = ("db", "isd", "kowl")

def label_for_pattern(cfg, pattern):
    """Configured label for a pattern, e.g. 'service-request-cancel-success' -> 'PUT_Success'.
    Falls back to the pattern text itself if it isn't in the configured list."""
    pattern = (pattern or "").strip()
    for entry in cfg.get("patterns", []):
        if (entry.get("pattern") or "").strip() == pattern:
            return (entry.get("label") or pattern).strip()
    return pattern

def _safe_folder(name):
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in name).strip("_")
    return safe or "OTHER"

def golden_path(key, source="db", label=None):
    """Write path: golden/{project}/{source}/{FOLDER}/{key}.json

    FOLDER is the configured pattern label when given (so e.g. a
    'service-request-cancel-success' pattern always files under its own
    label folder, never under the internal notification 'type' field it
    happens to carry, like PUT) — otherwise falls back to the legacy
    behaviour of deriving the folder from the key's type prefix.
    """
    flow_type = _safe_folder(label) if label else key.split("__")[0].upper()
    return golden_root() / source / flow_type / f"{key}.json"

def save_golden(key, payload, source="db", label=None):
    path = golden_path(key, source, label=label)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str))

def load_golden(key, source=None, label=None):
    """
    Find a golden by key.
    - source given  -> look ONLY in that source (golden/{project}/{source}/...).
    - source None   -> search all sources, then legacy locations.
    - label given   -> look under the label folder first (see golden_path),
                       then fall back to the legacy type-derived folder so
                       goldens captured before pattern-based labeling still
                       resolve.
    """
    flow_type = key.split("__")[0].upper()
    label_folder = _safe_folder(label) if label else None
    if source:
        candidates = []
        if label_folder:
            candidates.append(golden_root() / source / label_folder / f"{key}.json")
        candidates += [
            golden_root() / source / flow_type / f"{key}.json",
            golden_root() / source / f"{key}.json",
        ]
    else:
        candidates = []
        for s in GOLDEN_SOURCES:
            if label_folder:
                candidates.append(golden_root() / s / label_folder / f"{key}.json")
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

def process_rows(rows, mode="full", source=None, label=None):
    results = []
    for row in rows:
        try:
            ext_id  = extract_ext_id(row["payload"])
            payload = clean_payload(row["payload"])
            key     = notif_key(payload)
            golden  = load_golden(key, source=source, label=label)
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

def run_all_db_flows(since=None, limit=200, mode="full", source="db"):
    """Compare recent notifications for every configured pattern against goldens."""
    cfg = get_cfg()
    tunnel = open_tunnel(cfg, target=True)
    try:
        conn = connect_db(tunnel, cfg, target=True)
        cur = conn.cursor()
        all_results, per_flow = [], {}
        for entry in cfg.get("patterns", []):
            pattern = (entry.get("pattern") or "").strip()
            if not pattern:
                continue
            flow = (entry.get("label") or pattern).strip()
            sub_ids = resolve_subscriber_ids(cur, [pattern])
            if not sub_ids:
                continue
            rows = fetch_notifications(cur, sub_ids, since=since, limit=limit)
            res = process_rows(rows, mode=mode, source=source, label=flow)
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


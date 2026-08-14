"""Golden snapshot storage, project listing, row processing, run-all-flows."""
import json
from pathlib import Path

from deepdiff import DeepDiff  # type: ignore[import]

from core.config import *
from core.diffing import *
from core.db import open_connection, close_connection, fetch_notifications, resolve_subscriber_ids

def current_project():
    return (load_config().get("project") or "").strip()

def current_kowl_project():
    """Kowl's own project name — independent of the DB/ISD "project", so Kowl
    baselines can live under a different golden/{kowl_project}/ root."""
    return (load_config().get("kowl_project") or "").strip()

def golden_root(source=None, project_kind=None):
    """Root dir for goldens — golden/{project} when a project is set, else golden/.
    project_kind explicitly picks "db" or "kowl" as the project bucket; if
    omitted, it's inferred from source (source="kowl" -> kowl project, else
    the DB/ISD project). ISD goldens captured for the Kowl world pass
    project_kind="kowl" explicitly since their source is "isd", not "kowl"."""
    kind = project_kind or ("kowl" if source == "kowl" else "db")
    proj = current_kowl_project() if kind == "kowl" else current_project()
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

def _safe_folder_path(label):
    """Sanitize a label that may itself contain '/' into a nested folder path,
    e.g. 'Create/Cancel Request Message/service-request-create' -> each
    segment sanitized individually, '/' preserved as the directory separator."""
    parts = [_safe_folder(p) for p in label.split("/") if p.strip()]
    return "/".join(parts) if parts else "OTHER"

def golden_path(key, source="db", label=None, project_kind=None):
    """Write path: golden/{project}/{source}/{FOLDER}/{key}.json

    FOLDER is the configured pattern label when given (so e.g. a
    'service-request-cancel-success' pattern always files under its own
    label folder, never under the internal notification 'type' field it
    happens to carry, like PUT) — otherwise falls back to the legacy
    behaviour of deriving the folder from the key's type prefix. `label` may
    contain '/' for nested folders (e.g. topic label + message name).
    project_kind: see golden_root() — lets ISD goldens explicitly file under
    the Kowl project instead of the DB/ISD project.
    """
    flow_type = _safe_folder_path(label) if label else key.split("__")[0].upper()
    return golden_root(source, project_kind) / source / flow_type / f"{key}.json"

def save_golden(key, payload, source="db", label=None, project_kind=None):
    path = golden_path(key, source, label=label, project_kind=project_kind)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str))

def load_golden(key, source=None, label=None, project_kind=None):
    """
    Find a golden by key.
    - source given  -> look ONLY in that source (golden/{project}/{source}/...).
    - source None   -> search all sources, then legacy locations.
    - label given   -> look under the label folder first (see golden_path),
                       then fall back to the legacy type-derived folder so
                       goldens captured before pattern-based labeling still
                       resolve.
    - project_kind  -> see golden_root(); forces "db" or "kowl" regardless of source.
    """
    flow_type = key.split("__")[0].upper()
    label_folder = _safe_folder_path(label) if label else None
    if source:
        candidates = []
        if label_folder:
            candidates.append(golden_root(source, project_kind) / source / label_folder / f"{key}.json")
        candidates += [
            golden_root(source, project_kind) / source / flow_type / f"{key}.json",
            golden_root(source, project_kind) / source / f"{key}.json",
        ]
    else:
        candidates = []
        for s in GOLDEN_SOURCES:
            if label_folder:
                candidates.append(golden_root(s) / s / label_folder / f"{key}.json")
            candidates.append(golden_root(s) / s / flow_type / f"{key}.json")
            candidates.append(golden_root(s) / s / f"{key}.json")
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

# ─── SUBSCRIBER SNAPSHOTS (per-pattern subscriber row, baseline vs target) ────
# Stored under golden/{project}/subscriber/{label}.json — a sibling of the
# db/isd/kowl notification sources, but not one of them: it snapshots the
# subscriber table row itself (per configured pattern), not a notification
# payload, so it gets its own top-level folder rather than joining GOLDEN_SOURCES.

def subscriber_golden_path(label):
    return golden_root() / "subscriber" / f"{_safe_folder_path(label)}.json"

def save_subscriber_golden(label, data):
    path = subscriber_golden_path(label)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))

def load_subscriber_golden(label):
    path = subscriber_golden_path(label)
    if path.exists():
        return json.loads(path.read_text())
    return None

def list_subscriber_goldens():
    root = golden_root() / "subscriber"
    if not root.exists():
        return []
    return sorted(str(p.relative_to(root).with_suffix("")) for p in root.rglob("*.json"))

def extract_ext_id(raw_payload):
    """Extract externalServiceRequestId before stripping — used for grouping only."""
    try:
        data = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
        data = normalize(data)
        nd = data.get("notification_data") or data
        return nd.get("externalServiceRequestId", "")
    except Exception:
        return ""

def dedupe_by_key(results):
    """Collapse results down to one per (flow, notification key), keeping the
    first occurrence — mirrors Full Run's live dedup (core/live.py) so Compare
    doesn't flood the table with N identical-looking rows every time the same
    key (e.g. PICK__inventory_awaited__PAUSED) recurs across many requests.
    Returns (deduped_results, skipped_count).
    """
    seen = set()
    deduped = []
    skipped = 0
    for r in results:
        dedup_key = (r.get("flow"), r.get("key"))
        if dedup_key in seen:
            skipped += 1
            continue
        seen.add(dedup_key)
        deduped.append(r)
    return deduped, skipped

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
                                 "status": status_from_findings(findings),
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
    handle = open_connection(cfg, target=True)
    try:
        all_results, per_flow = [], {}
        for entry in cfg.get("patterns", []):
            pattern = (entry.get("pattern") or "").strip()
            if not pattern:
                continue
            flow = (entry.get("label") or pattern).strip()
            sub_ids = resolve_subscriber_ids(handle, [pattern])
            if not sub_ids:
                continue
            rows = fetch_notifications(handle, sub_ids, since=since, limit=limit)
            res = process_rows(rows, mode=mode, source=source, label=flow)
            for r in res:
                r["flow"] = flow
            per_flow[flow] = {
                "total": len(res),
                "pass":  sum(1 for r in res if r["status"] == "PASS"),
                "fail":  sum(1 for r in res if r["status"] == "FAIL"),
            }
            all_results.extend(res)
        return all_results, per_flow
    finally:
        close_connection(handle)

# ─── LIVE CAPTURE THREAD ──────────────────────────────────────────────────────
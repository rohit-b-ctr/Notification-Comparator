#!/usr/bin/env python3
"""
Notification Comparator
=======================
Compares WMS notifications from a Postgres DB against golden snapshots.
Connects via SSH tunnel to the remote DB server.

Usage:
  # Capture current notifications as golden baseline
  python notification_comparator.py capture --since "2026-06-03 10:00:00" --subscriber 158

  # Compare new notifications against golden snapshots
  python notification_comparator.py compare --since "2026-06-03 11:00:00" --subscriber 158

  # Watch mode: poll DB live as you run a flow
  python notification_comparator.py watch --subscriber 158 --poll-interval 3
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import psycopg2
    import sshtunnel
    from deepdiff import DeepDiff
    from jinja2 import Template
except ImportError:
    print("Missing dependencies. Run:")
    print("  pip install psycopg2-binary sshtunnel deepdiff jinja2")
    sys.exit(1)

# ─── CONFIG ───────────────────────────────────────────────────────────────────

SSH_HOST = "172.29.32.137"
SSH_USER = "rohit_b_ctr_greyorange_com"
SSH_KEY  = os.path.expanduser("~/rohit_b_ctr_greyorange_com")  # adjust path if needed

DB_HOST  = "10.57.117.201"
DB_PORT  = 5432
DB_NAME  = "wms_notification"
DB_USER  = "postgres"
DB_PASS  = "b7e440cc68eeedd3"

TABLE    = "subscriber_history"

GOLDEN_DIR  = Path(__file__).parent / "golden"
REPORTS_DIR = Path(__file__).parent / "reports"

# Fields to ignore during comparison (dynamic/run-specific values)
IGNORE_VALUE_FIELDS = {
    "id", "eventdata_id", "notification_id", "execution_id",
    "createdOn", "updatedOn", "receivedOn", "create_time",
    "externalServiceRequestId", "sr_parent", "sr_parentsIds",
    # nested ids inside serviceRequests / containers / products
}

# ─── JAVA ARTIFACT NORMALIZATION ─────────────────────────────────────────────

def normalize(obj):
    """
    Strip Java serialization artifacts:
      {"@type": "java.util.LinkedHashMap", "key": val}  →  {"key": val}
      ["java.util.ArrayList", [...]]                     →  [...]
    """
    if isinstance(obj, dict):
        cleaned = {k: normalize(v) for k, v in obj.items() if k != "@type"}
        return cleaned
    elif isinstance(obj, list):
        if (len(obj) == 2
                and isinstance(obj[0], str)
                and obj[0].startswith("java.")
                and isinstance(obj[1], list)):
            return [normalize(i) for i in obj[1]]
        return [normalize(i) for i in obj]
    return obj


def strip_dynamic_fields(obj, fields=IGNORE_VALUE_FIELDS):
    """Recursively remove keys that have dynamic/run-specific values."""
    if isinstance(obj, dict):
        return {k: strip_dynamic_fields(v, fields)
                for k, v in obj.items()
                if k not in fields}
    elif isinstance(obj, list):
        return [strip_dynamic_fields(i, fields) for i in obj]
    return obj


def clean_payload(raw_payload):
    """Parse, normalize, and strip dynamic fields from a payload."""
    if isinstance(raw_payload, str):
        data = json.loads(raw_payload)
    else:
        data = raw_payload
    data = normalize(data)
    data = strip_dynamic_fields(data)
    return data


def notification_key(payload):
    """
    Derive a stable key: (flow_type, state, notification_type)
    e.g. ('PICK', 'created', 'order_information')
    """
    nd = payload.get("notification_data", {})
    flow_type = nd.get("type", "UNKNOWN")
    state     = nd.get("state", "UNKNOWN")
    notif_type = payload.get("notification_type", "UNKNOWN")
    return f"{flow_type}__{state}__{notif_type}"

# ─── DB ACCESS ────────────────────────────────────────────────────────────────

def get_notifications(cursor, subscriber_id, since: str = None, limit: int = 200):
    """Fetch notifications from subscriber_history."""
    query = f"""
        SELECT id, create_time, status, status_code, subscriber_id, payload
        FROM {TABLE}
        WHERE subscriber_id = %s
          AND payload IS NOT NULL
    """
    params = [subscriber_id]
    if since:
        query += " AND create_time >= %s"
        params.append(since)
    query += " ORDER BY id ASC LIMIT %s"
    params.append(limit)

    cursor.execute(query, params)
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

# ─── GOLDEN SNAPSHOT MANAGEMENT ──────────────────────────────────────────────

def save_golden(key: str, payload: dict):
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    path = GOLDEN_DIR / f"{key}.json"
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"  ✅ Saved golden: {path.name}")


def load_golden(key: str):
    path = GOLDEN_DIR / f"{key}.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)

# ─── COMPARISON ──────────────────────────────────────────────────────────────

def compare_payloads(golden: dict, actual: dict):
    """Return DeepDiff result between golden and actual."""
    diff = DeepDiff(golden, actual, ignore_order=True, verbose_level=2)
    return diff


def diff_to_human(diff: DeepDiff):
    """Convert DeepDiff output to a readable list of findings."""
    findings = []

    for change_type, changes in diff.items():
        if change_type == "dictionary_item_added":
            for path in changes:
                findings.append({"type": "added", "path": str(path), "detail": ""})
        elif change_type == "dictionary_item_removed":
            for path in changes:
                findings.append({"type": "removed", "path": str(path), "detail": ""})
        elif change_type == "values_changed":
            for path, info in changes.items():
                findings.append({
                    "type": "value_changed",
                    "path": str(path),
                    "detail": f"{info['old_value']!r} → {info['new_value']!r}"
                })
        elif change_type == "type_changes":
            for path, info in changes.items():
                findings.append({
                    "type": "type_changed",
                    "path": str(path),
                    "detail": f"{type(info['old_value']).__name__} → {type(info['new_value']).__name__}"
                })
        elif change_type == "iterable_item_added":
            for path in changes:
                findings.append({"type": "list_item_added", "path": str(path), "detail": ""})
        elif change_type == "iterable_item_removed":
            for path in changes:
                findings.append({"type": "list_item_removed", "path": str(path), "detail": ""})

    return findings

# ─── REPORT GENERATION ───────────────────────────────────────────────────────

REPORT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Notification Comparison Report</title>
<style>
  body { font-family: monospace; background: #f5f5f5; padding: 20px; }
  h1 { color: #333; }
  .meta { color: #666; margin-bottom: 20px; font-size: 13px; }
  .card { background: white; border-radius: 6px; padding: 16px; margin-bottom: 16px;
          box-shadow: 0 1px 4px rgba(0,0,0,0.1); }
  .card h2 { margin: 0 0 8px; font-size: 15px; }
  .pass { border-left: 4px solid #4caf50; }
  .fail { border-left: 4px solid #f44336; }
  .no-golden { border-left: 4px solid #ff9800; }
  .badge { display:inline-block; padding: 2px 8px; border-radius: 4px;
           font-size: 12px; font-weight: bold; }
  .badge-pass { background:#e8f5e9; color:#2e7d32; }
  .badge-fail { background:#ffebee; color:#c62828; }
  .badge-warn { background:#fff3e0; color:#e65100; }
  table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }
  th { text-align: left; background: #f0f0f0; padding: 6px 8px; }
  td { padding: 5px 8px; border-bottom: 1px solid #eee; }
  .type-added { color: #2e7d32; }
  .type-removed { color: #c62828; }
  .type-value_changed { color: #1565c0; }
  .type-type_changed { color: #6a1b9a; }
  .type-list_item_added { color: #2e7d32; }
  .type-list_item_removed { color: #c62828; }
  .summary { font-size: 14px; margin-bottom: 24px; }
  .summary span { margin-right: 16px; }
</style>
</head>
<body>
<h1>🔔 Notification Comparison Report</h1>
<div class="meta">
  Generated: {{ generated_at }}<br>
  Subscriber ID: {{ subscriber_id }}<br>
  Since: {{ since }}<br>
  Total notifications fetched: {{ total }}
</div>

<div class="summary">
  <span>✅ Pass: <strong>{{ pass_count }}</strong></span>
  <span>❌ Fail: <strong>{{ fail_count }}</strong></span>
  <span>⚠️ No Golden: <strong>{{ no_golden_count }}</strong></span>
</div>

{% for result in results %}
<div class="card {{ result.css_class }}">
  <h2>
    {{ result.key }}
    &nbsp;
    <span class="badge badge-{{ result.badge_class }}">{{ result.status }}</span>
    &nbsp;
    <small style="color:#999">DB id: {{ result.db_id }} | {{ result.create_time }}</small>
  </h2>

  {% if result.status == "PASS" %}
    <div style="color:#4caf50">No differences found.</div>
  {% elif result.status == "NO GOLDEN" %}
    <div style="color:#ff9800">No golden snapshot found for this key. Run in <code>capture</code> mode first.</div>
  {% else %}
    <table>
      <tr><th>Type</th><th>Path</th><th>Detail</th></tr>
      {% for f in result.findings %}
      <tr>
        <td class="type-{{ f.type }}"><strong>{{ f.type }}</strong></td>
        <td><code>{{ f.path }}</code></td>
        <td>{{ f.detail }}</td>
      </tr>
      {% endfor %}
    </table>
  {% endif %}
</div>
{% endfor %}

</body>
</html>
"""

def generate_report(results, subscriber_id, since, total):
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"report_{ts}.html"

    pass_count     = sum(1 for r in results if r["status"] == "PASS")
    fail_count     = sum(1 for r in results if r["status"] == "FAIL")
    no_golden_count = sum(1 for r in results if r["status"] == "NO GOLDEN")

    for r in results:
        r["css_class"]   = {"PASS": "pass", "FAIL": "fail", "NO GOLDEN": "no-golden"}[r["status"]]
        r["badge_class"] = {"PASS": "pass", "FAIL": "fail", "NO GOLDEN": "warn"}[r["status"]]

    html = Template(REPORT_TEMPLATE).render(
        generated_at=datetime.now().isoformat(timespec="seconds"),
        subscriber_id=subscriber_id,
        since=since or "beginning",
        total=total,
        results=results,
        pass_count=pass_count,
        fail_count=fail_count,
        no_golden_count=no_golden_count,
    )
    path.write_text(html)
    return path

# ─── SSH TUNNEL CONTEXT ───────────────────────────────────────────────────────

def open_tunnel():
    tunnel = sshtunnel.SSHTunnelForwarder(
        (SSH_HOST, 22),
        ssh_username=SSH_USER,
        ssh_pkey=SSH_KEY,
        remote_bind_address=(DB_HOST, DB_PORT),
    )
    tunnel.start()
    return tunnel


def connect_db(tunnel):
    return psycopg2.connect(
        host="127.0.0.1",
        port=tunnel.local_bind_port,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )

# ─── MODES ────────────────────────────────────────────────────────────────────

def mode_capture(args):
    """Save current notifications as golden snapshots."""
    print(f"📸 Capture mode | subscriber={args.subscriber} | since={args.since}")
    tunnel = open_tunnel()
    conn   = connect_db(tunnel)
    cur    = conn.cursor()

    rows = get_notifications(cur, args.subscriber, args.since)
    print(f"  Fetched {len(rows)} notifications")

    saved = {}
    for row in rows:
        try:
            payload = clean_payload(row["payload"])
            key = notification_key(payload)
            if key not in saved:
                save_golden(key, payload)
                saved[key] = True
            else:
                print(f"  ⚠️  Duplicate key {key} — keeping first occurrence")
        except Exception as e:
            print(f"  ⚠️  Error processing row {row['id']}: {e}")

    cur.close(); conn.close(); tunnel.stop()
    print(f"\nDone. {len(saved)} golden snapshots saved to {GOLDEN_DIR}/")


def mode_compare(args):
    """Compare new notifications against golden snapshots and generate report."""
    print(f"🔍 Compare mode | subscriber={args.subscriber} | since={args.since}")
    tunnel = open_tunnel()
    conn   = connect_db(tunnel)
    cur    = conn.cursor()

    rows = get_notifications(cur, args.subscriber, args.since)
    print(f"  Fetched {len(rows)} notifications")

    results = []
    for row in rows:
        try:
            payload = clean_payload(row["payload"])
            key = notification_key(payload)
            golden = load_golden(key)

            if golden is None:
                results.append({
                    "db_id": row["id"],
                    "create_time": str(row["create_time"]),
                    "key": key,
                    "status": "NO GOLDEN",
                    "findings": [],
                })
                print(f"  ⚠️  {key} — no golden found")
                continue

            diff = compare_payloads(golden, payload)
            findings = diff_to_human(diff)

            status = "PASS" if not findings else "FAIL"
            icon   = "✅" if status == "PASS" else "❌"
            print(f"  {icon} {key} — {len(findings)} differences")

            results.append({
                "db_id": row["id"],
                "create_time": str(row["create_time"]),
                "key": key,
                "status": status,
                "findings": findings,
            })
        except Exception as e:
            print(f"  ⚠️  Error processing row {row['id']}: {e}")

    cur.close(); conn.close(); tunnel.stop()

    report_path = generate_report(results, args.subscriber, args.since, len(rows))
    print(f"\n📄 Report saved: {report_path}")


def mode_watch(args):
    """
    Watch mode: poll the DB every N seconds.
    Useful when you're actively running a flow and want live comparison.
    """
    print(f"👁  Watch mode | subscriber={args.subscriber} | interval={args.poll_interval}s")
    print("  Press Ctrl+C to stop and generate report.\n")

    tunnel = open_tunnel()
    conn   = connect_db(tunnel)
    cur    = conn.cursor()

    seen_ids = set()
    results  = []
    since    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    try:
        while True:
            rows = get_notifications(cur, args.subscriber, since)
            new_rows = [r for r in rows if r["id"] not in seen_ids]

            for row in new_rows:
                seen_ids.add(row["id"])
                try:
                    payload = clean_payload(row["payload"])
                    key = notification_key(payload)
                    golden = load_golden(key)

                    if golden is None:
                        print(f"  ⚠️  [{row['id']}] {key} — no golden")
                        results.append({
                            "db_id": row["id"],
                            "create_time": str(row["create_time"]),
                            "key": key, "status": "NO GOLDEN", "findings": [],
                        })
                        continue

                    diff = compare_payloads(golden, payload)
                    findings = diff_to_human(diff)
                    status = "PASS" if not findings else "FAIL"
                    icon   = "✅" if status == "PASS" else "❌"
                    print(f"  {icon} [{row['id']}] {key} — {len(findings)} diff(s)")

                    if findings:
                        for f in findings[:5]:  # preview first 5
                            print(f"       {f['type']:20s} {f['path']}  {f['detail']}")
                        if len(findings) > 5:
                            print(f"       ... and {len(findings)-5} more")

                    results.append({
                        "db_id": row["id"],
                        "create_time": str(row["create_time"]),
                        "key": key, "status": status, "findings": findings,
                    })
                except Exception as e:
                    print(f"  ⚠️  Error row {row['id']}: {e}")

            time.sleep(args.poll_interval)

    except KeyboardInterrupt:
        print("\n\nStopped. Generating report...")

    cur.close(); conn.close(); tunnel.stop()

    if results:
        report_path = generate_report(results, args.subscriber, since, len(seen_ids))
        print(f"📄 Report saved: {report_path}")
    else:
        print("No notifications captured.")

# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="WMS Notification Comparator")
    sub = parser.add_subparsers(dest="mode", required=True)

    # capture
    p_cap = sub.add_parser("capture", help="Save notifications as golden snapshots")
    p_cap.add_argument("--since",      default=None, help="ISO datetime filter, e.g. '2026-06-03 10:00:00'")
    p_cap.add_argument("--subscriber", type=int, default=158)

    # compare
    p_cmp = sub.add_parser("compare", help="Compare notifications against golden snapshots")
    p_cmp.add_argument("--since",      required=True, help="ISO datetime filter")
    p_cmp.add_argument("--subscriber", type=int, default=158)

    # watch
    p_watch = sub.add_parser("watch", help="Live watch mode — poll and compare in real time")
    p_watch.add_argument("--subscriber",    type=int, default=158)
    p_watch.add_argument("--poll-interval", type=int, default=3)

    args = parser.parse_args()

    if args.mode == "capture":
        mode_capture(args)
    elif args.mode == "compare":
        mode_compare(args)
    elif args.mode == "watch":
        mode_watch(args)


if __name__ == "__main__":
    main()

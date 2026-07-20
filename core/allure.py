"""Allure results building, zipping, and CLI generation."""
import json
import shutil
import subprocess
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from core.config import *
from core.diffing import fail_count

ALLURE_DIR = BASE_DIR / "allure-results"

def _findings_summary(findings):
    if not findings:
        return "matches golden"
    nfail = fail_count(findings)
    nwarn = len(findings) - nfail
    parts = []
    if nfail: parts.append(f"{nfail} difference(s)")
    if nwarn: parts.append(f"{nwarn} warning(s)")
    return ", ".join(parts)

ALLURE_STATUS = {
    "PASS": "passed", "FAIL": "failed",
    "NO GOLDEN": "skipped", "NO BASELINE": "skipped", "ERROR": "broken",
}

def build_allure_results(results, meta, start_ms, stop_ms):
    """Write allure-results JSON files into a fresh per-run dir. Returns (dir, run_id)."""
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ALLURE_DIR / run_id
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)
    project = meta.get("Project", "") or "default"
    gsource = meta.get("Golden source", "db")
    for r in results:
        u = str(uuid.uuid4())
        key = r.get("key", "?")
        flow = r.get("flow") or key.split("__")[0]
        findings = r.get("findings", [])
        trace = "\n".join(f"{f.get('type','')}: {f.get('path','')} {f.get('detail','')}".strip()
                          for f in findings)
        attachments = []
        if r.get("payload") is not None:
            att = f"{u}-payload.json"
            (out / att).write_text(json.dumps(r["payload"], indent=2, default=str))
            attachments.append({"name": "payload.json", "source": att, "type": "application/json"})
        res = {
            "uuid": u,
            # Unique per row, not just per key — otherwise multiple rows that
            # share the same notification key (common in a single run) get
            # treated by Allure as retries of the *same* test case and get
            # collapsed, silently hiding a failed row behind a later passing
            # one with the same key (or vice versa) instead of showing both.
            "historyId": f"{project}.{key}.{r.get('db_id', u)}",
            "name": key + (f"  [{r.get('ext_id')}]" if r.get("ext_id") else ""),
            "fullName": f"{flow}.{key}",
            "status": ALLURE_STATUS.get(r.get("status"), "unknown"),
            "statusDetails": {
                "message": _findings_summary(findings),
                "trace": trace,
            },
            "stage": "finished",
            "start": start_ms,
            "stop": stop_ms,
            "labels": [
                {"name": "feature", "value": flow},
                {"name": "suite", "value": project},
                {"name": "parentSuite", "value": f"{gsource} golden"},
                {"name": "framework", "value": "NotificationComparator"},
            ],
            "parameters": [{"name": "request id", "value": str(r.get("ext_id") or "")}],
            "attachments": attachments,
        }
        (out / f"{u}-result.json").write_text(json.dumps(res, indent=2))

    (out / "environment.properties").write_text(
        "\n".join(f"{k.replace(' ', '_')}={v}" for k, v in meta.items()))
    (out / "categories.json").write_text(json.dumps([
        {"name": "Schema / value mismatches", "matchedStatuses": ["failed"]},
        {"name": "Missing golden", "matchedStatuses": ["skipped"]},
        {"name": "Errors", "matchedStatuses": ["broken"]},
    ], indent=2))
    return out, run_id

def zip_dir(src_dir, zip_path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for p in src_dir.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(src_dir))

def try_allure_generate(results_dir):
    """If the allure CLI is installed, build an HTML report. Returns dir name or None."""
    allure = shutil.which("allure")
    if not allure:
        return None
    html = results_dir.parent / f"{results_dir.name}-html"
    try:
        subprocess.run([allure, "generate", str(results_dir), "-o", str(html), "--clean"],
                       check=True, capture_output=True, timeout=180)
        return html
    except Exception:
        return None

def generate_allure(results, meta, start_dt, stop_dt):
    """Build allure-results, zip them, and (if CLI present) the HTML report.
    Returns {'zip': name, 'html': name|None, 'run_id': id}."""
    start_ms = int(start_dt.timestamp() * 1000)
    stop_ms = int(stop_dt.timestamp() * 1000)
    out, run_id = build_allure_results(results, meta, start_ms, stop_ms)
    zip_name = f"allure_{run_id}.zip"
    zip_dir(out, REPORTS_DIR / zip_name)
    html_dir = try_allure_generate(out)
    return {"zip": zip_name, "html": (html_dir.name if html_dir else None), "run_id": run_id}

# ─── FULL RUN THREAD (live compare across ALL flows) ──────────────────────────


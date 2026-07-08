"""HTML report rendering, payload colorization, and report metadata."""
import json
from datetime import datetime

from core.config import REPORTS_DIR

def _diff_dot_paths(findings):
    """DeepDiff paths (root['a']['b'][0]) -> dot-paths (a.b.0). Mirrors the UI colorizer."""
    import re
    out = set()
    for f in findings or []:
        segs = re.findall(r"\[['\"]?[^\]'\"]+['\"]?\]", f.get("path", ""))
        path = ".".join(seg.strip("[]'\"") for seg in segs)
        if path:
            out.add(path)
    return out

def _dot_path_bad(path, diff_paths):
    if not path:
        return False
    if path in diff_paths:
        return True
    return any(path.startswith(d + ".") for d in diff_paths)

def color_payload_html(payload, findings):
    """Pretty-print payload to HTML lines: green = matches golden, red = exact mismatch."""
    dp = _diff_dot_paths(findings)

    def esc(s):
        return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    lines = []

    def walk(node, path, indent, key, comma):
        pad = "  " * indent
        bad = _dot_path_bad(path, dp)
        prefix = f'"{key}": ' if key is not None else ""
        if isinstance(node, (dict, list)):
            is_arr = isinstance(node, list)
            lines.append((pad + prefix + ("[" if is_arr else "{"), bad))
            ents = list(enumerate(node)) if is_arr else list(node.items())
            for i, (k, v) in enumerate(ents):
                cp = f"{path}.{k}" if path else str(k)
                walk(v, cp, indent + 1, (None if is_arr else k), i < len(ents) - 1)
            lines.append((pad + ("]" if is_arr else "}") + ("," if comma else ""), bad))
        else:
            lines.append((pad + prefix + json.dumps(node, default=str) + ("," if comma else ""), bad))

    walk(payload, "", 0, None, False)
    spans = []
    for text, bad in lines:
        style = ("color:#b91c1c;background:#fee2e2;display:block;padding:0 4px"
                 if bad else "color:#15803d;display:block;padding:0 4px")
        spans.append(f'<span style="{style}">{esc(text)}</span>')
    return "".join(spans)

def build_html_report(results, title="Notification Comparison Report", meta=None):
    """Self-contained HTML report of a comparison run (green=pass, red=fail)."""
    meta = meta or {}
    total = len(results)
    npass = sum(1 for r in results if r["status"] == "PASS")
    nfail = sum(1 for r in results if r["status"] == "FAIL")
    nother = total - npass - nfail

    def esc(s):
        return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

    rows = []
    for r in results:
        st = r["status"]
        color = {"PASS": "#16a34a", "FAIL": "#dc2626"}.get(st, "#b45309")
        findings = "".join(
            f'<div style="color:#b91c1c;font-family:monospace;font-size:12px;padding:2px 0">'
            f'<b>{esc(f["type"])}</b> {esc(f["path"])} {esc(f.get("detail",""))}</div>'
            for f in r.get("findings", [])
        ) or '<div style="color:#16a34a;font-size:12px">✓ matches golden</div>'
        if r.get("payload") is not None:
            findings += (
                '<details style="margin-top:6px"' + (' open' if r.get("findings") else '') + '>'
                '<summary style="cursor:pointer;font-size:11px;color:#475569">'
                'payload (<span style="color:#15803d">green = matches golden</span>, '
                '<span style="color:#b91c1c">red = mismatch</span>)</summary>'
                '<pre style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:6px;'
                'padding:10px;overflow:auto;font-size:11px;line-height:1.55;margin:6px 0 0;'
                'font-family:ui-monospace,Menlo,monospace;white-space:pre">'
                + color_payload_html(r["payload"], r.get("findings", []))
                + '</pre></details>'
            )
        rows.append(f"""
        <tr style="border-bottom:1px solid #e5e7eb">
          <td style="padding:8px;font-family:monospace;font-size:12px">{esc(r.get('db_id',''))}</td>
          <td style="padding:8px;font-family:monospace;font-size:12px">{esc(r.get('key',''))}</td>
          <td style="padding:8px;font-family:monospace;font-size:11px;color:#6b7280">{esc(r.get('ext_id','') or '—')}</td>
          <td style="padding:8px"><b style="color:{color}">{esc(st)}</b></td>
          <td style="padding:8px">{findings}</td>
        </tr>""")

    meta_rows = "".join(
        f'<span style="margin-right:18px"><b>{esc(k)}:</b> {esc(v)}</span>' for k, v in meta.items()
    )

    # SVG donut chart (circumference-based dash segments)
    import math
    R, C = 60, 2 * math.pi * 60
    base = total or 1
    seg_pass, seg_fail = C * npass / base, C * nfail / base
    seg_other = C * nother / base
    pct_pass = round(npass / base * 100)
    donut = f"""<svg width="160" height="160" viewBox="0 0 160 160">
      <circle cx="80" cy="80" r="{R}" fill="none" stroke="#e5e7eb" stroke-width="22"/>
      <g transform="rotate(-90 80 80)">
        <circle cx="80" cy="80" r="{R}" fill="none" stroke="#16a34a" stroke-width="22"
                stroke-dasharray="{seg_pass:.2f} {C - seg_pass:.2f}" stroke-dashoffset="0"/>
        <circle cx="80" cy="80" r="{R}" fill="none" stroke="#dc2626" stroke-width="22"
                stroke-dasharray="{seg_fail:.2f} {C - seg_fail:.2f}" stroke-dashoffset="{-seg_pass:.2f}"/>
        <circle cx="80" cy="80" r="{R}" fill="none" stroke="#eab308" stroke-width="22"
                stroke-dasharray="{seg_other:.2f} {C - seg_other:.2f}" stroke-dashoffset="{-(seg_pass + seg_fail):.2f}"/>
      </g>
      <text x="80" y="74" text-anchor="middle" font-size="26" font-weight="700" fill="#0f172a">{pct_pass}%</text>
      <text x="80" y="94" text-anchor="middle" font-size="11" fill="#64748b">pass</text>
    </svg>"""

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{esc(title)}</title></head>
<body style="font-family:-apple-system,Segoe UI,sans-serif;background:#f8fafc;color:#0f172a;margin:0;padding:28px">
  <h1 style="font-size:20px;margin:0 0 4px">{esc(title)}</h1>
  <div style="font-size:12px;color:#64748b;margin-bottom:16px">{meta_rows}</div>
  <div style="display:flex;align-items:center;gap:28px;margin-bottom:20px;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:16px">
    {donut}
    <div style="font-size:13px">
      <div style="margin:5px 0"><span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:#16a34a;margin-right:7px"></span>Pass <b>{npass}</b></div>
      <div style="margin:5px 0"><span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:#dc2626;margin-right:7px"></span>Fail <b>{nfail}</b></div>
      <div style="margin:5px 0"><span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:#eab308;margin-right:7px"></span>Other <b>{nother}</b></div>
    </div>
  </div>
  <div style="display:flex;gap:12px;margin-bottom:20px">
    <div style="flex:1;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px;text-align:center"><div style="font-size:26px;font-weight:700">{total}</div><div style="font-size:11px;color:#64748b">TOTAL</div></div>
    <div style="flex:1;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px;text-align:center"><div style="font-size:26px;font-weight:700;color:#16a34a">{npass}</div><div style="font-size:11px;color:#64748b">PASS</div></div>
    <div style="flex:1;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px;text-align:center"><div style="font-size:26px;font-weight:700;color:#dc2626">{nfail}</div><div style="font-size:11px;color:#64748b">FAIL</div></div>
    <div style="flex:1;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px;text-align:center"><div style="font-size:26px;font-weight:700;color:#b45309">{nother}</div><div style="font-size:11px;color:#64748b">OTHER</div></div>
  </div>
  <table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden">
    <thead><tr style="background:#f1f5f9;text-align:left"><th style="padding:8px;font-size:11px;color:#64748b">ID</th><th style="padding:8px;font-size:11px;color:#64748b">KEY</th><th style="padding:8px;font-size:11px;color:#64748b">REQUEST ID</th><th style="padding:8px;font-size:11px;color:#64748b">STATUS</th><th style="padding:8px;font-size:11px;color:#64748b">FINDINGS</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</body></html>"""

def save_report(html, prefix="report"):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"{prefix}_{ts}.html"
    (REPORTS_DIR / name).write_text(html)
    return name

def save_report_meta(report_name, results, project="", mode="full", per_flow=None,
                     created=None, kind="run", allure_zip=None, allure_html=None):
    """Write a sidecar so the dashboard list can show project/time/pass-fail
    (and persistent Allure links) without opening each HTML report."""
    total = len(results)
    npass = sum(1 for r in results if r.get("status") == "PASS")
    nfail = sum(1 for r in results if r.get("status") == "FAIL")
    meta = {
        "name": report_name,
        "project": project or "(none)",
        "created": created or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode": mode,
        "kind": kind,
        "total": total,
        "pass": npass,
        "fail": nfail,
        "other": total - npass - nfail,
        "per_flow": per_flow or {},
        "allure_zip": allure_zip,
        "allure_html": allure_html,
    }
    (REPORTS_DIR / f"{report_name}.meta.json").write_text(json.dumps(meta, indent=2))
    return meta

def _created_from_name(name):
    """Parse 'prefix_YYYYMMDD_HHMMSS.html' -> 'YYYY-MM-DD HH:MM:SS' (best effort)."""
    try:
        stem = name.rsplit(".", 1)[0]
        d, t = stem.split("_")[-2], stem.split("_")[-1]
        return f"{d[:4]}-{d[4:6]}-{d[6:8]} {t[:2]}:{t[2:4]}:{t[4:6]}"
    except Exception:
        return ""

def list_reports_meta():
    """Return report descriptors (newest first), reading sidecars when present."""
    out = []
    for p in REPORTS_DIR.glob("*.html"):
        sidecar = REPORTS_DIR / f"{p.name}.meta.json"
        if sidecar.exists():
            try:
                out.append(json.loads(sidecar.read_text()))
                continue
            except Exception:
                pass
        out.append({"name": p.name, "created": _created_from_name(p.name)})
    # Sort by actual creation time, not filename — report names have different
    # prefixes (full_run_, run_all_, topic_compare_...) so sorting by the raw
    # filename alphabetizes by prefix first and scrambles the true time order.
    out.sort(key=lambda r: r.get("created") or "", reverse=True)
    # Stable id, independent of sort/filter order in the UI — oldest report is 1.
    total = len(out)
    for i, rep in enumerate(out):
        rep["id"] = total - i
    return out

# ─── RUN-ALL (collective comparison across all DB flows) ──────────────────────


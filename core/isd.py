"""ISD (PDF) parsing and golden capture."""
import json
import re
from pathlib import Path

from core.diffing import *
from core.golden import save_golden, current_project, golden_path

# PyMuPDF is only needed for ISD PDF parsing; keep it optional.
try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

def clean_isd_text(text):
    """Strip PDF furniture that gets injected into the middle of multi-page JSON:
    page footers ('Page 39'), the GreyOrange logo line, and zero-width spaces."""
    import re
    text = text.replace("​", "")
    noise = re.compile(r"^\s*(Page\s+\d+|Grey\s?Orange.*)\s*$", re.I)
    return "\n".join(l for l in text.split("\n") if not noise.match(l))

def _try_json(chunk):
    """json.loads with a tolerant repair pass for common ISD-PDF corruption:
    smart quotes used as delimiters and trailing commas."""
    import re
    try:
        return json.loads(chunk)
    except Exception:
        pass
    repaired = (chunk.replace("“", '"').replace("”", '"')
                     .replace("‘", "'").replace("’", "'"))
    repaired = re.sub(r",(\s*[}\]])", r"\1", repaired)   # trailing commas
    try:
        return json.loads(repaired)
    except Exception:
        return None

def extract_json_objects(text):
    """Scan text for balanced JSON objects. Returns (objects, attempted_count)."""
    objs, attempts, i, n = [], 0, 0, len(text)
    while i < n:
        if text[i] != "{":
            i += 1
            continue
        depth, instr, esc, j = 0, False, False, i
        while j < n:
            c = text[j]
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                instr = not instr
            elif not instr:
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        chunk = text[i:j + 1]
                        attempts += 1
                        obj = _try_json(chunk)
                        if isinstance(obj, dict) and obj:
                            objs.append(obj)
                        break
            j += 1
        i = j + 1
    return objs, attempts

def guess_project_name(text, filename=""):
    """Best-effort project/interface name from the ISD header or filename."""
    import re
    for line in (text or "").splitlines():
        s = line.strip()
        m = re.match(r"(?i)(project|interface|module)\s*[:\-]\s*(.+)", s)
        if m and m.group(2).strip():
            return m.group(2).strip()[:60]
    if filename:
        return Path(filename).stem[:60]
    return ""

def parse_isd_pdf(data, filename=""):
    """
    Returns {"project": str, "pages": int, "payloads": [raw dicts], "text_len": int}.
    payloads = sample JSON notifications found in the document.
    """
    if fitz is None:
        raise RuntimeError("PyMuPDF not installed. Run: pip install PyMuPDF")
    doc = fitz.open(stream=data, filetype="pdf")
    pages = doc.page_count
    raw_text = "\n".join(page.get_text() for page in doc)
    doc.close()
    text = clean_isd_text(raw_text)
    payloads, attempts = extract_json_objects(text)
    return {
        "project": guess_project_name(raw_text, filename),
        "pages": pages,
        "text_len": len(text),
        "payloads": payloads,
        "attempts": attempts,           # how many balanced {...} blocks we tried
    }

def unwrap_notification(obj):
    """From an ISD Kafka envelope, return (notification_data, notification_type).
    Handles {payload:{notification_type, notification_data}} and flat shapes."""
    pl = obj.get("payload") if isinstance(obj.get("payload"), dict) else obj
    nt = pl.get("notification_type") or obj.get("notification_type") or ""
    nd = pl.get("notification_data") or obj.get("notification_data")
    if isinstance(nd, dict):
        return nd, nt
    return None, nt

def isd_golden_key(nd, notification_type):
    """Build a golden key from a notification_data block. Prefers {FLOW}__{state}__{status};
    falls back to the notification_type when type/state/status are absent (dock, cancel, tag)."""
    flow   = str(nd.get("type") or "").strip().upper()
    state  = str(nd.get("state") or "").strip().lower()
    status = str(nd.get("status") or "").strip().upper()
    if not flow:
        flow = (notification_type or "NOTIFICATION").strip().upper()
    import re
    parts = [re.sub(r"[^A-Za-z0-9.-]", "_", p) for p in (flow, state, status) if p]
    return "__".join(parts)

def capture_isd_goldens(payloads):
    """Unwrap each ISD envelope to its notification_data, key it, and save the
    largest payload per key as golden. Returns summary list."""
    best = {}
    for raw in payloads:
        nd, nt = unwrap_notification(raw)
        if not nd:
            continue
        try:
            payload = clean_payload(nd)
            key = isd_golden_key(nd, nt)
            if not key:
                continue
            size = len(json.dumps(payload, default=str))
            if key not in best or size > best[key]["size"]:
                best[key] = {"payload": payload, "size": size}
        except Exception:
            continue
    saved = []
    for key, info in best.items():
        save_golden(key, info["payload"], source="isd")
        saved.append({"key": key, "count": 1})
    return saved

# ─── HTML REPORT (downloadable execution report) ──────────────────────────────


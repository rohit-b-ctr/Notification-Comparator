"""ISD (PDF) parsing and golden capture."""
import json
import re
from pathlib import Path

from core.diffing import *
from core.golden import save_golden, load_golden, current_project, golden_path
from core.config import get_cfg
from core.kowl import (as_kowl_envelope, kowl_notification_data, topic_notif_key, clean_topic_payload,
                       load_topic_baseline, save_topic_baseline, topic_short)

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

def _scan_balanced_object(text, i):
    """From text[i] == '{', find the matching closing brace (quote/escape aware).
    Returns (chunk, end_index) or (None, None) if unbalanced (ran off the end)."""
    n = len(text)
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
                    return text[i:j + 1], j + 1
        j += 1
    return None, None

def extract_json_objects(text):
    """Scan text for every balanced JSON object, anywhere. Returns (objects,
    attempted_count, failed_blocks). failed_blocks is a list of {"start": char
    offset, "preview": first ~100 chars, "raw": full chunk} for every balanced
    {...} block that couldn't be parsed even after repair.

    This is the unscoped fallback — it has no idea which blocks are actual
    notification payloads vs. unrelated example JSON elsewhere in the document,
    so prefer extract_topic_anchored_objects() when the doc follows the usual
    "Kafka Topic: <topic> ... JSON Payload: {...}" layout (see parse_isd_pdf)."""
    objs, attempts, failed, i, n = [], 0, [], 0, len(text)
    while i < n:
        if text[i] != "{":
            i += 1
            continue
        chunk, end = _scan_balanced_object(text, i)
        if chunk is None:
            break
        attempts += 1
        obj = _try_json(chunk)
        if isinstance(obj, dict) and obj:
            objs.append(obj)
        else:
            preview = re.sub(r"\s+", " ", chunk[:100]).strip()
            failed.append({"start": i, "preview": preview, "raw": chunk})
        i = end
    return objs, attempts, failed

# Matches a "Kafka Topic" heading (often stacked on two lines in PDF table
# cells) followed by the topic name on its own line, e.g.:
#   Kafka\nTopic\naph.service-request-put-notification.events
_KAFKA_TOPIC_RE = re.compile(r"Kafka\s+Topic\s*[\r\n]+\s*([^\r\n]+)", re.I)
# Matches a "JSON Payload" heading, also possibly stacked on two lines.
_JSON_PAYLOAD_RE = re.compile(r"JSON\s+Payload", re.I)

def extract_topic_anchored_objects(text, known):
    """Scan for 'Kafka Topic: <topic>' headings whose topic name contains one
    of the user's configured pattern/topic names, then grab the JSON block
    under the 'JSON Payload' heading that follows it. This is what keeps
    capture scoped to notifications you actually configured, instead of
    picking up every unrelated {...} block elsewhere in the ISD (example
    configs, unrelated sample payloads, etc.) and instead of silently missing
    real ones buried among that noise.

    known: list of (label, substring, kind) triples from Config's patterns
    ("db") + Kowl topics ("kowl") — substring matched as a case-insensitive
    substring of the topic line (the doc's topic name usually carries an
    env-specific prefix/suffix like "aph." / ".events" the configured pattern
    doesn't have), label is that pattern/topic's configured human label,
    carried through on each match so capture_isd_goldens_labeled() can
    key/file goldens by label — ISD specs often reuse identical placeholder
    example values across many different notification sections, so without a
    label those would collapse into one golden and silently overwrite each
    other. `kind` is carried through too so resolve_golden_target() knows
    whether this match came from a Kowl topic or a DB pattern WITHOUT having
    to guess from the JSON's shape — an ISD doc's "JSON Payload" is often just
    the flat notification body (no Kowl envelope wrapper), which shape
    detection alone can't tell apart from a DB notification.

    Returns (matches, attempted_count, failed_blocks) where matches is a list
    of {"payload": obj, "label": label, "topic": topic_name, "kind": kind}.
    """
    matches, attempts, failed = [], 0, []
    known = [(lbl, s.lower(), kind) for lbl, s, kind in known if s]
    if not known:
        return matches, attempts, failed
    for m in _KAFKA_TOPIC_RE.finditer(text):
        topic_name = m.group(1).strip()
        topic_lower = topic_name.lower()
        hit = next(((lbl, kind) for lbl, s, kind in known if s in topic_lower), None)
        if hit is None:
            continue
        hit_label, hit_kind = hit
        pm = _JSON_PAYLOAD_RE.search(text, m.end())
        if not pm:
            continue
        brace_idx = text.find("{", pm.end())
        if brace_idx == -1:
            continue
        chunk, _end = _scan_balanced_object(text, brace_idx)
        if chunk is None:
            continue
        attempts += 1
        obj = _try_json(chunk)
        if isinstance(obj, dict) and obj:
            matches.append({"payload": obj, "label": hit_label, "topic": topic_name, "kind": hit_kind})
        else:
            preview = re.sub(r"\s+", " ", chunk[:100]).strip()
            failed.append({"start": brace_idx, "preview": preview, "raw": chunk})
    return matches, attempts, failed

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

def _page_for_offset(offset, page_offsets):
    """Which 1-based page a character offset in the joined/cleaned text falls on."""
    page = 1
    for i, start in enumerate(page_offsets):
        if offset >= start:
            page = i + 1
        else:
            break
    return page

def parse_isd_pdf(data, filename="", known=None):
    """
    Returns {"project": str, "pages": int, "payloads": [raw dicts],
    "labeled_payloads": [{"payload","label","topic"}], "text_len": int,
    "failed_blocks": [{"page", "preview", "raw"}], "scoped": bool}.

    known (from Config's patterns + Kowl topics, as (label, substring, kind) triples):
    when given, capture is scoped to "Kafka Topic: <topic>" sections whose
    topic name matches one of these — so unrelated example JSON elsewhere in
    the doc is never captured as a golden, and real configured notifications
    aren't lost among that noise. Each hit carries its configured label
    through in "labeled_payloads", used by capture_isd_goldens_labeled() to
    keep same-looking examples from different notifications from colliding.

    "scoped" reports whether this anchored scan actually found anything; if it
    found nothing (e.g. the doc doesn't follow that Kafka Topic/JSON Payload
    layout at all), we fall back to scanning the whole document for any
    balanced {...} block (into "payloads", unlabeled), same as before this
    scoping existed.
    """
    if fitz is None:
        raise RuntimeError("PyMuPDF not installed. Run: pip install PyMuPDF")
    doc = fitz.open(stream=data, filetype="pdf")
    pages = doc.page_count
    raw_text = "\n".join(page.get_text() for page in doc)
    # Clean per-page (not on the joined text) so we can track each page's start
    # offset in the final joined+cleaned text — needed to map a failed block's
    # character position back to a page number.
    page_texts = [clean_isd_text(page.get_text()) for page in doc]
    doc.close()
    page_offsets, cum = [], 0
    for pt in page_texts:
        page_offsets.append(cum)
        cum += len(pt) + 1  # +1 for the "\n" joiner below
    text = "\n".join(page_texts)

    scoped = False
    payloads, labeled_payloads, attempts, failed = [], [], 0, []
    if known:
        labeled_payloads, attempts, failed = extract_topic_anchored_objects(text, known)
        scoped = attempts > 0
    if not scoped:
        payloads, attempts, failed = extract_json_objects(text)

    failed_blocks = [
        {"page": _page_for_offset(f["start"], page_offsets), "preview": f["preview"], "raw": f["raw"]}
        for f in failed
    ]
    return {
        "project": guess_project_name(raw_text, filename),
        "pages": pages,
        "text_len": len(text),
        "payloads": payloads,
        "labeled_payloads": labeled_payloads,
        "failed_blocks": failed_blocks,
        "attempts": attempts,           # how many balanced {...} blocks we tried
        "scoped": scoped,               # True if capture was limited to configured patterns/topics
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

def resolve_golden_target(raw, label=None, topic=None, kind=None):
    """Work out whether a raw ISD/paste payload is a Kowl-envelope or a
    DB/ISD notification, and derive the SAME key a real live capture of that
    notification would produce — so it can be filed as a genuine db or kowl
    golden (no separate isd bucket), and therefore be found later by an
    ordinary DB/Kowl compare instead of needing a dedicated ISD compare mode.

    kind ("kowl"/"db"), when already known from a Kafka-Topic-anchored ISD
    match (see extract_topic_anchored_objects), is trusted OVER shape
    detection. This matters because an ISD doc's "JSON Payload" for a Kowl
    topic is often just the flat notification body — no name/payload
    envelope wrapper — which as_kowl_envelope() has no way to recognize as
    Kowl-shaped even though the doc's own "Kafka Topic:" heading already told
    us it is one; trusting shape alone there would misfile it as a db golden
    keyed in a format live Kowl compare never looks for.

    label/topic, when already known, are reused as-is so a Kowl-shaped hit
    keys identically to what a real live Kowl capture of that topic would
    produce (topic_notif_key needs a (label, topic) pair). When kind/label/
    topic are unknown (unscoped extraction, or the paste box), fall back to
    shape detection — best-effort matching the envelope's 'name' against
    configured Kowl topics, or using the name itself as label/topic.

    Returns {"source": "kowl"|"db", "key": str, "label": str|None, "payload": dict}
    or None if the payload can't be keyed at all.
    """
    env = as_kowl_envelope(raw)
    use_kowl = kind == "kowl" or (kind is None and env is not None)
    if kind == "db":
        use_kowl = False

    if use_kowl:
        if env is None:
            # Known (from the anchored match) to be Kowl-shaped, but the ISD
            # doc only showed the flat notification body — synthesize the
            # envelope wrapper a real live Kowl message would have, so key/
            # payload derivation below sees the same shape either way.
            env = {"name": topic_short(topic) if topic else (label or ""), "payload": raw}
        if label is None or topic is None:
            name = (env.get("name") or "").lower()
            for t in get_cfg().get("topics", []):
                topic_name = t.get("topic") or ""
                if name and name in topic_name.lower():
                    label, topic = t.get("label") or name, topic_name
                    break
            if label is None:
                label = env.get("name") or "NOTIFICATION"
                topic = env.get("name") or ""
        key = topic_notif_key(env, label, topic)
        if not key:
            return None
        return {"source": "kowl", "key": key, "label": label, "payload": clean_topic_payload(env)}

    nd, nt = unwrap_notification(raw)
    core = nd if nd else raw
    payload = clean_payload(core)
    # notif_key() first — the SAME deriver a live DB fetch uses (core/golden.py
    # process_rows), so a well-formed payload keys identically either way.
    # isd_golden_key() is only a fallback for payloads that genuinely lack
    # type/state/status (some ISD samples do — dock/cancel/tag notifications),
    # where notif_key()'s literal "UNKNOWN__unknown__UNKNOWN" placeholder would
    # be actively wrong.
    key = notif_key(payload)
    if key == "UNKNOWN__unknown__UNKNOWN":
        key = isd_golden_key(core, nt)
    # A payload with no type/state/status AND no explicit notification_type
    # isn't identifiable as a notification at all — most likely an unrelated
    # JSON block the unscoped whole-document scan picked up (schemas, example
    # configs, ...). Saving it would create a garbage "NOTIFICATION" golden;
    # skip it instead, same as the old unwrap_notification()-only capture did.
    if not key or key == "NOTIFICATION":
        return None
    return {"source": "db", "key": key, "label": label, "payload": payload}

def _golden_exists(source, key, label=None, project_kind="db"):
    if source == "kowl":
        return load_topic_baseline(key) is not None
    return load_golden(key, source="db", label=label, project_kind=project_kind) is not None

def _save_golden_if_missing(target, project_kind="db"):
    """Gap-fill only — never overwrite a golden a real live capture already
    produced for this exact key."""
    source, key, label, payload = target["source"], target["key"], target["label"], target["payload"]
    if _golden_exists(source, key, label=label, project_kind=project_kind):
        return False
    if source == "kowl":
        save_topic_baseline(key, payload)
    else:
        save_golden(key, payload, source="db", label=label, project_kind=project_kind)
    return True

def capture_isd_goldens(payloads, project_kind="db"):
    """For each ISD-extracted payload, resolve where a real live capture of
    that notification would file it (db or kowl golden), and save it there —
    filling a gap only, never overwriting an existing golden. Returns a
    summary list of what was actually saved (payloads matching an
    already-captured key are silently skipped)."""
    best = {}
    for raw in payloads:
        try:
            target = resolve_golden_target(raw)
            if target is None:
                continue
            dedup_key = (target["source"], target["label"], target["key"])
            size = len(json.dumps(target["payload"], default=str))
            if dedup_key not in best or size > best[dedup_key]["size"]:
                best[dedup_key] = {"target": target, "size": size}
        except Exception:
            continue
    saved = []
    for (source, label, key), info in best.items():
        if _save_golden_if_missing(info["target"], project_kind=project_kind):
            saved.append({"key": key, "source": source, "count": 1})
    return saved

def capture_isd_goldens_labeled(matches, project_kind="db"):
    """Like capture_isd_goldens, but each match already carries the configured
    pattern/topic (label, topic) it was extracted under (see
    extract_topic_anchored_objects) — reused directly so a Kowl-shaped hit
    keys identically to a real live Kowl capture of that same topic. Goldens
    are deduped by (label, key), not key alone — ISD spec documents often
    reuse identical placeholder examples across different notification
    sections, so keying by (type, state, status) alone would collapse
    different notifications together."""
    best = {}
    for m in matches:
        label = m.get("label") or None
        topic = m.get("topic") or None
        kind  = m.get("kind") or None
        try:
            target = resolve_golden_target(m["payload"], label=label, topic=topic, kind=kind)
            if target is None:
                continue
            dedup_key = (target["source"], target["label"], target["key"])
            size = len(json.dumps(target["payload"], default=str))
            if dedup_key not in best or size > best[dedup_key]["size"]:
                best[dedup_key] = {"target": target, "size": size}
        except Exception:
            continue
    saved = []
    for (source, label, key), info in best.items():
        if _save_golden_if_missing(info["target"], project_kind=project_kind):
            saved.append({"key": key, "label": label, "source": source, "count": 1})
    return saved

# ─── HTML REPORT (downloadable execution report) ──────────────────────────────


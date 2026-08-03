"""Payload normalization, dynamic-field stripping, and diff helpers.

Also hosts XML parsing for the XML comparator (xmltodict -> dict -> DeepDiff).
"""
import json
import re

try:
    import xmltodict  # XML comparator support
except ImportError:
    xmltodict = None

def normalize(obj):
    if isinstance(obj, dict):
        return {k: normalize(v) for k, v in obj.items() if k != "@type"}
    elif isinstance(obj, list):
        if len(obj) == 2 and isinstance(obj[0], str) and obj[0].startswith("java.") and isinstance(obj[1], list):
            return [normalize(i) for i in obj[1]]
        return [normalize(i) for i in obj]
    return obj

IGNORE_FIELDS = {
    "id", "eventdata_id", "notification_id", "execution_id",
    "createdOn", "updatedOn", "receivedOn", "create_time",
    "externalServiceRequestId", "sr_parent", "sr_parentsIds",
}

def strip_dynamic(obj):
    if isinstance(obj, dict):
        return {k: strip_dynamic(v) for k, v in obj.items() if k not in IGNORE_FIELDS}
    elif isinstance(obj, list):
        return [strip_dynamic(i) for i in obj]
    return obj

def deepdiff_path_to_dot(path):
    """DeepDiff's "root['a']['b'][0]" -> dot notation "a.b.0"."""
    segs = re.findall(r"\[['\"]?[^\]'\"]+['\"]?\]", path or "")
    return ".".join(seg.strip("[]'\"") for seg in segs)

def flatten_dict(obj, prefix=""):
    """Flatten a nested dict/list into {dot.path: leaf_value} — used to render
    a full field-by-field side-by-side view (not just the differences)."""
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(flatten_dict(v, f"{prefix}.{k}" if prefix else str(k)))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.update(flatten_dict(v, f"{prefix}.{i}" if prefix else str(i)))
    else:
        out[prefix] = obj
    return out

def clean_payload(raw):
    data = json.loads(raw) if isinstance(raw, str) else raw
    return strip_dynamic(normalize(data))

def notif_key(payload):
    # Derive key from type, state, status — all from notification_data
    # Shape 1: { notification_data: { type, state, status }, ... }
    # Shape 2: flat { type, state, status }
    nd     = payload.get("notification_data") or {}

    def pick(field, default="UNKNOWN"):
        val = nd.get(field) or payload.get(field)
        return str(val).strip() if val else default

    ftype  = pick("type").upper()
    state  = pick("state").lower()
    status = pick("status").upper()
    return f"{ftype}__{state}__{status}"

SCHEMA_ONLY_TYPES = {"dictionary_item_added", "dictionary_item_removed"}

def diff_to_list(diff, mode="full"):
    """
    mode='full'   — report all differences (missing keys, extra keys, value/type changes)
    mode='schema' — report only missing/extra keys, ignore value and type changes
    """
    out = []
    for change_type, changes in diff.items():
        # Schema-only: skip value/type/list-item changes
        if mode == "schema" and change_type not in SCHEMA_ONLY_TYPES:
            continue
        if change_type in ("dictionary_item_added", "dictionary_item_removed",
                           "iterable_item_added", "iterable_item_removed"):
            # DeepDiff(golden, payload) — "removed" means present in golden (t1) and
            # missing from the actual payload (t2); "added" means the reverse.
            # Spell that out instead of the raw DeepDiff jargon.
            is_removed = change_type.endswith("_removed")
            label = "Missing Field" if is_removed else "Extra Field"
            detail = "Present in the Golden Data but not in the Target Environment." if is_removed else "Not in Golden Data but Present in the Target Environment."
            for path in changes:
                out.append({"type": label, "path": str(path), "detail": detail})
        elif change_type in ("values_changed", "type_changes"):
            for path, info in changes.items():
                old_t, new_t = type(info['old_value']).__name__, type(info['new_value']).__name__
                # Always show both the value and its type — makes it obvious at a
                # glance whether this is a plain value drift (same type) or an
                # actual type mismatch (e.g. str -> int) hiding inside the values.
                detail = f"{info['old_value']!r} ({old_t}) → {info['new_value']!r} ({new_t})"
                out.append({"type": change_type.replace("_", " "), "path": str(path), "detail": detail})
    return out

# Finding types that indicate a real schema break — a field appeared/disappeared,
# or the same field changed data type (e.g. string -> int). These fail a compare.
# A plain "values changed" finding (same field, same type, different value — think
# a changing counter, a regenerated id, a timestamp) doesn't fail the compare —
# it's expected data drift, not a broken schema — but it's still surfaced as a
# yellow "warning" finding in the UI/report so it isn't silently invisible.
FAIL_FINDING_TYPES = {"type changes", "Missing Field", "Extra Field"}

def fail_count(findings):
    """How many findings are real schema breaks — excludes value-only warnings,
    so log/summary lines don't count a value drift as a 'diff'."""
    return sum(1 for f in (findings or []) if f.get("type") in FAIL_FINDING_TYPES)

def status_from_findings(findings):
    """PASS / FAIL verdict for a list of diff findings. A compare with only
    value-only warnings (no schema break) still passes overall."""
    if any(f.get("type") in FAIL_FINDING_TYPES for f in (findings or [])):
        return "FAIL"
    return "PASS"

def side_by_side_fields(golden, actual):
    """Full field-by-field baseline-vs-target view (unlike diff_to_list, this
    includes matching fields too, not just the differences) — used to render
    a side-by-side compare table. Each row's status:
      'same' — identical value
      'warn' — differs, but not a schema break (value-only drift, or a field
               that's in IGNORE_FIELDS and expected to always differ)
      'fail' — differs in a way that fails the compare (missing/extra key,
               type change) and isn't an ignored field

    Statuses are derived by comparing the two sides' flattened values
    directly at each path, rather than from a DeepDiff(ignore_order=True)
    pass: with ignore_order, DeepDiff re-pairs list items by similarity
    before reporting paths, so its paths don't reliably line up with the
    positional paths flatten_dict produces (which is what the table actually
    renders) — for lists that hold several similarly-shaped-but-different
    items (e.g. multiple subscriber rows for the same pattern), that mismatch
    silently left genuinely different cells marked 'same'.
    """
    g_full = normalize(golden)
    a_full = normalize(actual)
    b_flat = flatten_dict(g_full)
    t_flat = flatten_dict(a_full)
    _MISSING = object()

    def leaf_name(path):
        return path.split(".")[-1] if path else path

    rows = []
    for p in sorted(set(b_flat) | set(t_flat)):
        bval = b_flat.get(p, _MISSING)
        tval = t_flat.get(p, _MISSING)
        if leaf_name(p) in IGNORE_FIELDS:
            status = "warn"
        elif bval is _MISSING or tval is _MISSING:
            status = "fail"
        elif type(bval) is not type(tval):
            status = "fail"
        elif bval != tval:
            status = "warn"
        else:
            status = "same"
        # Distinguish "field absent on this side" from "field present with a
        # real null value" — both would otherwise render identically, making
        # a 'fail' row (baseline lacked a whole sub-object the target has)
        # look identical to an unrelated 'same' null-vs-null row.
        rows.append({
            "path": p,
            "baseline": "(not present)" if bval is _MISSING else bval,
            "target": "(not present)" if tval is _MISSING else tval,
            "status": status,
        })
    return rows

def xml_to_obj(text):
    """Parse an XML document into a plain dict so it can flow through the same
    DeepDiff pipeline as JSON. Raises ValueError on bad input or missing dep."""
    if xmltodict is None:
        raise ValueError("xmltodict not installed. Run: pip install xmltodict")
    if isinstance(text, (dict, list)):
        return text
    try:
        # force_list=None keeps single elements as dicts; attributes become @-prefixed keys
        return xmltodict.parse(text)
    except Exception as e:
        raise ValueError(f"not valid XML: {e}")

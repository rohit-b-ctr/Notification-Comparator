"""Payload normalization, dynamic-field stripping, and diff helpers.

Also hosts XML parsing for the XML comparator (xmltodict -> dict -> DeepDiff).
"""
import json

from deepdiff import DeepDiff  # type: ignore[import]

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
            for path in changes:
                out.append({"type": change_type.replace("_", " "), "path": str(path), "detail": ""})
        elif change_type in ("values_changed", "type_changes"):
            for path, info in changes.items():
                if change_type == "values_changed":
                    detail = f"{info['old_value']!r} → {info['new_value']!r}"
                else:
                    detail = f"{type(info['old_value']).__name__} → {type(info['new_value']).__name__}"
                out.append({"type": change_type.replace("_", " "), "path": str(path), "detail": detail})
    return out

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

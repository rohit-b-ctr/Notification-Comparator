"""Kowl/Kafka topic capture, baselines, and comparison."""
import json
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from deepdiff import DeepDiff  # type: ignore[import]

from core.config import *
from core.diffing import *
from core.golden import load_golden, save_golden, golden_path, golden_root, dedupe_by_key
from core.state import kowl_capture_state

# websocket-client is only needed for Kowl topic streaming; keep it optional.
try:
    import websocket  # websocket-client  # type: ignore[import]
except ImportError:
    websocket = None

TOPIC_IGNORE_FIELDS = IGNORE_FIELDS | {
    "message_id", "entity_id", "timestamp", "source_service",
    "transactionId", "transaction_start_time", "transaction_end_time",
}

def strip_fields(obj, fields):
    if isinstance(obj, dict):
        return {k: strip_fields(v, fields) for k, v in obj.items() if k not in fields}
    if isinstance(obj, list):
        return [strip_fields(i, fields) for i in obj]
    return obj

def normalize_kowl_envelope(env):
    """Normalize a Kowl envelope so env['payload'] is ALWAYS the flat
    notification data. Some messages double-wrap: env['payload'] is itself an
    ISD-style {notification_type, notification_data} envelope instead of the
    flat data. Un-nesting this ONCE, here, means every downstream consumer —
    key derivation (topic_notif_key) AND the actual stored/compared payload
    (clean_topic_payload) — sees one consistent shape. Doing this in only one
    of the two previously let a baseline captured from a flat-shaped message
    and a live double-wrapped message of the same notification match by key
    but still report spurious Missing/Extra Field diffs from the leftover
    wrapper layer."""
    if not isinstance(env, dict):
        return env
    inner = env.get("payload")
    if isinstance(inner, dict):
        nd = inner.get("notification_data")
        if isinstance(nd, dict):
            return {**env, "payload": nd}
    return env

def clean_topic_payload(env):
    """env = the message's value.payload envelope from Kowl."""
    return strip_fields(normalize(normalize_kowl_envelope(env)), TOPIC_IGNORE_FIELDS)

def apply_prefix(topics, prefix):
    """Prepend env prefix to each topic's name if a prefix is configured.
    e.g. prefix='aph', topic='item-create.requests' -> 'aph.item-create.requests'
    If topic already starts with the prefix, leave it unchanged (idempotent)."""
    if not prefix:
        return topics
    result = []
    for spec in topics:
        t = spec["topic"]
        full = t if t.startswith(prefix + ".") else f"{prefix}.{t}"
        result.append({**spec, "topic": full})
    return result

def topic_short(topic):
    """stpfunction-sbscloud.put_information.events -> put_information"""
    parts = topic.split(".")
    return parts[-2] if len(parts) >= 2 else topic

def _sr_path(sr):
    """Recursively build a type path for one serviceRequest node.
    e.g. PICK → 'PICK'
         PICK containing [PICK_LINE] → 'PICK/PICK_LINE'
         PICK containing [PICK_LINE containing [PICK_LINE]] → 'PICK/PICK_LINE/PICK_LINE'
    """
    if not isinstance(sr, dict):
        return ""
    top = (sr.get("type") or "").upper()
    nested = sr.get("serviceRequests")
    if nested and isinstance(nested, list):
        child_paths = [_sr_path(n) for n in nested if isinstance(n, dict)]
        child_paths = [p for p in child_paths if p]
        if child_paths:
            # Use the first child path to represent the chain (children should be homogeneous)
            return f"{top}/{child_paths[0]}"
    return top

def sr_type_path(env):
    """Derive a type-hierarchy signature from serviceRequests in the payload.

    Recursively walks serviceRequests so any depth is captured:
      PICK                      → 'PICK'
      PICK → PICK_LINE          → 'PICK/PICK_LINE'
      PICK → PICK_LINE → PICK_LINE → 'PICK/PICK_LINE/PICK_LINE'
    Multiple distinct top-level paths are joined with '+'.
    Returns '' for topics that have no serviceRequests field.
    """
    payload = env.get("payload") if isinstance(env.get("payload"), dict) else {}
    srs = payload.get("serviceRequests")
    if not srs or not isinstance(srs, list):
        return ""
    seen, paths = [], []
    for sr in srs:
        path = _sr_path(sr)
        if path and path not in seen:
            seen.append(path)
            paths.append(path)
    return "+".join(paths)

def payload_type_signature(env):
    """Extract a 'type' signature from the payload, whichever shape it's in:
      - payload is a dict with a top-level 'type' field, e.g. {"type": "PUT", ...}
      - payload is a list of dicts each carrying their own 'type', e.g. [{"type": "PICK"}, ...]
    Returns '' when no type info is present. Without this, two messages under
    the same name/state (e.g. two 'service-request-cancel' envelopes, one
    type PICK and one type PUT) would collapse into the same golden key and
    silently overwrite each other."""
    payload = env.get("payload")
    if isinstance(payload, dict):
        t = payload.get("type")
        return str(t).strip().upper() if t else ""
    if isinstance(payload, list):
        types = []
        for item in payload:
            if isinstance(item, dict):
                t = item.get("type")
                t = str(t).strip().upper() if t else ""
                if t and t not in types:
                    types.append(t)
        return "+".join(types)
    return ""

def topic_notif_key(env, label, topic):
    """Pairing key across setups: {LABEL}__{name}__{state}[__{type}][__{sr_type_path}].

    normalize_kowl_envelope() handles the double-wrap case (value.payload.payload
    itself being an ISD-style {notification_type, notification_data} envelope) —
    applied here too (not just in clean_topic_payload) so the key always matches
    what the stored/compared payload actually looks like.
    """
    env   = normalize_kowl_envelope(env)
    name  = env.get("name") or topic_short(topic)
    # Some callers pass the flat notification data itself as `env` (no
    # "payload" wrapper at all) — fall back to treating env as its own inner
    # data in that case.
    inner = env.get("payload") if isinstance(env.get("payload"), dict) else env
    state = (inner.get("state") or env.get("state")
             or inner.get("status") or env.get("status") or "all")
    key = f"{label}__{name}__{str(state).strip().lower()}"
    type_sig = payload_type_signature({"payload": inner})
    if type_sig:
        key += f"__{type_sig}"
    sr_path = sr_type_path({"payload": inner})
    if sr_path:
        key += f"__{sr_path}"
    return key

def as_kowl_envelope(o):
    """Detect & unwrap a raw Kowl message (or its value.payload envelope)
    pasted directly into 'paste as golden', so it can be keyed properly
    instead of falling through to the ISD-shape parser and degenerating to a
    generic 'NOTIFICATION' key. Accepts either the full Kowl message
    ({"value": {"payload": {...}}, "partitionID": ..., ...}) or just the inner
    envelope itself ({"name": ..., "payload": {...}}). Returns None if `o`
    doesn't look like either shape."""
    if not isinstance(o, dict):
        return None
    env = message_envelope(o)
    if isinstance(env, dict):
        return env
    if isinstance(o.get("name"), str) and isinstance(o.get("payload"), dict):
        return o
    return None

def _normalise_host(host):
    """Return (http_base_url, ws_url_base, is_tls) from a host string that may or may not have a scheme."""
    host = host.strip().rstrip("/")
    if host.startswith("https://"):
        return host, "wss://" + host[len("https://"):], True
    if host.startswith("http://"):
        return host, "ws://" + host[len("http://"):], False
    # bare host:port — assume plain HTTP/WS (Kowl default, no TLS)
    return "http://" + host, "ws://" + host, False


def fetch_topic_messages(host, topic, count=50, start_offset=-1,
                         idle_timeout=5, hard_timeout=20):
    """
    Consume up to `count` messages from a Kowl/Redpanda-Console topic via WebSocket.

    Kowl / Redpanda Console serves /api/topics/{topic}/messages as a WebSocket-only
    endpoint.  Params go in the URL query string; the server streams back JSON frames.

    start_offset: -1 = newest (recent N), -2 = oldest.
    Returns the raw message objects (with .value.payload).
    """
    if websocket is None:
        raise RuntimeError("websocket-client not installed. Run: pip install websocket-client")

    _, ws_base, is_tls = _normalise_host(host)

    # No query params — the server upgrades first, then waits for a JSON request
    # body sent as the first WebSocket message (ListMessagesRequest).
    ws_url = f"{ws_base}/api/topics/{topic}/messages"
    offset_num = -1 if start_offset == -1 else (-2 if start_offset == -2 else int(start_offset))
    req_body = {
        "topicName": topic,
        "startOffset": offset_num,
        "startTimestamp": 0,
        "partitionId": -1,
        "maxResults": int(count),
        "filterInterpreterCode": "",
    }

    return _fetch_topic_messages_ws(ws_url, ws_base, is_tls, count,
                                    idle_timeout, hard_timeout, req_body)


def _fetch_topic_messages_ws(ws_url, ws_base, is_tls, count,
                              idle_timeout, hard_timeout, req_body):
    """WebSocket transport — request sent as a JSON frame after connecting (Redpanda Console / Kowl)."""
    import ssl as _ssl
    bare_host = ws_base.split("://", 1)[-1]
    sslopt = {"cert_reqs": _ssl.CERT_NONE} if is_tls else {}
    ws = websocket.create_connection(
        ws_url,
        timeout=idle_timeout,
        sslopt=sslopt,
        header={"Origin": f"https://{bare_host}"},
    )
    ws.send(json.dumps(req_body))
    ws.settimeout(idle_timeout)
    msgs, start = [], time.time()
    # After receiving at least one message, a short silence means the burst is done.
    post_burst_timeout = 2  # seconds to wait after first message before giving up
    try:
        while time.time() - start < hard_timeout:
            # Shrink the socket timeout once we have messages — don't wait the full idle window
            if msgs:
                ws.settimeout(post_burst_timeout)
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                break  # silence after burst = done
            except websocket.WebSocketConnectionClosedException:
                break
            except Exception as _e:
                if msgs:
                    break
                raise RuntimeError(f"WebSocket error: {_e}") from _e
            if not raw:
                break
            o = json.loads(raw)
            t = o.get("type")
            if t == "message":
                msgs.append(o.get("message"))
                if len(msgs) >= count:
                    break
            elif t == "done":
                break
            elif t == "error":
                raise RuntimeError(o.get("message") or "Kowl returned an error")
    finally:
        try:
            ws.close()
        except Exception:
            pass
    return msgs

def message_envelope(msg):
    """Extract the notification payload envelope (value.payload) from a Kowl message."""
    if not msg or msg.get("isValueNull"):
        return None
    val = msg.get("value") or {}
    return val.get("payload")

def _kowl_label_path(key):
    """{configured_label}/{message_name} folder path, derived from a topic
    key's own first two '__'-separated segments. A single topic config entry
    (e.g. label 'Create/Cancel Request Message' on topic
    'service-request.requests') can carry multiple distinct message names
    (service-request-create, service-request-cancel, ...) — this keeps each
    one in its own subfolder instead of lumping them together under the
    shared label."""
    parts = key.split("__")
    return "/".join(parts[:2]) if len(parts) >= 2 else parts[0]

# Kowl baselines are golden data: stored under golden/{project}/kowl/{LABEL}/{NAME}/{key}.json
def topic_baseline_path(key):
    return golden_path(key, source="kowl", label=_kowl_label_path(key))

def save_topic_baseline(key, payload):
    save_golden(key, payload, source="kowl", label=_kowl_label_path(key))

def load_topic_baseline(key):
    p = topic_baseline_path(key)
    if p.exists():
        return json.loads(p.read_text())
    # fall back to the pre-split layout (one folder per label, not per label/name)
    # so baselines captured before this change still resolve
    legacy_nested = golden_path(key, source="kowl")
    if legacy_nested.exists():
        return json.loads(legacy_nested.read_text())
    # fallback to the legacy flat store for baselines captured before this change
    legacy = TOPIC_DIR / f"{key}.json"
    return json.loads(legacy.read_text()) if legacy.exists() else None

def list_topic_baselines():
    """Return the raw notif keys (topic_baseline_path/load_topic_baseline/
    save_topic_baseline all take this same raw key and internally re-derive
    the on-disk label/name subfolder from it via _kowl_label_path).

    This must NOT be the full relative filesystem path (folder/subfolder/key) —
    that used to be returned here, but feeding a path back into
    _kowl_label_path() (which expects a raw '__'-joined key, not a path with
    '/' folder separators) re-derives a bogus, wrong-nested folder and view/
    delete requests 404 even though the file exists right where it was saved.
    """
    root = golden_root(source="kowl") / "kowl"
    seen, keys = set(), []
    if root.exists():
        for p in root.rglob("*.json"):
            key = p.stem
            if key not in seen:
                seen.add(key); keys.append(key)
    for p in TOPIC_DIR.glob("*.json"):  # legacy flat store
        if p.stem not in seen:
            seen.add(p.stem); keys.append(p.stem)
    return sorted(keys)

def capture_topics(host, topics, count):
    """Fetch each topic and store one baseline file per derived key. Returns summary."""
    saved = {}
    errors = []
    for spec in topics:
        label, topic = spec["label"], spec["topic"]
        try:
            for msg in _fetch_with_timeout(host, topic, count):
                env = message_envelope(msg)
                if env is None:
                    continue
                key = topic_notif_key(env, label, topic)
                save_topic_baseline(key, clean_topic_payload(env))  # last message per key wins
                entry = saved.setdefault(key, {"key": key, "topic": topic, "count": 0})
                entry["count"] += 1
        except Exception as e:
            errors.append({"topic": topic, "error": str(e)})
    return list(saved.values()), errors

def capture_topics_thread(host, topics, count, state):
    """One-shot capture with per-topic progress pushed to state['log_queue']."""
    log = state["log_queue"]
    saved, errors = {}, []
    total = len(topics)
    for i, spec in enumerate(topics, 1):
        if not state.get("running", True):
            log.put({"type": "error", "msg": "Stopped by user."})
            state["running"] = False
            return
        label, topic = spec["label"], spec["topic"]
        log.put({"type": "progress", "topic": topic, "label": label,
                 "current": i, "total": total,
                 "msg": f"[{i}/{total}] Fetching: {topic} (up to {count} msgs, timeout 20s)…"})
        try:
            count_saved = 0
            for msg in _fetch_with_timeout(host, topic, count):
                env = message_envelope(msg)
                if env is None:
                    continue
                key = topic_notif_key(env, label, topic)
                save_topic_baseline(key, clean_topic_payload(env))
                entry = saved.setdefault(key, {"key": key, "topic": topic, "count": 0})
                entry["count"] += 1
                count_saved += 1
            log.put({"type": "ok", "topic": topic, "current": i, "total": total,
                     "msg": f"[{i}/{total}] ✅ {topic} — {count_saved} message(s)"})
        except Exception as e:
            errors.append({"topic": topic, "error": str(e)})
            log.put({"type": "topic_error", "topic": topic, "current": i, "total": total,
                     "msg": f"[{i}/{total}] ✗ {topic}: {e}"})
    log.put({"type": "done", "saved": list(saved.values()), "errors": errors,
             "keys": len(saved), "messages": sum(s["count"] for s in saved.values())})
    state["running"] = False

def kowl_capture_thread(host, interval):
    """Live-poll Kowl topics and save each new message as a kowl golden, until stopped.
    Mirrors the DB 'Live Poll & Capture' flow."""
    cfg    = load_config()
    count  = int(cfg.get("topic_count") or 50)
    # Baseline capture always prepends the configured env prefix (e.g. "aph.") to
    # each topic suffix — this live-poll variant was missing that step, so it
    # queried bare suffixes that don't exist on the cluster (UNKNOWN_TOPIC_OR_PARTITION).
    topics = apply_prefix(cfg.get("topics", []), (cfg.get("topic_prefix") or "").strip())
    log    = kowl_capture_state["log_queue"]
    if not host:
        log.put({"type": "error", "msg": "No Kowl host configured (Config tab)."})
        kowl_capture_state["running"] = False
        return
    if not topics:
        log.put({"type": "error", "msg": "No topics configured (Config tab)."})
        kowl_capture_state["running"] = False
        return
    seen = set()
    primed = False   # ignore messages already in the topic at Start; capture only new ones
    try:
        while kowl_capture_state["running"]:
            for spec in topics:
                if not kowl_capture_state["running"]:
                    break
                label, topic = spec["label"], spec["topic"]
                try:
                    for msg in _fetch_with_timeout(host, topic, count):
                        env = message_envelope(msg)
                        if env is None:
                            continue
                        rid = f"{topic}@p{msg.get('partitionID','?')}@{msg.get('offset','?')}"
                        if rid in seen:
                            continue
                        seen.add(rid)
                        if not primed:
                            continue   # pre-existing message at start — ignore
                        key = topic_notif_key(env, label, topic)
                        save_topic_baseline(key, clean_topic_payload(env))  # last msg per key wins
                        first = key not in kowl_capture_state["saved"]
                        kowl_capture_state["saved"][key] = True
                        log.put({"type": "saved" if first else "info",
                                 "msg": f"{'💾 NEW' if first else '↻ update'} {key}", "key": key})
                except Exception as e:
                    log.put({"type": "error", "msg": f"{topic}: {e}"})
            if not primed:
                primed = True
                log.put({"type": "info",
                         "msg": f"Capturing from Kowl {host} — {len(seen)} existing message(s) ignored. Trigger your flow now..."})
            time.sleep(interval)
        log.put({"type": "done", "msg": f"Capture stopped. {len(kowl_capture_state['saved'])} golden key(s) saved.",
                 "saved": sorted(kowl_capture_state["saved"].keys())})
    except Exception as e:
        log.put({"type": "error", "msg": f"Error: {e}"})
    finally:
        kowl_capture_state["running"] = False

def kowl_notification_data(env):
    """Extract a DB/ISD-style notification object from a Kowl envelope so it can be
    keyed with notif_key and compared against db/isd goldens."""
    if isinstance(env.get("notification_data"), dict):
        return env
    inner = env.get("payload")
    if isinstance(inner, dict):
        if isinstance(inner.get("notification_data"), dict):
            return inner
        return inner
    return env

def compare_kowl_env(env, label, topic, mode, golden_source, row_id):
    """Diff a single Kowl message envelope against the chosen golden source.
    golden_source='kowl' -> kowl baseline by topic key; 'db'/'isd' -> notif_key."""
    ext_id = env.get("entity_id") or ""
    if golden_source == "kowl":
        key     = topic_notif_key(env, label, topic)
        payload = clean_topic_payload(env)
        golden  = load_topic_baseline(key)
    else:  # 'isd' or 'db' — match by notification key
        nd      = kowl_notification_data(env)
        payload = clean_payload(nd)
        key     = notif_key(payload)
        # ISD goldens captured for the Kowl world live under the independent
        # kowl_project (see capture_isd_goldens' project_kind option) — this
        # compare is itself always Kowl-live-data-driven, so look there.
        project_kind = "kowl" if golden_source == "isd" else None
        golden  = load_golden(key, source=golden_source, project_kind=project_kind)
    base = {"db_id": row_id, "create_time": topic_short(topic), "key": key,
            "ext_id": ext_id, "flow": label}
    if golden is None:
        return {**base, "status": "NO GOLDEN", "findings": [], "payload": payload}
    diff = DeepDiff(golden, payload, ignore_order=True, verbose_level=2)
    findings = diff_to_list(diff, mode=mode)
    return {**base, "status": status_from_findings(findings),
            "findings": findings, "payload": payload}

_FETCH_TIMEOUT = 25  # seconds — outer wall-clock limit; must stay > fetch_topic_messages'
                     # own hard_timeout (20s default) so that inner timeout — which already
                     # returns whatever messages it collected — gets a chance to finish and
                     # hand back partial results, instead of this outer wrapper cutting it off
                     # first and discarding a fetch that was actually working, just slow.

def _fetch_with_timeout(host, topic, count):
    """Run fetch_topic_messages in a daemon thread; raise if it hangs past _FETCH_TIMEOUT."""
    ex = ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(fetch_topic_messages, host, topic, count)
    ex.shutdown(wait=False)  # don't block on exit — daemon thread will die with the process
    try:
        return fut.result(timeout=_FETCH_TIMEOUT)
    except FuturesTimeoutError:
        raise RuntimeError(f"No response from Kowl for '{topic}' within {_FETCH_TIMEOUT}s — "
                           f"the cluster may be slow or the topic may have no recent messages")

_STATUS_RANK = {"PASS": 0, "FAIL": 1, "ERROR": 2, "NO GOLDEN": 3}

def compare_topics(host, topics, count, mode="full", golden_source="kowl", state=None):
    """Fetch Kowl topics and diff each message against the chosen golden source.
    If state dict with log_queue is provided, emits per-topic progress events.

    When the same physical topic appears under multiple labels (e.g. 'service-request-cancel'
    listed twice with different label names), every label is tried for each message and only
    the best result per (partition, offset) is kept — PASS beats FAIL beats NO GOLDEN.
    This prevents duplicate rows when the topic config has overlapping entries.
    """
    # Group specs by physical topic name so we fetch each topic once and try all labels.
    from collections import defaultdict
    by_topic = defaultdict(list)
    for spec in topics:
        by_topic[spec["topic"]].append(spec["label"])

    results = []
    total   = len(by_topic)
    log     = state["log_queue"] if state else None

    for i, (topic, labels) in enumerate(by_topic.items(), 1):
        if state and not state.get("running", True):
            break  # user pressed Stop
        if log:
            log.put({"type": "progress", "topic": topic, "current": i, "total": total,
                     "msg": f"[{i}/{total}] Fetching: {topic} (up to {count} msgs, timeout 20s)…"})
        try:
            msgs = _fetch_with_timeout(host, topic, count)

            # best result per (partition, offset) across all labels
            best: dict = {}  # row_id -> result dict

            for msg in msgs:
                env = message_envelope(msg)
                if env is None:
                    continue
                row_id = f"p{msg.get('partitionID','?')}@{msg.get('offset','?')}"

                for label in labels:
                    try:
                        r = compare_kowl_env(env, label, topic, mode, golden_source, row_id)
                    except Exception as e:
                        r = {"db_id": row_id, "create_time": topic_short(topic),
                             "key": "ERROR", "ext_id": "", "flow": label, "status": "ERROR",
                             "findings": [{"type": "exception", "path": "", "detail": str(e)}]}
                    prev = best.get(row_id)
                    if prev is None or _STATUS_RANK.get(r["status"], 9) < _STATUS_RANK.get(prev["status"], 9):
                        best[row_id] = r

            topic_results = list(best.values())
            results.extend(topic_results)
            if log:
                passes = sum(1 for r in topic_results if r["status"] == "PASS")
                fails  = sum(1 for r in topic_results if r["status"] == "FAIL")
                log.put({"type": "ok", "topic": topic, "current": i, "total": total,
                         "msg": f"[{i}/{total}] ✅ {topic} — {len(topic_results)} msg(s), {passes} pass, {fails} fail"})
        except Exception as e:
            if log:
                log.put({"type": "topic_error", "topic": topic, "current": i, "total": total,
                         "msg": f"[{i}/{total}] ✗ {topic}: {e}"})

    # Multiple raw messages in the fetched window often resolve to the same
    # notification key (e.g. several PICK_LINE creates in a row) — collapse to
    # one row per (flow, key), same as the DB/Full Run compare, so the table
    # doesn't show N duplicate-looking rows for what is really one flow's result.
    results, skipped_repeats = dedupe_by_key(results)
    if log and skipped_repeats:
        log.put({"type": "ok", "msg": f"Collapsed {skipped_repeats} repeat-key message(s) into their first occurrence."})
    return results

def kowl_watch_loop(state, interval):
    """Live-poll Kowl topics and compare new messages against state['source'] golden.
    Shared by Watch and Full Run when the live data origin is Kowl."""
    cfg     = load_config()
    host    = (cfg.get("topic_host_b") or cfg.get("topic_host") or "").strip()
    count   = int(cfg.get("topic_count") or 50)
    prefix  = (cfg.get("topic_prefix_b") or "").strip()
    topics  = apply_prefix(cfg.get("topics", []), prefix)
    mode    = state.get("mode", "full")
    gsource = state.get("source", "kowl")
    log     = state["log_queue"]
    if not host:
        log.put({"type": "error", "msg": "No Kowl host configured (Config tab)."}); return
    if not topics:
        log.put({"type": "error", "msg": "No topics configured (Config tab)."}); return
    seen = set()
    # Dedup by (label, key): once a topic's notification key has been compared
    # once, later messages with the same key get the same schema verdict —
    # skip re-comparing/re-logging them instead of flooding the log.
    seen_keys = {}
    primed = False   # first sweep baselines existing messages; only newer ones are compared
    try:
        while state["running"]:
            for i, spec in enumerate(topics, 1):
                if not state["running"]:
                    break
                label, topic = spec["label"], spec["topic"]
                if primed:
                    log.put({"type": "scanning", "topic": topic, "current": i, "total": len(topics),
                             "msg": f"🔍 [{i}/{len(topics)}] Scanning: {topic}"})
                try:
                    for msg in _fetch_with_timeout(host, topic, count):
                        env = message_envelope(msg)
                        if env is None:
                            continue
                        rid = f"{topic}@p{msg.get('partitionID','?')}@{msg.get('offset','?')}"
                        if rid in seen:
                            continue
                        seen.add(rid)
                        if not primed:
                            continue   # pre-existing message at start — ignore
                        r = compare_kowl_env(env, label, topic, mode, gsource,
                                             f"p{msg.get('partitionID','?')}@{msg.get('offset','?')}")
                        dedup_key = (label, r["key"])
                        if dedup_key in seen_keys:
                            seen_keys[dedup_key] += 1
                            continue
                        seen_keys[dedup_key] = 1
                        state["results"].append(r)
                        icon = {"PASS": "✅", "FAIL": "❌", "NO GOLDEN": "⚠️", "ERROR": "🔥"}.get(r["status"], "?")
                        nfail = fail_count(r["findings"])
                        nwarn = len(r["findings"]) - nfail
                        counts = f"{nfail} diff(s)" + (f", {nwarn} warning(s)" if nwarn else "")
                        log.put({"type": r["status"].lower().replace(" ", "_"),
                                 "msg": f"{icon} {label} {r['key']} — {counts}",
                                 "result": r})
                except Exception as e:
                    log.put({"type": "error", "msg": f"✗ [{i}/{len(topics)}] {topic}: {e}"})
            if not primed:
                primed = True
                log.put({"type": "info",
                         "msg": f"Watching Kowl {host} vs {gsource} golden — {len(seen)} existing message(s) ignored. Trigger your flow now..."})
            time.sleep(interval)
    except Exception as e:
        log.put({"type": "error", "msg": f"Error: {e}"})

# ─── ISD GOLDEN CAPTURE (read PDF spec → golden) ──────────────────────────────
# The ISD (Interface Specification Document) is a PDF containing both a field
# spec and sample payloads. We extract every embedded JSON object as a candidate
# golden, derive its key, and store it under the current project.


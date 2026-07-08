"""SSH tunnel + Postgres access."""
import os

import psycopg2  # type: ignore[import]
import sshtunnel  # type: ignore[import]

from core.config import get_cfg

def open_tunnel(cfg=None, target=False):
    """target=False -> baseline host (where goldens are captured from).
    target=True  -> target host (live traffic compared against those goldens).
    ssh_user/ssh_port/ssh_key are shared across both."""
    cfg = cfg or get_cfg()
    ssh_host = (cfg.get("ssh_host_b") or cfg["ssh_host"]) if target else cfg["ssh_host"]
    db_host  = (cfg.get("db_host_b")  or cfg["db_host"])  if target else cfg["db_host"]
    t = sshtunnel.SSHTunnelForwarder(
        (ssh_host, int(cfg["ssh_port"])),
        ssh_username=cfg["ssh_user"],
        ssh_pkey=os.path.expanduser(cfg["ssh_key"]),
        remote_bind_address=(db_host, int(cfg["db_port"])),
    )
    t.start()
    return t

def connect_db(tunnel, cfg=None, target=False):
    cfg = cfg or get_cfg()
    db_pass = (cfg.get("db_pass_b") or cfg.get("db_pass")) if target else cfg.get("db_pass")
    conn = psycopg2.connect(
        host="127.0.0.1", port=tunnel.local_bind_port,
        dbname=cfg["db_name"], user=cfg["db_user"], password=db_pass,
        options="-c default_transaction_read_only=on",
    )
    return conn

def resolve_subscriber_ids(cursor, patterns):
    """Look up subscriber.id for the given subscriber.pattern value(s).

    We key everything off the pattern name now instead of a hardcoded
    subscriber id, since there are far more notification patterns than the
    old fixed PUT/PICK/AUDIT/OTHER categories could represent.
    Returns a list of matching ids (may be shorter than `patterns` if some
    pattern names don't exist in the subscriber table).
    """
    patterns = [p for p in (patterns or []) if p]
    if not patterns:
        return []
    placeholders = ",".join(["%s"] * len(patterns))
    cursor.execute(f"SELECT id FROM subscriber WHERE pattern IN ({placeholders})", patterns)
    return [r[0] for r in cursor.fetchall()]

def fetch_notifications(cursor, subscriber_ids, since=None, ext_id=None, limit=300):
    """
    subscriber_ids : int or list of ints
    since          : ISO datetime string — filter by create_time >= since
    ext_id         : externalServiceRequestId string — fetch only notifications for this flow run
    """
    cfg = get_cfg()
    if isinstance(subscriber_ids, int):
        subscriber_ids = [subscriber_ids]
    subscriber_ids = [s for s in subscriber_ids if s is not None]
    if not subscriber_ids:
        return []
    placeholders = ",".join(["%s"] * len(subscriber_ids))
    q = f"""SELECT id, create_time, status, status_code, subscriber_id, payload
            FROM {cfg['db_table']}
            WHERE subscriber_id IN ({placeholders})
            AND payload IS NOT NULL"""
    params = list(subscriber_ids)
    if ext_id:
        # externalServiceRequestId lives inside notification_data in the JSONB payload
        q += " AND payload->'notification_data'->>'externalServiceRequestId' = %s"
        params.append(ext_id)
    if since:
        q += " AND create_time >= %s"
        params.append(since)
    q += " ORDER BY id ASC LIMIT %s"
    params.append(limit)
    cursor.execute(q, params)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, r)) for r in cursor.fetchall()]


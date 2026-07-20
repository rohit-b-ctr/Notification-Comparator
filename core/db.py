"""SSH tunnel + Postgres access."""
import os
import socket

import psycopg2  # type: ignore[import]
import sshtunnel  # type: ignore[import]

from core.config import get_cfg

SSH_CONNECT_TIMEOUT = 6  # seconds

# Hardcoded rather than configurable — these never change across environments here.
SSH_PORT = 22
DB_PORT  = 5432
DB_USER  = "postgres"

def _check_reachable(host, port, timeout=SSH_CONNECT_TIMEOUT):
    """Fail fast with a clear error instead of the multi-minute OS-level hang
    sshtunnel is prone to: it opens the initial TCP connection from a plain
    (host, port) tuple, which paramiko/sshtunnel never applies a connect
    timeout to (only pre-made sockets/proxies get one) — so an unreachable
    gateway silently blocks for a long time before failing."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as e:
        raise RuntimeError(f"Cannot reach SSH host {host}:{port} — {e}. "
                           f"Check VPN/network connectivity.") from e

def open_tunnel(cfg=None, target=False):
    """target=False -> baseline host (where goldens are captured from).
    target=True  -> target host (live traffic compared against those goldens).
    ssh_user/ssh_key are shared across both; ssh_port/db_port are hardcoded (see above)."""
    cfg = cfg or get_cfg()
    ssh_host = (cfg.get("ssh_host_b") or cfg["ssh_host"]) if target else cfg["ssh_host"]
    db_host  = (cfg.get("db_host_b")  or cfg["db_host"])  if target else cfg["db_host"]
    _check_reachable(ssh_host, SSH_PORT)
    t = sshtunnel.SSHTunnelForwarder(
        (ssh_host, SSH_PORT),
        ssh_username=cfg["ssh_user"],
        ssh_pkey=os.path.expanduser(cfg["ssh_key"]),
        remote_bind_address=(db_host, DB_PORT),
    )
    t.start()
    return t

def connect_db(tunnel, cfg=None, target=False):
    cfg = cfg or get_cfg()
    db_pass = (cfg.get("db_pass_b") or cfg.get("db_pass")) if target else cfg.get("db_pass")
    conn = psycopg2.connect(
        host="127.0.0.1", port=tunnel.local_bind_port,
        dbname=cfg["db_name"], user=DB_USER, password=db_pass,
        options="-c default_transaction_read_only=on",
    )
    return conn

def db_now(cursor):
    """The DB server's own current timestamp, as a 'YYYY-MM-DD HH:MM:SS' string.

    Live polling filters rows with `create_time >= since`. If `since` were
    computed from this app's own clock (e.g. datetime.now(timezone.utc)) instead
    of the database's, any timezone mismatch between the app server and the
    `create_time` column (e.g. app in UTC, column stored in local/IST time)
    introduces a constant offset — rows that are actually old keep satisfying
    `>= since` on every fresh run until the app's real clock catches up to that
    offset, which looks like the exact same old notifications reappearing
    every time Watch/Full Run/Live Capture is restarted. Asking the DB for its
    own NOW() guarantees `since` is expressed in the same clock as the column
    it's compared against, regardless of what timezone either server is in.
    """
    cursor.execute("SELECT NOW()")
    return cursor.fetchone()[0].strftime("%Y-%m-%d %H:%M:%S")

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

def fetch_subscriber_details(cursor, patterns):
    """Fetch the full subscriber row(s) for the given pattern(s) — unlike
    resolve_subscriber_ids() (which only pulls subscriber.id), this snapshots
    the whole row so it can be diffed baseline vs target, catching drift in
    subscriber config (e.g. url, topic, active flag) beyond just the id.
    Returns a list of dicts, one per matching subscriber row.
    """
    patterns = [p for p in (patterns or []) if p]
    if not patterns:
        return []
    placeholders = ",".join(["%s"] * len(patterns))
    cursor.execute(f"SELECT * FROM subscriber WHERE pattern IN ({placeholders})", patterns)
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, r)) for r in cursor.fetchall()]

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


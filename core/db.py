"""SSH tunnel + Postgres access."""
import os

import psycopg2  # type: ignore[import]
import sshtunnel  # type: ignore[import]

from core.config import get_cfg

def open_tunnel(cfg=None):
    cfg = cfg or get_cfg()
    t = sshtunnel.SSHTunnelForwarder(
        (cfg["ssh_host"], int(cfg["ssh_port"])),
        ssh_username=cfg["ssh_user"],
        ssh_pkey=os.path.expanduser(cfg["ssh_key"]),
        remote_bind_address=(cfg["db_host"], int(cfg["db_port"])),
    )
    t.start()
    return t

def connect_db(tunnel, cfg=None):
    cfg = cfg or get_cfg()
    conn = psycopg2.connect(
        host="127.0.0.1", port=tunnel.local_bind_port,
        dbname=cfg["db_name"], user=cfg["db_user"], password=cfg["db_pass"],
        options="-c default_transaction_read_only=on",
    )
    return conn

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


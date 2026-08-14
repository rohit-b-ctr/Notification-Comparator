"""SSH tunnel + Postgres access — cloud (SSH key + tunnel) and on-prem
(SSH password + `sudo -u postgres psql`) modes, switched by cfg["access_mode"]."""
import os
import json
import shlex
import socket
import time
import uuid

import psycopg2  # type: ignore[import]
import sshtunnel  # type: ignore[import]
import paramiko  # type: ignore[import]

from core.config import get_cfg

SSH_CONNECT_TIMEOUT = 6  # seconds

# Hardcoded rather than configurable — these never change across environments here.
SSH_PORT = 22
DB_PORT  = 5432
DB_USER  = "postgres"

SSH_TUNNEL_RETRIES    = 5  # attempts before giving up on a flaky gateway
SSH_TUNNEL_RETRY_WAIT = 3  # seconds between attempts
REMOTE_CMD_TIMEOUT    = 30  # seconds to wait for a single remote psql command


def _check_reachable(host, port, timeout=SSH_CONNECT_TIMEOUT):
    """Fail fast with a clear error instead of the multi-minute OS-level hang
    sshtunnel/paramiko are prone to on an unreachable gateway."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as e:
        raise RuntimeError(f"Cannot reach SSH host {host}:{port} — {e}. "
                           f"Check VPN/network connectivity.") from e


# ─── CLOUD MODE: SSH key + tunnel + psycopg2 ──────────────────────────────────

def open_tunnel(cfg=None, target=False):
    """target=False -> baseline host (where goldens are captured from).
    target=True  -> target host (live traffic compared against those goldens).
    ssh_user/ssh_key are shared across both; ssh_port/db_port are hardcoded (see above).

    Retries up to SSH_TUNNEL_RETRIES times on failure (e.g. the gateway
    dropping the handshake under load) before giving up, since a single
    transient blip would otherwise fail the whole capture/compare/watch run.
    """
    cfg = cfg or get_cfg()
    ssh_host = (cfg.get("ssh_host_b") or cfg["ssh_host"]) if target else cfg["ssh_host"]
    db_host  = (cfg.get("db_host_b")  or cfg["db_host"])  if target else cfg["db_host"]

    last_err = None
    for attempt in range(1, SSH_TUNNEL_RETRIES + 1):
        try:
            _check_reachable(ssh_host, SSH_PORT)
            t = sshtunnel.SSHTunnelForwarder(
                (ssh_host, SSH_PORT),
                ssh_username=cfg["ssh_user"],
                ssh_pkey=os.path.expanduser(cfg["ssh_key"]),
                remote_bind_address=(db_host, DB_PORT),
            )
            t.start()
            return t
        except Exception as e:
            last_err = e
            if attempt < SSH_TUNNEL_RETRIES:
                time.sleep(SSH_TUNNEL_RETRY_WAIT)
    raise RuntimeError(
        f"Could not open SSH tunnel to {ssh_host} after {SSH_TUNNEL_RETRIES} attempts: {last_err}"
    ) from last_err

def connect_db(tunnel, cfg=None, target=False):
    cfg = cfg or get_cfg()
    db_pass = (cfg.get("db_pass_b") or cfg.get("db_pass")) if target else cfg.get("db_pass")
    conn = psycopg2.connect(
        host="127.0.0.1", port=tunnel.local_bind_port,
        dbname=cfg["db_name"], user=DB_USER, password=db_pass,
        options="-c default_transaction_read_only=on",
    )
    return conn


# ─── ON-PREM MODE: SSH password + remote `sudo -u postgres psql` ─────────────

def open_ssh(cfg=None, target=False):
    """SSH login via username + password (no key) — matches the manual
    `ssh <user>@<host>` + password-prompt flow on hosts that have no
    network/password login to Postgres itself. Baseline and target commonly
    use different SSH passwords (e.g. separate local/prod credentials), so
    ssh_pass_b is tried first when target=True and falls back to the
    baseline ssh_pass only if it's blank."""
    cfg = cfg or get_cfg()
    ssh_host = (cfg.get("ssh_host_b") or cfg["ssh_host"]) if target else cfg["ssh_host"]
    ssh_pass = (cfg.get("ssh_pass_b") or cfg.get("ssh_pass")) if target else cfg.get("ssh_pass")

    last_err = None
    for attempt in range(1, SSH_TUNNEL_RETRIES + 1):
        try:
            _check_reachable(ssh_host, SSH_PORT)
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                ssh_host, port=SSH_PORT,
                username=cfg["ssh_user"], password=ssh_pass,
                timeout=SSH_CONNECT_TIMEOUT, allow_agent=False, look_for_keys=False,
            )
            return client
        except Exception as e:
            last_err = e
            if attempt < SSH_TUNNEL_RETRIES:
                time.sleep(SSH_TUNNEL_RETRY_WAIT)
    raise RuntimeError(
        f"Could not SSH to {ssh_host} after {SSH_TUNNEL_RETRIES} attempts: {last_err}"
    ) from last_err

def _run_remote(client, cmd, sudo_password=None, timeout=REMOTE_CMD_TIMEOUT):
    """Run `cmd` over the SSH session, feeding a sudo password if prompted
    (some hosts require one even for `sudo -u postgres ...`)."""
    chan = client.get_transport().open_session()
    chan.get_pty()
    chan.exec_command(cmd)

    out, err = b"", b""
    sent_pw = False
    start = time.time()
    while True:
        if chan.recv_ready():
            chunk = chan.recv(4096)
            out += chunk
            if not sent_pw and sudo_password and b"password" in chunk.lower():
                chan.send(sudo_password.encode() + b"\n")
                sent_pw = True
        if chan.recv_stderr_ready():
            err += chan.recv_stderr(4096)
        if chan.exit_status_ready():
            break
        if time.time() - start > timeout:
            chan.close()
            raise RuntimeError(f"Remote command timed out after {timeout}s: {cmd}")
        time.sleep(0.05)

    exit_code = chan.recv_exit_status()
    while chan.recv_ready():
        out += chan.recv(4096)
    while chan.recv_stderr_ready():
        err += chan.recv_stderr(4096)
    return exit_code, out.decode(errors="replace"), err.decode(errors="replace")

def _pg_literal(value):
    """Format a Python value as a safe SQL literal. There's no psycopg2-style
    parameterization once we're going through a shell command, so strings are
    single-quoted with embedded quotes doubled, and numbers inserted bare."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"

def run_remote_psql(client, db_name, query, sudo_password=None):
    """Run a single SELECT via `sudo -u postgres psql` and return the results
    as a list of dicts. Wraps the query in row_to_json(t) and writes it to a
    remote temp file (via SFTP) rather than inlining on the command line, to
    avoid shell-quoting issues and because JSON-per-row is far safer to parse
    back out than delimiter-split text (the `payload` column is JSONB and can
    contain almost any character)."""
    remote_path = f"/tmp/.dbq_{uuid.uuid4().hex}.sql"
    wrapped = f"SELECT row_to_json(t) FROM ({query}) t;"

    sftp = client.open_sftp()
    try:
        with sftp.file(remote_path, "w") as f:
            f.write(wrapped)
    finally:
        sftp.close()

    try:
        # -P pager=off / --no-psqlrc: the pty we allocate in _run_remote (so an
        # optional sudo password prompt can be answered) also makes psql think
        # it's talking to a real terminal, so by default it pipes any longer
        # result set through `less` to page it. With nothing there to press
        # q/scroll, that just hangs until the remote-command timeout fires —
        # this is what "Remote command timed out" almost always means here,
        # not an actual connectivity problem. Forcing the pager off (and
        # skipping .psqlrc, in case it re-enables one) avoids that regardless
        # of whether a pty is attached.
        remote_cmd = (f'sudo -u {DB_USER} psql -d {db_name} -t -A -q '
                     f'-P pager=off --no-psqlrc -f {remote_path}')
        # Sentinels: bash -lc (a login shell) can print MOTD/welcome-banner
        # text on its own before our command even runs — this only shows up
        # once a pty is allocated, which we need for the optional sudo-password
        # prompt. That banner text lands in the same stdout stream as psql's
        # JSON rows and breaks naive line-by-line json.loads() (surfacing as
        # a cryptic "Unterminated string" error). Wrapping the real command in
        # unique markers lets us slice out exactly the psql output and ignore
        # anything the shell printed before/after it.
        start_tag = f"__PSQL_START_{uuid.uuid4().hex}__"
        end_tag   = f"__PSQL_END_{uuid.uuid4().hex}__"
        full_cmd  = f"echo {start_tag}; {remote_cmd}; echo {end_tag}"
        cmd = f"bash -lc {shlex.quote(full_cmd)}"
        exit_code, out, err = _run_remote(client, cmd, sudo_password=sudo_password)
        if exit_code != 0:
            raise RuntimeError(f"psql failed (exit {exit_code}): {err.strip() or out.strip()}")
        if start_tag in out and end_tag in out:
            out = out.split(start_tag, 1)[1].split(end_tag, 1)[0]
        rows = []
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # stray non-JSON output (banner remnants, notices) — skip, don't fail the whole query
        return rows
    finally:
        cleanup_cmd = f"bash -lc {shlex.quote(f'rm -f {remote_path}')}"
        _run_remote(client, cleanup_cmd, sudo_password=sudo_password)


# ─── UNIFIED HANDLE — used by core/routes.py regardless of access_mode ───────

def open_connection(cfg=None, target=False):
    """Returns a handle dict tagging which access mode is active. Every
    higher-level function below (run_query, db_now, resolve_subscriber_ids,
    etc.) branches on handle["mode"] internally, so callers don't need to
    know or care which mode is configured."""
    cfg = cfg or get_cfg()
    mode = cfg.get("access_mode", "cloud")

    if mode == "onprem":
        client = open_ssh(cfg, target)
        sudo_password = (cfg.get("sudo_pass_b") or cfg.get("sudo_pass")) if target else cfg.get("sudo_pass")
        return {"mode": "onprem", "cfg": cfg, "client": client,
                "db_name": cfg["db_name"], "sudo_password": sudo_password or None}
    else:
        tunnel = open_tunnel(cfg, target)
        conn = connect_db(tunnel, cfg, target)
        return {"mode": "cloud", "cfg": cfg, "tunnel": tunnel, "conn": conn}

def close_connection(handle):
    if handle["mode"] == "onprem":
        handle["client"].close()
    else:
        handle["conn"].close()
        handle["tunnel"].stop()

def run_query(handle, query, params=None):
    """params (list) are only meaningful in cloud mode, where psycopg2 does
    real server-side parameterization with %s placeholders. In onprem mode,
    build the full query with _pg_literal()-escaped values before calling
    this (see resolve_subscriber_ids etc. below) — params is ignored there."""
    if handle["mode"] == "onprem":
        return run_remote_psql(handle["client"], handle["db_name"], query,
                               sudo_password=handle.get("sudo_password"))
    else:
        cur = handle["conn"].cursor()
        cur.execute(query, params or [])
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows

def db_now(handle):
    """The DB server's own current timestamp, as 'YYYY-MM-DD HH:MM:SS'.

    Live polling filters rows with `create_time >= since`. If `since` were
    computed from this app's own clock instead of the database's, any
    timezone mismatch between the app server and the `create_time` column
    introduces a constant offset — rows that are actually old keep satisfying
    `>= since` on every fresh run, which looks like the same old notifications
    reappearing every time Watch/Full Run/Live Capture is restarted. Asking
    the DB for its own NOW() guarantees both sides use the same clock."""
    rows = run_query(handle, "SELECT NOW() AS now")
    now = rows[0]["now"]
    if handle["mode"] == "onprem":
        return now[:19].replace("T", " ")  # ISO string from row_to_json
    return now.strftime("%Y-%m-%d %H:%M:%S")  # datetime object from psycopg2

def resolve_subscriber_ids(handle, patterns):
    """Look up subscriber.id for the given subscriber.pattern value(s)."""
    patterns = [p for p in (patterns or []) if p]
    if not patterns:
        return []
    if handle["mode"] == "onprem":
        in_list = ",".join(_pg_literal(p) for p in patterns)
        rows = run_query(handle, f"SELECT id FROM subscriber WHERE pattern IN ({in_list})")
    else:
        placeholders = ",".join(["%s"] * len(patterns))
        rows = run_query(handle, f"SELECT id FROM subscriber WHERE pattern IN ({placeholders})", patterns)
    return [r["id"] for r in rows]

def fetch_subscriber_details(handle, patterns):
    """Fetch the full subscriber row(s) for the given pattern(s) — for
    diffing baseline vs target beyond just the id."""
    patterns = [p for p in (patterns or []) if p]
    if not patterns:
        return []
    if handle["mode"] == "onprem":
        in_list = ",".join(_pg_literal(p) for p in patterns)
        return run_query(handle, f"SELECT * FROM subscriber WHERE pattern IN ({in_list}) ORDER BY id")
    else:
        placeholders = ",".join(["%s"] * len(patterns))
        return run_query(handle, f"SELECT * FROM subscriber WHERE pattern IN ({placeholders}) ORDER BY id", patterns)

def fetch_all_subscribers(handle):
    """Fetch every row from the subscriber table."""
    return run_query(handle, "SELECT * FROM subscriber")

def fetch_notifications(handle, subscriber_ids, since=None, ext_id=None, limit=300):
    """
    subscriber_ids : int or list of ints
    since          : ISO datetime string — filter by create_time >= since
    ext_id         : externalServiceRequestId string — fetch only notifications for this flow run
    """
    cfg = handle["cfg"]
    if isinstance(subscriber_ids, int):
        subscriber_ids = [subscriber_ids]
    subscriber_ids = [s for s in subscriber_ids if s is not None]
    if not subscriber_ids:
        return []

    most_recent = not since and not ext_id  # "the last N" -> newest first, restore order below

    if handle["mode"] == "onprem":
        id_list = ",".join(str(int(s)) for s in subscriber_ids)
        q = f"""SELECT id, create_time, status, status_code, subscriber_id, payload
                FROM {cfg['db_table']}
                WHERE subscriber_id IN ({id_list})
                AND payload IS NOT NULL"""
        if ext_id:
            q += f" AND payload->'notification_data'->>'externalServiceRequestId' = {_pg_literal(ext_id)}"
        if since:
            q += f" AND create_time >= {_pg_literal(since)}"
        q += f" ORDER BY id {'DESC' if most_recent else 'ASC'} LIMIT {int(limit)}"
        rows = run_query(handle, q)
    else:
        placeholders = ",".join(["%s"] * len(subscriber_ids))
        q = f"""SELECT id, create_time, status, status_code, subscriber_id, payload
                FROM {cfg['db_table']}
                WHERE subscriber_id IN ({placeholders})
                AND payload IS NOT NULL"""
        params = list(subscriber_ids)
        if ext_id:
            q += " AND payload->'notification_data'->>'externalServiceRequestId' = %s"
            params.append(ext_id)
        if since:
            q += " AND create_time >= %s"
            params.append(since)
        q += f" ORDER BY id {'DESC' if most_recent else 'ASC'} LIMIT %s"
        params.append(limit)
        rows = run_query(handle, q, params)

    if most_recent:
        rows.reverse()
    return rows
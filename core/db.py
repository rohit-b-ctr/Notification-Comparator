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
SOCKET_TIMEOUT        = 30  # raw-socket timeout on the SSH transport — see open_ssh()


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
        # Same reasoning as the onprem SSH keepalive above — surface a
        # connection that died silently (e.g. laptop sleep) within seconds
        # instead of leaving a long-running poll loop hung on it.
        keepalives=1, keepalives_idle=15, keepalives_interval=5, keepalives_count=3,
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
            # Without this, a connection that dies silently (laptop sleep,
            # NAT/firewall dropping an idle mapping — no RST ever sent) isn't
            # noticed by the OS for a long time, so a long-running Watch/Full
            # Run poll loop can hang for many minutes on the next remote
            # command before anything errors out. A periodic SSH-level
            # keepalive surfaces a dead connection within one or two
            # intervals instead.
            transport = client.get_transport()
            transport.set_keepalive(15)
            # Belt and suspenders beyond the keepalive above: a timeout on
            # the *raw socket* bounds every paramiko operation uniformly —
            # channel opens, the keepalive round-trip itself, everything —
            # at the lowest possible level. Without this, something like
            # open_session() (called on every reconnect attempt, before any
            # higher-level chan.settimeout() can apply) has no bound of its
            # own and can hang indefinitely if the connection is dead but
            # paramiko's own keepalive thread hasn't managed to notice yet
            # (it relies on the very same socket). Paramiko's transport read
            # loop already handles periodic socket.timeout internally as
            # part of normal operation, so this is safe to set broadly.
            transport.sock.settimeout(SOCKET_TIMEOUT)
            return client
        except Exception as e:
            last_err = e
            if attempt < SSH_TUNNEL_RETRIES:
                time.sleep(SSH_TUNNEL_RETRY_WAIT)
    raise RuntimeError(
        f"Could not SSH to {ssh_host} after {SSH_TUNNEL_RETRIES} attempts: {last_err}"
    ) from last_err

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

def open_psql_session(client, db_name, sudo_password=None, timeout=REMOTE_CMD_TIMEOUT):
    """Start one long-lived `sudo -u postgres psql` process and keep it
    running for the life of a DB handle, instead of spawning a brand-new
    login shell + sudo + psql process (a fresh Postgres backend included)
    for every single query. Watch/Full Run poll every few seconds — that
    per-query spawn overhead can occasionally push past the remote-command
    timeout under any host load and silently stall the whole run, which a
    persistent process sidesteps entirely.

    No pty is allocated: sudo reads its password from stdin via `-S`
    instead of prompting on a tty. That's specifically so the channel's
    stdin/stdout behave like plain pipes with no local echo — with a pty,
    every query we send would echo straight back into the same stream we're
    parsing results from, and reliably telling that echo apart from the
    real result (both contain the exact same marker text) turned out to
    depend on timing assumptions about the pty that can't be verified
    against a real target from here. A wrong guess there risks silently
    parsing echoed input as if it were a query result; getting *this*
    assumption wrong instead fails loudly — some sudoers configs
    (`Defaults requiretty`) refuse to run at all without a tty, and that
    shows up immediately as a clear "sorry, you must have a tty" error, not
    corrupted data.

    Still run through `bash -lc` (a login shell), not exec'd directly — the
    old per-query code always did, and a login shell sources
    /etc/profile, ~/.bash_profile etc., which can set environment variables
    (TZ in particular) that change how psql's session interprets
    timestamps. Skipping that once here reproduced exactly the kind of
    constant, timezone-offset drift db_now() exists to prevent — the same
    "old" rows matching `create_time >= since` on every fresh run,
    regardless of how much real time had passed.
    """
    chan = client.get_transport().open_session()
    chan.settimeout(timeout)
    cmd = f"sudo -S -u {DB_USER} psql -d {db_name} -t -A -q -P pager=off --no-psqlrc"
    chan.exec_command(f"bash -lc {shlex.quote(cmd)}")
    if sudo_password:
        # Harmless if sudo doesn't actually need it (e.g. NOPASSWD) — at
        # this point nothing but sudo itself has started reading stdin yet.
        chan.send(sudo_password.encode() + b"\n")
    session = {"chan": chan, "buffer": b""}
    try:
        _psql_session_query(session, "SELECT 1", timeout=timeout)
    except Exception as e:
        chan.close()
        raise RuntimeError(f"Failed to start persistent psql session: {e}") from e
    return session

def close_psql_session(session):
    if not session:
        return
    try:
        session["chan"].close()
    except Exception:
        pass

def _psql_session_query(session, query, timeout=REMOTE_CMD_TIMEOUT):
    """Run one query against an already-running psql session (see
    open_psql_session) and return its rows as row_to_json dicts. Reuses the
    session's leftover byte buffer across calls, since a channel read can
    span past this query's own output into the start of the next one."""
    chan = session["chan"]
    start_tag = f"__PSQL_START_{uuid.uuid4().hex}__"
    end_tag   = f"__PSQL_END_{uuid.uuid4().hex}__"
    wrapped = f"SELECT row_to_json(t) FROM ({query}) t;"
    chan.send(f"SELECT '{start_tag}';\n{wrapped}\nSELECT '{end_tag}';\n".encode())

    buf = session["buffer"]
    stderr_buf = b""
    start = time.time()
    out = None
    while out is None:
        if start_tag.encode() in buf:
            after = buf.split(start_tag.encode(), 1)[1]
            if end_tag.encode() in after:
                out, remainder = after.split(end_tag.encode(), 1)
                session["buffer"] = remainder
                break
        if chan.exit_status_ready():
            raise RuntimeError(f"psql session ended unexpectedly: {stderr_buf.decode(errors='replace').strip()}")
        # Checked every iteration, not just when nothing arrived — a slow
        # query that trickles out the odd byte (a NOTICE, partial output)
        # without ever actually finishing would otherwise keep resetting
        # the effective deadline forever, since `got_data` being true kept
        # this check from ever running at all. The cap is on total time to
        # get a *complete* result, not on gaps between reads.
        if time.time() - start > timeout:
            raise RuntimeError(f"Persistent psql query timed out after {timeout}s: {stderr_buf.decode(errors='replace').strip()}")
        got_data = False
        if chan.recv_ready():
            buf += chan.recv(65536)
            got_data = True
        if chan.recv_stderr_ready():
            stderr_buf += chan.recv_stderr(4096)
            got_data = True
        if not got_data:
            time.sleep(0.02)

    rows = []
    for line in out.decode(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # stray non-JSON output (NOTICE lines, etc.) — skip, don't fail the whole query
    return rows


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
        # One psql process, reused for every query made through this handle
        # — see open_psql_session() for why (a fresh sudo+psql spawn per
        # query is real overhead that Watch/Full Run's every-few-seconds
        # polling is especially exposed to).
        try:
            psql_session = open_psql_session(client, cfg["db_name"], sudo_password=sudo_password)
        except Exception:
            client.close()
            raise
        return {"mode": "onprem", "cfg": cfg, "client": client,
                "db_name": cfg["db_name"], "sudo_password": sudo_password or None,
                "psql_session": psql_session}
    else:
        tunnel = open_tunnel(cfg, target)
        conn = connect_db(tunnel, cfg, target)
        return {"mode": "cloud", "cfg": cfg, "tunnel": tunnel, "conn": conn}

def close_connection(handle):
    if handle["mode"] == "onprem":
        close_psql_session(handle.get("psql_session"))
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
        try:
            return _psql_session_query(handle["psql_session"], query)
        except Exception:
            # The persistent psql process itself died (crashed, OOM-killed,
            # etc.) — every query on this handle shares that one process
            # now, so without this a single hiccup would abort an entire
            # multi-pattern Capture/Compare run instead of just costing one
            # reconnect. The underlying SSH client is left alone; only the
            # psql child process is replaced.
            close_psql_session(handle.get("psql_session"))
            handle["psql_session"] = open_psql_session(
                handle["client"], handle["db_name"], sudo_password=handle.get("sudo_password"))
            return _psql_session_query(handle["psql_session"], query)
    else:
        cur = handle["conn"].cursor()
        cur.execute(query, params or [])
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows

def db_now(handle):
    """The DB server's own current timestamp, as 'YYYY-MM-DD HH:MM:SS' — in
    UTC, to match how `create_time` is stored (a naive/no-timezone column).

    Live polling filters rows with `create_time >= since`. If `since` were
    computed from this app's own clock instead of the database's, any
    timezone mismatch between the app server and the `create_time` column
    introduces a constant offset — rows that are actually old keep satisfying
    `>= since` on every fresh run, which looks like the same old notifications
    reappearing every time Watch/Full Run/Live Capture is restarted. Asking
    the DB for its own NOW() guarantees both sides use the same clock — but
    plain NOW() reflects whatever timezone the current session happens to be
    in (observed as -07:00 on one onprem host, for example, rather than the
    UTC `create_time` is actually stored in), reintroducing exactly that
    offset. Converting explicitly with AT TIME ZONE 'UTC' removes the
    ambient session timezone from the equation entirely."""
    rows = run_query(handle, "SELECT NOW() AT TIME ZONE 'UTC' AS now")
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

def fetch_all_subscribers(handle):
    """Fetch every row from the subscriber table."""
    return run_query(handle, "SELECT * FROM subscriber")

def fetch_notifications(handle, subscriber_ids, since=None, ext_id=None, limit=300, prefer_recent=False):
    """
    subscriber_ids : int or list of ints
    since          : ISO datetime string — filter by create_time >= since
    ext_id         : externalServiceRequestId string — fetch only notifications for this flow run
    prefer_recent  : fetch the newest `limit` rows in the window instead of the oldest.
                     Only matters when `since` covers more rows than `limit` — normally
                     that's fine (Compare/Watch care about not skipping anything near
                     the start of the window), but Capture Golden wants "what does this
                     flow look like right now", so a wide/old `since` shouldn't silently
                     cap it to a batch of ancient rows and never reach today's data at all.
    """
    cfg = handle["cfg"]
    if isinstance(subscriber_ids, int):
        subscriber_ids = [subscriber_ids]
    subscriber_ids = [s for s in subscriber_ids if s is not None]
    if not subscriber_ids:
        return []

    most_recent = prefer_recent or (not since and not ext_id)  # "the last N" -> newest first, restore order below
    # A pattern commonly resolves to several subscriber_ids (fan-out routing
    # rules). A flat `LIMIT` over the combined IN-list lets one very busy
    # subscriber crowd out a quieter sibling's rows entirely — its distinct
    # notification shapes would then never get fetched (and so never
    # captured) no matter how large the limit is. `prefer_recent` partitions
    # the limit per subscriber_id instead of sharing one pool across all of
    # them, so every fan-out subscriber gets its own fair `limit` rows.
    per_subscriber = prefer_recent and len(subscriber_ids) > 1

    if handle["mode"] == "onprem":
        id_list = ",".join(str(int(s)) for s in subscriber_ids)
        base = f"""SELECT id, create_time, status, status_code, subscriber_id, payload
                FROM {cfg['db_table']}
                WHERE subscriber_id IN ({id_list})
                AND payload IS NOT NULL"""
        if ext_id:
            base += f" AND payload->'notification_data'->>'externalServiceRequestId' = {_pg_literal(ext_id)}"
        if since:
            base += f" AND create_time >= {_pg_literal(since)}"
        if per_subscriber:
            q = f"""SELECT id, create_time, status, status_code, subscriber_id, payload FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY subscriber_id ORDER BY id DESC) AS rn
                        FROM ({base}) t
                    ) ranked WHERE rn <= {int(limit)} ORDER BY id DESC"""
        else:
            q = base + f" ORDER BY id {'DESC' if most_recent else 'ASC'} LIMIT {int(limit)}"
        rows = run_query(handle, q)
    else:
        placeholders = ",".join(["%s"] * len(subscriber_ids))
        base = f"""SELECT id, create_time, status, status_code, subscriber_id, payload
                FROM {cfg['db_table']}
                WHERE subscriber_id IN ({placeholders})
                AND payload IS NOT NULL"""
        params = list(subscriber_ids)
        if ext_id:
            base += " AND payload->'notification_data'->>'externalServiceRequestId' = %s"
            params.append(ext_id)
        if since:
            base += " AND create_time >= %s"
            params.append(since)
        if per_subscriber:
            q = f"""SELECT id, create_time, status, status_code, subscriber_id, payload FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY subscriber_id ORDER BY id DESC) AS rn
                        FROM ({base}) t
                    ) ranked WHERE rn <= %s ORDER BY id DESC"""
            params.append(limit)
        else:
            q = base + f" ORDER BY id {'DESC' if most_recent else 'ASC'} LIMIT %s"
            params.append(limit)
        rows = run_query(handle, q, params)

    if most_recent:
        rows.reverse()
    return rows
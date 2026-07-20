"""Configuration, secrets, and on-disk paths."""
import os
import json
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

# Project root = the directory that contains app.py (one level above this package).
# All on-disk data (config.json, golden/, reports/, allure-results/) lives here.
BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG_PATH = BASE_DIR / "config.json"

# Fields that are NEVER written to disk in plaintext — only their _enc
# counterpart (see SECRET_ENC_FIELDS below) is persisted, in config.json.
# ssh_key is just a local file *path*, not a secret payload, so it's a normal
# persisted field (see DEFAULT_CONFIG) — only the two DB passwords are encrypted.
SECRET_FIELDS = {"db_pass", "db_pass_b"}

# plaintext field name -> the encrypted-blob field name stored in config.json
SECRET_ENC_FIELDS = {f: f"{f}_enc" for f in SECRET_FIELDS}

# Local-only key that encrypts secrets at rest in config.json. config.json
# itself may be committed/shared; this file must never leave the machine —
# see .gitignore. Without it the *_enc blobs in config.json are just noise.
KEY_PATH = BASE_DIR / ".config_key"

def _get_fernet():
    if KEY_PATH.exists():
        key = KEY_PATH.read_bytes()
    else:
        key = Fernet.generate_key()
        KEY_PATH.write_bytes(key)
        try:
            os.chmod(KEY_PATH, 0o600)
        except OSError:
            pass
    return Fernet(key)

def encrypt_secret(plain):
    if not plain:
        return ""
    return _get_fernet().encrypt(plain.encode()).decode()

def decrypt_secret(token):
    if not token:
        return ""
    try:
        return _get_fernet().decrypt(token.encode()).decode()
    except (InvalidToken, ValueError):
        return ""

DEFAULT_CONFIG = {
    # Baseline: where goldens are captured from.
    "ssh_host": "172.29.32.137",
    "db_host":  "10.57.117.201",
    # Target: what live traffic is compared against those goldens.
    "ssh_host_b": "172.29.32.137",
    "db_host_b":  "10.57.117.201",
    # Shared across baseline + target. ssh_port (22) / db_port (5432) / db_user
    # ("postgres") are not configurable — hardcoded in core/db.py since they
    # never change here.
    "ssh_user": "rohit_b_ctr_greyorange_com",
    "ssh_key":  "",  # path to private key file — not a secret payload, persisted plainly
    "db_name":  "wms_notification",
    "db_table": "subscriber_history",
    # Notification patterns: each maps a human label to a `subscriber.pattern`
    # value; the app looks up subscriber.id for that pattern, then queries
    # db_table by subscriber_id. Add as many as you need — no fixed categories.
    "patterns": [],
    "poll_interval": 3,
    # ── Golden categorization ──
    "project": "",                       # DB/ISD project — golden saved under golden/{project}/...
    "kowl_project": "",                  # Kowl project — independent of "project", golden saved under golden/{kowl_project}/kowl/...
    # ── Topic Compare (Kowl / Kafka UI) ──
    "topic_host":     "172.29.32.39:9003",  # baseline Kowl host:port
    "topic_host_b":   "172.29.32.39:9003",  # target Kowl host:port
    "topic_prefix":   "",                   # baseline env prefix (e.g. "aph")
    "topic_prefix_b": "",                   # target env prefix  (e.g. "stpfunction-apotekreg")
    "topic_count": 50,                      # recent N messages to pull per topic
    "topics": [
        {"label": "PUT",  "topic": "stpfunction-sbscloud.put_information.events"},
        {"label": "SR",   "topic": "stpfunction-sbscloud.service-request-update.events"},
        {"label": "PICK", "topic": "stpfunction-sbscloud.order_information.events"},
    ],
}

# Field groupings for the split DB / Kowl config export & import.
DB_CONFIG_FIELDS = [
    "ssh_host", "ssh_host_b", "ssh_user", "ssh_key",
    "db_host", "db_host_b", "db_name", "db_table",
    "patterns", "poll_interval", "project",
]
# Encrypted DB password blobs — included in DB config export/import (still
# ciphertext, only decryptable on a machine holding the same .config_key) but
# never part of the general load_config()/GET /api/config contract.
DB_CONFIG_ENC_FIELDS = list(SECRET_ENC_FIELDS.values())
KOWL_CONFIG_FIELDS = [
    "topic_host", "topic_host_b", "topic_prefix", "topic_prefix_b", "topic_count", "topics", "kowl_project",
]

# In-memory only — never persisted to disk
RUNTIME_SECRETS = {
    "db_pass":   "",  # baseline DB password
    "db_pass_b": "",  # target DB password
}

def parse_int(value, default=None):
    """int() that tolerates None/'' (blank form fields) and returns default instead of crashing."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def load_config():
    if CONFIG_PATH.exists():
        try:
            saved = json.loads(CONFIG_PATH.read_text())
            # strip plaintext secrets (older versions) and encrypted blobs alike —
            # callers of load_config() (incl. the GET /api/config the browser sees)
            # must never receive either form. Encrypted blobs are decrypted
            # separately, straight into RUNTIME_SECRETS — see load_saved_secrets().
            for f in SECRET_FIELDS:
                saved.pop(f, None)
            for f in SECRET_ENC_FIELDS.values():
                saved.pop(f, None)
            # ssh_port/db_port/db_user used to be configurable — now hardcoded in
            # core/db.py, so drop any stale value still sitting in an older config.json.
            saved.pop("ssh_port", None)
            saved.pop("db_port", None)
            saved.pop("db_user", None)
            # strip whitespace from all string values (including inside topics list)
            def _strip(v):
                if isinstance(v, str): return v.strip()
                if isinstance(v, dict): return {k2: _strip(v2) for k2, v2 in v.items()}
                if isinstance(v, list): return [_strip(i) for i in v]
                return v
            saved = {k: _strip(v) for k, v in saved.items()}
            return {**DEFAULT_CONFIG, **saved}
        except Exception:
            pass
    return dict(DEFAULT_CONFIG)

def save_config(data):
    # Never write plaintext secrets to disk — only *_enc blobs (see
    # save_secrets_to_disk) are allowed to persist. "secrets_ready" is a
    # computed flag the GET /api/config response adds for the UI — never
    # persist it (round-tripping a fetched config back through save_config
    # would otherwise write it into config.json as a stale literal).
    safe = {k: v for k, v in data.items() if k not in SECRET_FIELDS and k != "secrets_ready"}
    CONFIG_PATH.write_text(json.dumps(safe, indent=2))

def get_cfg():
    """Return merged config — disk config + in-memory secrets."""
    cfg = load_config()
    cfg.update(RUNTIME_SECRETS)
    return cfg

def secrets_ready():
    return bool(RUNTIME_SECRETS.get("db_pass")) and bool(load_config().get("ssh_key"))

# Legacy standalone secrets file — no longer written to, only read once for
# migration (see _migrate_legacy_secrets_file below).
SECRETS_PATH = BASE_DIR / ".secrets"

def load_saved_secrets():
    """Auto-load secrets on startup by decrypting the *_enc blobs stored
    directly in config.json (see save_secrets_to_disk)."""
    if not CONFIG_PATH.exists():
        return False
    try:
        raw = json.loads(CONFIG_PATH.read_text())
    except Exception:
        return False
    found = False
    for plain_field, enc_field in SECRET_ENC_FIELDS.items():
        token = raw.get(enc_field)
        if not token:
            continue
        value = decrypt_secret(token)
        if value:
            RUNTIME_SECRETS[plain_field] = value
            found = True
    return found

def save_secrets_to_disk(db_pass, db_pass_b=""):
    """Encrypt and persist the DB passwords as *_enc fields inside config.json
    itself — no separate secrets file. Blank fields keep whatever was already
    saved. (ssh_key isn't handled here — it's a plain persisted field, saved
    the normal way through save_config()/the main Save button.)"""
    current = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else dict(DEFAULT_CONFIG)
    if db_pass:
        current[SECRET_ENC_FIELDS["db_pass"]] = encrypt_secret(db_pass)
    if db_pass_b:
        current[SECRET_ENC_FIELDS["db_pass_b"]] = encrypt_secret(db_pass_b)
    save_config(current)

def clear_saved_secrets():
    """Remove any encrypted secret blobs from config.json (and RUNTIME_SECRETS)."""
    if CONFIG_PATH.exists():
        try:
            current = json.loads(CONFIG_PATH.read_text())
            for enc_field in SECRET_ENC_FIELDS.values():
                current.pop(enc_field, None)
            save_config(current)
        except Exception:
            pass
    for plain_field in SECRET_ENC_FIELDS:
        RUNTIME_SECRETS[plain_field] = ""
    if SECRETS_PATH.exists():
        SECRETS_PATH.unlink()

def _migrate_legacy_secrets_file():
    """One-time migration: fold an old standalone .secrets file into the
    encrypted config.json fields, then remove it."""
    if not SECRETS_PATH.exists():
        return
    try:
        data = json.loads(SECRETS_PATH.read_text())
        save_secrets_to_disk(data.get("db_pass", ""), data.get("db_pass_b", ""))
        ssh_key = data.get("ssh_key", "")
        if ssh_key:
            current = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else dict(DEFAULT_CONFIG)
            current["ssh_key"] = ssh_key
            save_config(current)
    except Exception:
        pass
    finally:
        SECRETS_PATH.unlink(missing_ok=True)

def _migrate_ssh_key_to_plaintext():
    """One-time migration: an earlier version of this app encrypted ssh_key
    too (as ssh_key_enc). It's just a local file path, not a secret payload —
    decrypt it once into a plain "ssh_key" field and drop the _enc blob."""
    if not CONFIG_PATH.exists():
        return
    try:
        current = json.loads(CONFIG_PATH.read_text())
    except Exception:
        return
    token = current.pop("ssh_key_enc", None)
    if token is None:
        return
    if not current.get("ssh_key"):
        value = decrypt_secret(token)
        if value:
            current["ssh_key"] = value
    save_config(current)

# Migrate legacy secret storage, then auto-load secrets at startup
_migrate_legacy_secrets_file()
_migrate_ssh_key_to_plaintext()
_secrets_auto_loaded = load_saved_secrets()

GOLDEN_DIR  = BASE_DIR / "golden"
REPORTS_DIR = BASE_DIR / "reports"
TOPIC_DIR   = BASE_DIR / "topic_baseline"   # legacy store (read-only fallback)
GOLDEN_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)
# Kowl baselines now live under golden/{project}/kowl/... — TOPIC_DIR is no longer created,
# only read as a fallback for any pre-migration baselines that might still exist.


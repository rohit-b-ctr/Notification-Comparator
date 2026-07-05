"""Configuration, secrets, and on-disk paths."""
import os
import json
from pathlib import Path

# Project root = the directory that contains app.py (one level above this package).
# All on-disk data (config.json, golden/, reports/, .secrets, allure-results/) lives here.
BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG_PATH = BASE_DIR / "config.json"

# Fields that are NEVER written to disk — must be entered in UI each session
SECRET_FIELDS = {"db_pass", "ssh_key"}

DEFAULT_CONFIG = {
    "ssh_host": "172.29.32.137",
    "ssh_port": 22,
    "ssh_user": "rohit_b_ctr_greyorange_com",
    "db_host":  "10.57.117.201",
    "db_port":  5432,
    "db_name":  "wms_notification",
    "db_user":  "postgres",
    "db_table": "subscriber_history",
    "subscriber_put":   None,
    "subscriber_pick":  None,
    "subscriber_audit": None,
    "subscriber_other": None,
    "poll_interval": 3,
    # ── Golden categorization ──
    "project": "",                       # current project — golden saved under golden/{project}/...
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

# In-memory only — never persisted to disk
RUNTIME_SECRETS = {
    "db_pass": "",
    "ssh_key": "",
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
            # strip any secrets that may have been saved by older versions
            for f in SECRET_FIELDS:
                saved.pop(f, None)
            # strip whitespace from all string values
            saved = {k: v.strip() if isinstance(v, str) else v for k, v in saved.items()}
            return {**DEFAULT_CONFIG, **saved}
        except Exception:
            pass
    return dict(DEFAULT_CONFIG)

def save_config(data):
    # Never write secrets to disk
    safe = {k: v for k, v in data.items() if k not in SECRET_FIELDS}
    CONFIG_PATH.write_text(json.dumps(safe, indent=2))

def get_cfg():
    """Return merged config — disk config + in-memory secrets."""
    cfg = load_config()
    cfg.update(RUNTIME_SECRETS)
    return cfg

def secrets_ready():
    return bool(RUNTIME_SECRETS.get("db_pass")) and bool(RUNTIME_SECRETS.get("ssh_key"))

SECRETS_PATH = BASE_DIR / ".secrets"

def load_saved_secrets():
    """Auto-load secrets from .secrets file on startup if it exists."""
    if SECRETS_PATH.exists():
        try:
            data = json.loads(SECRETS_PATH.read_text())
            if data.get("db_pass"):
                RUNTIME_SECRETS["db_pass"] = data["db_pass"]
            if data.get("ssh_key"):
                RUNTIME_SECRETS["ssh_key"] = data["ssh_key"]
            return True
        except Exception:
            pass
    return False

def save_secrets_to_disk(db_pass, ssh_key):
    SECRETS_PATH.write_text(json.dumps({"db_pass": db_pass, "ssh_key": ssh_key}))

def clear_saved_secrets():
    if SECRETS_PATH.exists():
        SECRETS_PATH.unlink()

# Auto-load secrets at startup
_secrets_auto_loaded = load_saved_secrets()

GOLDEN_DIR  = BASE_DIR / "golden"
REPORTS_DIR = BASE_DIR / "reports"
TOPIC_DIR   = BASE_DIR / "topic_baseline"   # legacy store (read-only fallback)
GOLDEN_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)
# Kowl baselines now live under golden/{project}/kowl/... — TOPIC_DIR is no longer created,
# only read as a fallback for any pre-migration baselines that might still exist.


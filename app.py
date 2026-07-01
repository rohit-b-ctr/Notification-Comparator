#!/usr/bin/env python3
"""Notification Comparator — Flask Web UI (entrypoint).

This file just wires the app together; all logic lives in the `core` package
and the UI lives in `static/` (index.html, app.css, app.js).

Run: python app.py   →   http://localhost:5050
"""
try:
    from flask import Flask  # type: ignore[import]
    import psycopg2          # type: ignore[import]  # noqa: F401
    import sshtunnel         # type: ignore[import]  # noqa: F401
    from deepdiff import DeepDiff  # type: ignore[import]  # noqa: F401
except ImportError:
    print("Missing dependencies. Run:")
    print("  pip install -r requirements.txt")
    raise

from core.config import load_saved_secrets
from core.routes import bp

# Auto-load any persisted secrets on startup (DB password + SSH key path).
load_saved_secrets()

app = Flask(__name__, static_folder="static", static_url_path="/static")
app.register_blueprint(bp)

if __name__ == "__main__":
    print("🔔 Notification Comparator UI")
    print("   → http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False, threaded=True)

# WMS Notification Comparator

A Flask web application that automates WMS notification validation. It captures Kafka/Kowl topic messages and Postgres DB notification payloads as **golden snapshots**, then compares any future run against those goldens — surfacing schema regressions and payload drift through a browser UI, HTML reports, and Allure test reports.

---

## Features

- **DB Capture & Compare** — poll `subscriber_history` over an SSH tunnel, capture golden snapshots per notification type, then diff any new run against them
- **Live Watch** — real-time DB polling triggered by you; compare notifications as they land without knowing timestamps in advance
- **Full Run** — automated end-to-end: watch, capture, and compare in a single flow with Allure report generation
- **Kafka / Kowl Topic Capture & Compare** — pull messages from Kowl REST API, save per-topic baselines, and compare target-env topics against them
- **ISD PDF Extraction** — extract structured JSON from ISD PDF documents and promote them directly to golden snapshots
- **Direct JSON / XML Compare** — paste two payloads and diff them instantly, no DB or Kafka required
- **Allure Reports** — every Full Run emits a downloadable `allure-results.zip`; if the Allure CLI is installed, the HTML report is generated in-app automatically
- **HTML Reports** — lightweight per-run HTML diff reports stored in `reports/`
- **Dynamic field exclusion** — noisy fields (`id`, `createdOn`, timestamps, etc.) are stripped before diffing to eliminate false positives
- **Multi-project support** — switch between named project configs (e.g. `Apotek_Prod`) from the Config tab
- **Secrets never written to disk by default** — DB password and SSH key path are memory-only unless you explicitly save them via the UI

---

## Requirements

- Python 3.9+
- Network access to the SSH jump host and Kowl UI
- Java Runtime Environment (JRE) — only if you want in-app Allure HTML report generation

---

## Installation

### 1. Clone / download the repository

```bash
git clone <repo-url>
cd Comparator
```

### 2. (Recommended) Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

> **Note on pinned versions:** `paramiko` is pinned to `2.12.0` because newer versions changed the SSH key handling API used by `sshtunnel`. Do not upgrade it without testing the tunnel first.

### 4. (Optional) Install Allure CLI for in-app HTML reports

```bash
brew install allure        # macOS — pulls a JRE automatically
# OR
npm install -g allure-commandline   # requires Java on PATH
```

Without the Allure CLI, Full Run still produces a downloadable `allure-results.zip` that you can open anywhere Allure is installed:

```bash
unzip allure_<timestamp>.zip -d allure-results
allure serve allure-results
```

---

## Configuration

### Non-secret settings — `config.json`

These are safe to commit and are editable either directly in the file or via the **Config** tab in the UI.

| Key | Description |
|-----|-------------|
| `ssh_host` | IP / hostname of the SSH jump host |
| `ssh_port` | SSH port (default `22`) |
| `ssh_user` | SSH username |
| `db_host` | Postgres host (reachable from the jump host) |
| `db_port` | Postgres port (default `5432`) |
| `db_name` | Database name |
| `db_user` | Database user |
| `db_table` | Notification table (default `subscriber_history`) |
| `subscriber_put` | Subscriber ID for Putaway notifications |
| `subscriber_pick` | Subscriber ID for Pick notifications |
| `subscriber_audit` | Subscriber ID for Audit notifications (null to skip) |
| `subscriber_other` | Subscriber ID for any other notification type (null to skip) |
| `poll_interval` | Live-watch DB poll interval in seconds |
| `project` | Active project name (used in report filenames) |
| `topic_host` | Kowl base URL for the **source** environment |
| `topic_host_b` | Kowl base URL for the **target** environment |
| `topic_prefix` | Kafka topic prefix for the source environment |
| `topic_prefix_b` | Kafka topic prefix for the target environment |
| `topic_count` | Number of latest messages to fetch per topic |
| `topics` | List of `{label, topic}` objects — the topics displayed in the Kafka tab |

### Secret settings — entered in the UI each session

These are **never written to `config.json`**. Enter them on the **Config** tab after starting the app:

| Secret | Description |
|--------|-------------|
| DB Password | Password for `db_user` |
| SSH Key Path | Absolute path to your private key, e.g. `~/.ssh/rohit_b_ctr_greyorange_com` |

The UI offers a **Save secrets** option that writes them to a local `.secrets` file so they survive app restarts. Use this only on a personal machine — never on a shared or CI server.

---

## Running the App

```bash
python app.py
```

Open **http://localhost:5050** in your browser.

The app binds to `0.0.0.0:5050` so it is reachable from other machines on the same network (useful when running on a dev server and accessing from a laptop).

---

## Usage Guide

### Config Tab

1. Enter your **DB Password** and **SSH Key Path**.
2. Click **Test Connection** to verify the SSH tunnel + DB are reachable.
3. Click **Test Kowl** to verify the Kowl REST API is reachable.
4. Adjust any non-secret settings and click **Save Config**.

### DB Notifications — Capture Golden Baseline

1. Run a known-good WMS flow (Putaway, Pick, Audit, etc.) in your source environment.
2. Go to the **Capture** tab.
3. Set the **Since** timestamp to just before the flow started.
4. Click **Capture**. The app fetches rows from `subscriber_history`, groups them by `(flow_type, state, notification_type)`, and saves one `.json` file per group under `golden/`.

Golden files are named:
```
golden/{project}/{FLOW_TYPE}__{state}__{notification_type}.json
```
For example: `golden/Apotek_Prod/PICK__created__order_information.json`

### DB Notifications — Compare a New Run

1. Trigger the same flow in your target environment.
2. Go to the **Compare** tab.
3. Set the **Since** timestamp.
4. Click **Compare**. Each notification payload is diffed against its golden counterpart. Results appear as a pass/fail table with expandable diff rows showing exact field-level changes.

### Live Watch

Use this when you do not know the exact start time of the flow in advance.

1. Go to the **Watch** tab and click **Start Watch**.
2. Trigger your WMS flow.
3. The app polls the DB in real time (every `poll_interval` seconds) and streams results to the browser as they arrive.
4. Click **Stop Watch** when done.

### Full Run (Automated)

Combines Watch + Compare into a single timed session and generates Allure + HTML reports.

1. Go to the **Full Run** tab.
2. Click **Start**. The app begins watching.
3. Trigger your flow.
4. Click **Stop**. The app finalises the comparison and generates reports.
5. Download the `allure-results.zip` from the **Reports** tab, or view the in-app Allure HTML report if the CLI is installed.

### Kafka / Kowl Topics

1. Go to the **Topics** tab.
2. Select one or more topics from the list.
3. Click **Capture Baseline** to snapshot the latest N messages from the **source** Kowl host.
4. Later, click **Compare** to fetch the same topics from the **target** Kowl host and diff them against the baseline.

### ISD PDF Extraction

Use this to create a golden snapshot from an ISD specification PDF rather than a live capture.

1. Go to **ISD Extract**.
2. Upload the ISD PDF.
3. The app extracts structured JSON from the document.
4. Review the extracted payload, then click **Promote to Golden** to save it as a golden file.

### Direct JSON / XML Compare

No DB, no Kafka — just paste two payloads and compare.

1. Go to **JSON Compare** or **XML Compare**.
2. Paste the **expected** payload on the left and the **actual** payload on the right.
3. Click **Compare**. Differences are highlighted inline.

### Reports Tab

- Lists all generated HTML diff reports and Allure runs.
- Download any report as a ZIP or open the Allure HTML viewer in-browser.
- Delete individual reports or bulk-clear old runs.

---

## Project Structure

```
Comparator/
├── app.py                      # Entrypoint — starts Flask on :5050
├── config.json                 # Non-secret configuration (committed)
├── requirements.txt            # Python dependencies
├── core/
│   ├── routes.py               # All Flask API endpoints
│   ├── config.py               # Config load/save + secrets management
│   ├── db.py                   # SSH tunnel + Postgres queries
│   ├── golden.py               # Golden snapshot read/write
│   ├── diffing.py              # DeepDiff wrapper + dynamic field exclusion
│   ├── live.py                 # Live watch / Full Run logic
│   ├── kowl.py                 # Kowl REST API client
│   ├── isd.py                  # ISD PDF extraction (PyMuPDF)
│   ├── allure.py               # Allure results generation
│   ├── reports.py              # HTML report generation
│   └── state.py                # In-memory run state
├── static/
│   ├── index.html              # Single-page UI
│   ├── app.js                  # Frontend logic
│   └── app.css                 # Styles
├── golden/                     # Golden snapshot JSON files (auto-created)
└── reports/                    # HTML + Allure reports (auto-created)
```

---

## Dynamic Fields — Ignored During Comparison

The following fields are stripped from both the golden and actual payloads before diffing to avoid noise from run-specific identifiers and timestamps:

```
id, eventdata_id, notification_id, execution_id,
createdOn, updatedOn, receivedOn, create_time,
externalServiceRequestId, sr_parent, sr_parentsIds
```

To add or remove fields from this list, edit `IGNORED_FIELDS` in `core/diffing.py`.

---

## Troubleshooting

### "SSH connection refused" or tunnel timeout
- Confirm VPN is active and the jump host (`ssh_host`) is reachable: `ping <ssh_host>`
- Verify the SSH key path is correct and the key has the right permissions: `chmod 600 ~/.ssh/<key>`
- Try connecting manually: `ssh -i ~/.ssh/<key> <ssh_user>@<ssh_host>`

### "Authentication failed" for DB
- Double-check the DB password entered on the Config tab.
- Confirm `db_user` and `db_name` match the target environment.

### Kowl test fails
- Check that `topic_host` / `topic_host_b` are reachable from this machine (e.g. `curl http://<topic_host>/api/topics`).
- The app accepts plain `host:port`, `http://host:port`, or `https://host:port` — no trailing slash needed.
- Some Kowl deployments are behind a reverse proxy or require VPN — verify the URL manually in a browser first.

### Golden files not found during Compare
- Run **Capture** first with a known-good flow before comparing.
- Check that the `project` setting in Config matches the subdirectory name under `golden/`.

### Allure report not generated in-app
- Ensure `allure` is on your `PATH`: `allure --version`
- Install it via `brew install allure` (macOS) or the npm package (needs Java).
- If Allure is missing, the app falls back to producing a downloadable ZIP — this is expected behaviour, not an error.

### `ImportError` on startup
- Activate your virtual environment before running: `source .venv/bin/activate`
- Re-run `pip install -r requirements.txt`

### Port 5050 already in use
```bash
lsof -i :5050          # find the process
kill -9 <PID>          # stop it
python app.py          # restart
```

---

## Screenshots

> _Add screenshots here after the demo. Suggested captures:_
> - Config tab with connection test passing
> - Compare results table with a failing diff expanded
> - Full Run stream view mid-execution
> - Allure HTML report summary page
> - Topics capture and compare side-by-side

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `flask` | Web framework |
| `psycopg2-binary` | Postgres driver |
| `sshtunnel` | SSH tunnel to reach DB |
| `paramiko==2.12.0` | SSH transport (pinned — see note above) |
| `deepdiff` | Structured JSON diffing |
| `jinja2` | HTML report templating |
| `websocket-client` | WebSocket connection to Kowl for topic message streaming |
| `requests` | Kowl REST API calls (topic list, connection test) |
| `PyMuPDF` | ISD PDF text extraction |
| `xmltodict` | XML-to-dict parsing for XML compare |

---

## License

Internal tool — GreyOrange. Not for external distribution.

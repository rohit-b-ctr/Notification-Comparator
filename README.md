# WMS Notification Comparator

A Flask web application that automates WMS notification validation. It captures Kafka/Kowl topic messages and Postgres DB notification payloads as **golden snapshots**, then compares any future run against those goldens — surfacing schema regressions and payload drift through a browser UI, HTML reports, and Allure test reports.

---

## Features

- **DB Capture & Compare** — poll `subscriber_history` over an SSH tunnel, capture golden snapshots per configured notification **pattern**, then diff any new run against them
- **Pattern-based subscriber lookup** — notifications are selected by `subscriber.pattern` (a human-readable name you configure), not a hardcoded subscriber ID; supports any number of patterns, and multiple patterns can be captured/watched at once
- **Separate baseline/target DB connections** — capture goldens from one environment (baseline) and compare live traffic from a different one (target); SSH port/user/key and DB name/user/table are shared
- **Live Watch** — real-time DB polling triggered by you; compare notifications as they land without knowing timestamps in advance
- **Full Run** — automated end-to-end: watch, capture, and compare in a single flow with Allure report generation
- **Kafka / Kowl Topic Capture & Compare** — stream messages from Kowl's WebSocket API, save per-topic baselines, and compare target-env topics against them; repeat messages that resolve to the same notification key are automatically collapsed to one row instead of flooding the results table
- **ISD PDF Extraction & Paste-as-Golden** — extract structured JSON straight from ISD PDF documents, or paste a payload (an ISD-style notification, a flat notification body, or a raw Kowl message envelope) directly. Either way, each payload is auto-detected and filed as a real `db` or `kowl` golden — the same buckets a live DB/Kowl capture uses, so it's found by the ordinary Compare tab with no separate "ISD" comparison mode. Gap-fill only: an ISD/paste capture never overwrites a golden a real live capture already produced for that key
- **Subscriber Config Compare** — snapshot **every** row in the `subscriber` table from the baseline env (not limited to the patterns typed into the Config tab), then diff each pattern's target-env row against its snapshot. Patterns that exist in the baseline but have no subscriber at all in the target are called out as their own **MISSING IN TARGET** status with the baseline's details shown directly, instead of being lumped into a generic failure. Patterns present on both sides get a full side-by-side (baseline vs. target) field table — matching fields included, not just diffs — with drift on volatile fields (timestamps, IPs, request IDs) surfaced as a non-failing warning rather than a hard failure. Reports for this land in their own collapsible "Subscriber Compare Reports" section on the Dashboard, separate from the main Past Reports list
- **Direct JSON / XML / Text Compare** — paste two payloads and diff them instantly, no DB or Kafka required. JSON and XML are diffed structurally (schema/value aware); plain text is diffed line-by-line for unstructured content (logs, request bodies, config files) that doesn't parse as either
- **Allure Reports** — every Full Run emits a downloadable `allure-results.zip`; if the Allure CLI is installed, the HTML report is generated in-app automatically
- **HTML Reports** — lightweight per-run HTML diff reports stored in `reports/`
- **Dynamic field exclusion** — noisy fields (`id`, `createdOn`, timestamps, etc.) are stripped before diffing to eliminate false positives
- **Independent DB and Kowl projects** — the DB project (`project`) and the Kowl project (`kowl_project`) are separate values, each with their own `golden/{project}/...` folder — so DB and Kowl baselines never collide even when captured under different names
- **Split config export/import** — download/restore DB-side config (`db_config-*.json`) and Kowl-side config (`kowl_config-*.json`) independently from their respective Config cards, as a portable backup or to move settings to another machine
- **Encrypted secrets at rest** — DB passwords (baseline + target) are encrypted with `cryptography`'s Fernet and stored directly in `config.json` (as `db_pass_enc` / `db_pass_b_enc`); the encryption key lives in a local-only `.config_key` file that's never committed. SSH key path is just a file path, not a secret, so it's stored in plain text

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

Everything below lives in `config.json`, editable either directly in the file or via the **Config** tab in the UI. `config.json` is **gitignored** — it holds host/IP details plus encrypted DB password blobs, so it should never be committed (see [Secrets](#secrets-in-configjson) below).

### DB / SSH fields

| Key | Description |
|-----|-------------|
| `ssh_host` | SSH jump host for the **baseline** environment (where goldens are captured from) |
| `ssh_host_b` | SSH jump host for the **target** environment (what's compared against the goldens) |
| `ssh_user` | SSH username — shared by baseline and target |
| `ssh_key` | Path to your private key, e.g. `~/.ssh/id_rsa` — shared by baseline and target. Just a file path, not a secret payload, so it's stored in plain text |
| `db_host` | Postgres host for the **baseline** environment (reachable from `ssh_host`) |
| `db_host_b` | Postgres host for the **target** environment (reachable from `ssh_host_b`) |
| `db_table` | Notification table (default `subscriber_history`) |
| `patterns` | List of `{label, pattern}` objects. `pattern` must match a `subscriber.pattern` value in the DB; the app resolves it to `subscriber.id` and queries `db_table` by that ID. Add as many as you need — no fixed PUT/PICK/AUDIT categories |
| `poll_interval` | Live-watch DB poll interval in seconds |
| `project` | DB project name — golden data saved under `golden/{project}/db/...` and `golden/{project}/subscriber/...` |
| `db_pass_enc` / `db_pass_b_enc` | Encrypted DB passwords (see [Secrets](#secrets-in-configjson)) |

> `ssh_port` (22), `db_port` (5432), and `db_user` (`postgres`) are **not configurable** — hardcoded in `core/db.py` since they never change across environments here.

Exportable/importable as one unit from the DB/SSH card's **⬇ Export DB Config** / **⬆ Import DB Config** buttons (`db_config-{project}.json`), including the encrypted password blobs.

Golden snapshots are captured under a **pattern's own label folder**, not by whatever internal `type` field happens to be in the payload — so a pattern like `service-request-cancel-success` that happens to carry `type: PUT` internally still files under its own label, never mixed into a generic `PUT` folder.

### Kowl / Kafka fields

| Key | Description |
|-----|-------------|
| `topic_host` | Kowl base URL for the **baseline** environment |
| `topic_host_b` | Kowl base URL for the **target** environment |
| `topic_prefix` | Kafka topic prefix for the baseline environment |
| `topic_prefix_b` | Kafka topic prefix for the target environment |
| `topic_count` | Number of latest messages to fetch per topic |
| `topics` | List of `{label, topic}` objects — the topics displayed in the Kafka tab |
| `kowl_project` | Kowl's own project name — **independent of `project`** above. Kowl golden data is saved under `golden/{kowl_project}/kowl/...`, a completely separate folder from the DB project |

Exportable/importable as one unit from the Kowl card's **⬇ Export Kowl Config** / **⬆ Import Kowl Config** buttons (`kowl_config-{kowl_project}.json`).

> **Why two separate project names?** DB captures and Kowl topic captures are often run against different environments at different times. Keeping `project` and `kowl_project` independent means renaming/switching one never silently moves or hides the other's golden data. An ISD/paste capture auto-detects which of the two it belongs to and files under the matching project.

### Secrets in `config.json`

DB passwords (baseline + target) are the only true secrets — they're encrypted with `cryptography`'s Fernet cipher and stored directly in `config.json` as `db_pass_enc` / `db_pass_b_enc`. The encryption key lives in a **local-only `.config_key` file** (auto-generated on first use, gitignored, mode `600`) — without it, those blobs are unreadable noise, so `config.json` itself is safe to move around even though it contains them (as long as `.config_key` doesn't travel with it).

Enter DB passwords on the **Config** tab; they're picked up automatically the next time you click **Save** — no separate "save secrets" step. `ssh_key` is not treated as a secret (see above) since it's just a local file path.

There is no more standalone `.secrets` file — an older version of this app used one; if you're upgrading from that version, it's migrated into the encrypted `config.json` fields automatically on first run and then deleted.

---

## Running the App

```bash
python app.py
```

Open **http://localhost:5050** in your browser.

The app binds to `0.0.0.0:5050` so it is reachable from other machines on the same network (useful when running on a dev server and accessing from a laptop).

---

## Usage Guide

The app has six tabs in the left nav: **Dashboard**, **Capture Golden**, **Compare**, **Watch (Live)**, **Golden Snapshots**, **Config**. Full Run and Past Reports live on the Dashboard, not as separate tabs.

### Config Tab

**DB/SSH card:**
1. Set a **Current project** name (required — golden data is grouped under it) and add your notification **patterns**, one per line as `Label = pattern`, where `pattern` matches a `subscriber.pattern` value in the DB (e.g. `PUT_Success = service-request-cancel-success`).
2. Enter your **SSH Key** path and **DB Password** (baseline, and target if different) — click the 👁 button to reveal a password field while typing.
3. Click **💾 Save** — this persists everything (including encrypting and saving the DB passwords) in one action.
4. Click **🔌 Test Baseline** / **🔌 Test Target** to verify each SSH tunnel + DB is reachable.
5. Use **⬇ Export DB Config** / **⬆ Import DB Config** to back up or restore this card's settings as a portable file.

**Kowl card:** same flow, but with its own **Kowl Project** field (independent of the DB project above) and **🔌 Test Baseline** / **🔌 Test Target** buttons that check the Kowl host's reachability instead of Postgres.

### Capture Golden Tab

Four sources, picked via the pills at the top: **🗄 From DB**, **🧬 From Kowl**, **📄 From ISD**, **👤 Subscriber**.

**🗄 From DB** — capture from a live baseline run:
1. Run a known-good WMS flow in your **baseline** environment.
2. Check one or more configured patterns (pulled straight from Config), or use **⚡ Live Poll & Capture** to capture as the flow runs.
3. Set the **Since** timestamp to just before the flow started (or leave blank for live poll).
4. Click **Capture**. The app resolves each checked pattern to its `subscriber.id`, fetches matching rows from `subscriber_history` on the **baseline** DB, and saves one `.json` golden per distinct `(type, state, status)` key under `golden/{project}/db/{pattern_label}/`.

For example, with project `Columbus`, capturing pattern `PUT_Success` saves to:
```
golden/Columbus/db/PUT_Success/PUT__created__success.json
```

**🧬 From Kowl** — capture per-topic baselines from the **baseline** Kowl host: **📥 Capture Baseline (snapshot)** pulls the latest N messages for every configured topic once; **⚡ Live Capture** polls continuously until you click Stop, saving one golden per notification key as new ones appear.

**📄 From ISD** — extract goldens from an ISD spec instead of a live capture:
1. Pick **Save under: 🗄 DB Project / 🧬 Kowl Project** once at the top — it applies to both methods below.
2. Either upload an ISD PDF (auto-extracts every recognized notification payload), or paste one/more payloads directly into the **➕ Paste payload(s) as golden** box — a plain ISD payload, a flat notification body, or a raw Kowl message envelope all work; unparseable PDF blocks can be loaded straight into the paste box to fix and retry.
3. Each payload is auto-detected and saved as a real `db` or `kowl` golden (whichever it actually is) — there is no separate "ISD" golden bucket, so it shows up under the ordinary **Compare** tab like any live capture would. This fills gaps only: if a golden already exists for that exact key, the ISD/paste capture is silently skipped rather than overwriting it.

**👤 Subscriber** — snapshot **every** row in the `subscriber` table from the **baseline** env — not just patterns typed into the Config tab — one file per pattern under `golden/{project}/subscriber/`. Config-tab labels are only used to name the file nicer when a pattern happens to be configured; any other pattern is filed under its raw pattern name.

### Compare Tab

Pick a golden source pill, then run the compare:

- **🗄 DB** — enter/select a pattern, set **Since** (or an External Request ID), click **🔍 Compare**. Queries the **target** DB and diffs each notification against its golden. Results are a pass/fail table with expandable diff rows.
- **🧬 Kowl** — streams the same topics from the **target** Kowl host and diffs them against the stored Kowl baseline. Messages that resolve to the same notification key are automatically collapsed to one row.
- **🧩 Direct JSON** / **📰 Direct XML** — no DB or Kafka involved: paste the **expected** payload and the **actual** payload, click Compare, differences are highlighted inline. A ✨ Beautify button on each side pretty-prints and validates the payload before comparing.
- **📝 Direct Text** — for content that isn't JSON/XML (logs, raw request bodies, config files): paste the **expected** and **actual** text and click Compare for a line-by-line diff (added/removed/changed lines), with an option to ignore leading/trailing whitespace per line.
- **👤 Subscriber** — for every pattern captured via **👤 Subscriber** golden capture (i.e. every pattern that existed in the baseline env at capture time, not just the Config-tab list), fetches the matching `subscriber` row from the **target** env. A pattern missing from the target entirely is reported as **MISSING IN TARGET** with the baseline's details shown directly; a pattern present on both sides gets a full side-by-side baseline-vs-target field table (matching fields included, not just diffs), with volatile fields (timestamps, IPs, request IDs) flagged yellow as a non-failing warning rather than red. Produces a downloadable report that lands in its own **Subscriber Compare Reports** section on the Dashboard, with clickable Pass/Fail/Missing-in-target/Other filter tiles.

### Watch (Live) Tab

Use this when you do not know the exact start time of the flow in advance.

1. Pick golden source **🗄 DB** or **🧬 Kowl**, enter a pattern (DB) if needed, and click **Start Watch**.
2. Trigger your WMS flow.
3. The app polls in real time (every `poll_interval` seconds for DB) and streams results to the browser as they arrive.
4. Click **Stop Watch** when done.

### Full Run (Dashboard)

Combines Watch + Compare into a single timed session and generates Allure + HTML reports. Found at the top of the **Dashboard** tab, not a separate nav item.

1. Pick golden source **🗄 DB** or **🧬 Kowl**, then click **▶ Start Full Run**. The app begins watching all configured flows/topics.
2. Trigger your flow(s).
3. Click **Stop**. The app finalizes the comparison and generates reports.
4. Download the `allure-results.zip`, or view the in-app Allure HTML report if the CLI is installed, from the **Past Reports** section further down the Dashboard.

### Golden Snapshots Tab

Browse, view, and delete captured goldens (across DB/Kowl/ISD/subscriber sources). Supports multi-select for bulk delete.

### Dashboard Reports

Two independent, collapsed-by-default sections, each with its own **⤢ Expand / ⤡ Minimize** toggle so the page stays short until you need them:

- **Past Reports** — every HTML diff report and Allure run from DB/Kowl/Full Run compares, each with a stable `#id` (assigned chronologically, oldest = `#1`). Search by name/project, sort newest/oldest/name, download as HTML, open the Allure viewer, or bulk-delete.
- **Subscriber Compare Reports** — reports from the Subscriber compare feature, kept separate from the list above so the two report kinds never mix. Click a row to expand its per-pattern label/pattern/status breakdown inline.

---

## Project Structure

```
Comparator/
├── app.py                      # Entrypoint — starts Flask on :5050
├── config.json                 # Config incl. encrypted secret blobs — gitignored, not committed
├── .config_key                  # Local-only encryption key for config.json's secrets — gitignored, auto-generated
├── requirements.txt            # Python dependencies
├── core/
│   ├── routes.py               # All Flask API endpoints
│   ├── config.py               # Config load/save + secret encryption/decryption
│   ├── db.py                   # SSH tunnel + Postgres queries
│   ├── golden.py               # Golden snapshot read/write (project- and kowl_project-aware)
│   ├── diffing.py              # DeepDiff wrapper + dynamic field exclusion
│   ├── live.py                 # Live watch / Full Run logic
│   ├── kowl.py                 # Kowl WebSocket client (topic capture/compare)
│   ├── isd.py                  # ISD PDF/paste extraction — files straight into db/kowl goldens
│   ├── allure.py               # Allure results generation
│   ├── reports.py              # HTML report generation
│   └── state.py                # In-memory run state
├── static/
│   ├── index.html              # Single-page UI
│   ├── app.js                  # Frontend logic
│   └── app.css                 # Styles
├── golden/                     # Golden snapshot JSON files (auto-created)
│   ├── {project}/db/           #   DB (and ISD-captured DB-shaped) goldens, one folder per pattern label
│   ├── {project}/subscriber/   #   Subscriber snapshot goldens, one file per pattern
│   └── {kowl_project}/kowl/    #   Kowl (and ISD-captured Kowl-shaped) goldens — independent project namespace
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

To add or remove fields from this list, edit `IGNORE_FIELDS` in `core/diffing.py`.

---

## Troubleshooting

### "SSH connection refused" or tunnel timeout
- Every SSH tunnel attempt (Capture, Compare, Watch, Full Run, connection tests) already retries up to **5 times**, 3 seconds apart, before failing — so a single transient blip on the gateway won't fail the whole run. If you still see an error after that, it's a real connectivity issue, not a fluke.
- Confirm VPN is active and the jump host (`ssh_host` for baseline, `ssh_host_b` for target) is reachable: `ping <ssh_host>`
- Verify the SSH key path is correct and the key has the right permissions: `chmod 600 ~/.ssh/<key>`
- Try connecting manually: `ssh -i ~/.ssh/<key> <ssh_user>@<ssh_host>`

### "Authentication failed" for DB
- Double-check the DB password entered on the Config tab — baseline and target have separate password fields.
- Confirm `db_user` and `db_name` match the environment you're testing.

### "No subscriber found for pattern '...'"
- The pattern text must exactly match a `subscriber.pattern` value in the `subscriber` table on that DB — check for typos or trailing whitespace.
- Confirm you're querying the right side: Capture uses the **baseline** DB, Compare/Watch/Full Run use the **target** DB — the pattern may only exist on one of them.

### Kowl test fails
- Check that `topic_host` / `topic_host_b` are reachable from this machine (e.g. `curl http://<topic_host>/api/topics`).
- The app accepts plain `host:port`, `http://host:port`, or `https://host:port` — no trailing slash needed.
- Some Kowl deployments are behind a reverse proxy or require VPN — verify the URL manually in a browser first.

### Golden files not found during Compare
- Run **Capture** first with a known-good flow before comparing (or capture one from an ISD PDF/paste — either way it must exist before Compare can find it).
- Check that the `project` setting in Config matches the subdirectory name under `golden/` for DB data, or `kowl_project` for Kowl data — these are two independent values, so double-check you're looking at the right one. An ISD/paste capture files into whichever of the two it auto-detects as (db or kowl), so check both if unsure.

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
| `cryptography` | Encrypts DB passwords at rest in `config.json` |
| `xmltodict` | XML-to-dict parsing for XML compare |

---

## License

Internal tool — GreyOrange. Not for external distribution.

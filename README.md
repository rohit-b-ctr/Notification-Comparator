# Notification Comparator

Automates WMS notification schema validation. Connects to your Postgres DB via SSH tunnel, compares notification payloads against golden snapshots, and shows results in a web UI.

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

#### Optional — Allure HTML reports
**Full Run (Live)** always produces a downloadable `allure-results` `.zip`. To also
auto-generate the **Allure HTML report** in the app, install the Allure CLI (needs a JRE)
on the machine running `app.py`:
```bash
brew install allure                 # macOS (pulls Java if needed)
# or:  npm install -g allure-commandline   (still needs Java on PATH)
```
Without it, the app shows a warning on the dashboard and you can still view results anywhere
Allure is installed:
```bash
unzip allure_<timestamp>.zip -d allure-results
allure serve allure-results
```

### 2. Run
```bash
cd Comparator
python app.py
```
Then open **http://localhost:5050** in your browser.

### 3. Enter secrets (each session)
On the **Config** tab, enter your **DB password** and the path to your **SSH key**
(e.g. `~/rohit_b_ctr_greyorange_com`). These are never written to `config.json`;
they're kept in memory and must be re-entered each time the app starts (or saved
to a local `.secrets` file via the "Save" option). All other settings (hosts,
ports, subscriber IDs) live in `config.json` and persist.

---

## Files

| File | Purpose |
|------|---------|
| `app.py` | Flask web UI — run this |
| `notification_comparator.py` | CLI version (optional) |
| `golden/` | Golden snapshot JSON files (auto-created on first capture) |
| `reports/` | HTML reports from CLI compare runs (auto-created) |

---

## Workflow

### Step 1 — Capture Golden Baseline
Run a known-good flow, then in the UI go to **Capture Golden**, set the time range, and click Capture. This saves one `.json` file per notification type into `golden/`.

### Step 2 — Compare Future Runs
After each new flow run, go to **Compare**, set the `since` time, and click Compare. You'll see a pass/fail table with expandable diff rows.

### Step 3 — Or use Watch (Live)
Go to **Watch**, click Start, then trigger your flow. Notifications are compared in real time as they land in the DB.

---

## Golden Snapshot Naming
Files in `golden/` are named:
```
{FLOW_TYPE}__{state}__{notification_type}.json
```
e.g. `PICK__created__order_information.json`

---

## DB / SSH Config
Non-secret settings are stored in `config.json` (editable there or via the **Config** tab).
Defaults live in `DEFAULT_CONFIG` at the top of `app.py`:

Secrets (DB password, SSH key path) are **not** stored in `config.json` — see Setup step 3.

---

## Dynamic Fields (ignored during comparison)
These fields are stripped before diffing to avoid false positives:
`id`, `eventdata_id`, `notification_id`, `execution_id`, `createdOn`, `updatedOn`, `receivedOn`, `create_time`, `externalServiceRequestId`, `sr_parent`, `sr_parentsIds`

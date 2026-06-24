Golden Snapshot Directory
=========================

Each file here is a "golden" (expected) notification payload.

File naming convention:
  {FLOW_TYPE}__{state}__{notification_type}.json

Examples:
  PICK__created__order_information.json
  PICK__assigned__order_information.json
  PICK__completed__order_information.json
  PUT__created__order_information.json
  AUDIT__created__order_information.json

How to populate:
  Option A — Auto-capture from a known-good run:
    python notification_comparator.py capture --since "2026-06-03 10:00:00" --subscriber 158

  Option B — Paste your expected JSON manually into a file with the above naming.
    The payload should be already normalized (no @type / java.util.* wrappers).

Once golden files exist, run:
  python notification_comparator.py compare --since "2026-06-03 11:00:00"
or live watch:
  python notification_comparator.py watch

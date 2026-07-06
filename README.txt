# Telemetry Dashboard

Streamlit app to look up Dell PowerEdge support cases — pulls case info from Greenplum and hardware telemetry (LC logs + DCIM config) from S3, and flags BAD components.

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
streamlit run telemetry_dashboard_progressive.py
```

Opens at `http://localhost:8501`.

## Usage

1. Enter a case number (or multiple, comma-separated) and click **Fetch Data**.
2. Case info loads first, then LC Logs, then Config Data.
3. Use sidebar filters to narrow logs/config. Click a day in the timeline to drill into hours.
4. If results look wrong, open the **Diagnostics** panel.
5. Use **Clear Cache & Re-fetch** to force a fresh pull.

## Notes

- Credentials are hardcoded in the script — fine for personal use, move to env vars/secrets before sharing further.
- Config Data fetch is the slowest step (large upstream S3 files) — this is expected.

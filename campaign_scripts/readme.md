# Campaign File Processing Engine

A Python-based campaign processing engine for ingesting campaign CSV files, validating structure, tracking execution, moving files across process states, and sending operational alerts.

## Overview

The engine monitors the export folder, picks upcoming CSV files, verifies filename/channel context, and sends each file to the processing pipeline.

Current target channels:
- Email
- SMS

## Current Processing Flow

1. Read upcoming CSV files from `campaign/export`.
2. Identify channel from filename convention (example: `sms_1124.csv` -> channel `sms`).
3. Send file to `export_process`.
4. Read file and run empty-file check. `DONE`
5. Move file to in-progress once valid. `DONE`
6. Validate export column format from column master. `DONE`
7. Write audit logs for `START`, `FAIL`, `COMPLETE`. `DONE`
8. Send email alerts for fail and complete events. `DONE`
9. Move to processed folder on success. `DONE`
10. Move to failed folder on failure. `DONE`

## Implemented Modules

- File reader and CSV dispatcher (`forward_integration.py`)
- Export processing orchestrator (`export_process.py`)
- File movement utilities (`camp_utils.py`)
  - move to in-progress
  - move to processed
  - move to failed
- Audit logging module (`campaign_audit.py`)
  - start, fail, complete status tracking
- Email alert module (`email_alert.py`)
  - success/failure alerts
- Export column format validator (`modules/export_validator.py`)

## Upcoming Modules

### 1) Seedlist and Live Run Check
- Seedlist and liverun check
- Seedlist module
- Liverun module

### 2) Exclusion Module
- Global exclusion
- Marketing exclusion
- Marketing opt-out exclusion
- Other exclusion
- Loan exclusion
- Channel exclusion

### 3) Contact & Policy Modules
- Contact matching module
- Contact policy module

### 4) Channel Check and Delivery
- Channel module split by channel (Email and SMS)
- Communication ID logic
- Build API for sending campaign via email
- Build API for sending campaign via SMS

## Suggested File Naming Convention

Use filenames that start with channel name:
- `email_<batch>.csv`
- `sms_<batch>.csv`

Example:
- `email_1125.csv`
- `sms_1124.csv`

## Project Structure (Key Paths)

- `campaign/export` -> incoming files
- `campaign/inprogress_campaign` -> in-progress files
- `campaign/processed` -> successfully processed files
- `campaign/processed_failed` -> failed files
- `campaign/logs` -> execution logs
- `campaign/config/column_master.xlsx` -> expected column format
- `campaign_scripts/DB_files/campaign_audit.csv` -> audit history

## How to Run

From project root:

```bash
python campaign_scripts/forward_integration.py
```

## Logging and Audit

- Runtime logs are written to `campaign/logs/forward_integration_<yymmdd>.log`.
- Audit trail is written to `campaign_scripts/DB_files/campaign_audit.csv`.
- Status transitions: `START` -> `COMPLETE` or `failed`.

## Notes

- Email alert sender and credentials are configured in `campaign_scripts/email_alert.py`.
- For production, move secrets (SMTP/app password) to environment variables or a secure vault. 
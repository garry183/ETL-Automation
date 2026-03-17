# ProPlusData — Lead Aggregator

Daily automation that reads lead data from three sources, produces a brand-wise summary CSV, and posts the results to a Slack channel.

Runs every day at 12:00 PM via Windows Task Scheduler.

---

## What it does

1. Reads three input files placed in the configured input folder
2. Aggregates and breaks down leads by brand (**Livguard** / **Livfast**)
3. Saves a summary CSV to the output folder (dated to yesterday, since data is always previous day)
4. Posts the summary as a formatted table to Slack

---

## Output columns

| Column | Description |
|---|---|
| Brand | Overall / Livguard / Livfast |
| LSQ | Total leads from LSQ (differentiated by `pageUrl` domain) |
| Livsol | Total leads from Livsol |
| Allocated | Leads with Status = Allocated |
| Unallocated | Leads with Status = Un-Allocated |
| DB Submissions | Database form submissions where `otp_verified = false` |
| DB Unique Phones | Unique phone numbers among those submissions |

---

## Input files

Place these files in the folder defined by `input_folder` in `config.json`:

| File | Description |
|---|---|
| `LSQ Leads.csv` | Tab-separated export from LSQ. Brand inferred from `pageUrl` column (`livguard.com` → Livguard, `livfast.in` → Livfast) |
| `Livsol Leads.csv` | CSV export from Livsol. Brand from `Brand` column, allocation status from `Status` column |
| `Database Leads.csv` | Database form submissions with `form_response` JSON column and `brand` column |

> Input files and output files are **not committed** to this repository. Add them to the configured folders manually.

---

## Setup

### 1. Install dependencies

```bash
pip install pandas requests python-dotenv
```

### 2. Configure `config.json`

```json
{
    "input_folder": "path/to/input",
    "output_folder": "path/to/output",
    "files": {
        "lsq": "LSQ Leads.csv",
        "livsol": "Livsol Leads.csv",
        "database": "Database Leads.csv"
    },
    "livsol_allocation_column": "Status",
    "livsol_allocated_value": "Allocated",
    "slack": {
        "channel": "#your-channel",
        "mention_user": ""
    }
}
```

### 3. Set environment variables

Copy `.env.example` to `.env` and fill in your Slack webhook URL:

```bash
cp .env.example .env
```

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

> `.env` is gitignored and will never be committed.

### 4. Run manually

```bash
python lead_aggregator.py
```

### 5. Schedule via Windows Task Scheduler

- **Program:** `python`
- **Arguments:** `lead_aggregator.py`
- **Start in:** full path to this project folder
- **Trigger:** Daily at 12:00 PM

---

## Slack output

The bot posts a monospace table directly to the configured channel:

```
:bar_chart: Lead Summary — 16 Mar 2026

Brand        LSQ   Livsol   Alloc   Unalloc  DB Sub  DB Uniq
------------ ----- ------- ------- --------- ------- -------
Overall       271    270      48      222      333     275
Livguard      260    260      46      214      312     263
Livfast        10     10       2        8       15      11
```

---

## Project structure

```
ProPlusData/
├── lead_aggregator.py   # Main script
├── config.json          # Non-sensitive configuration
├── .env                 # Slack webhook URL (gitignored)
├── .env.example         # Template for .env
├── .gitignore
├── 3files/              # Input CSVs (gitignored)
└── ouput/               # Output CSVs (gitignored)
```

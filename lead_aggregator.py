"""
Lead Aggregator - Daily automation
Reads LSQ, Livsol, Database Leads files and sends summary to Slack.
Schedule: 12:00 PM daily via Windows Task Scheduler
"""

import json
import os
import re
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_FILE = Path(__file__).parent / "lead_aggregator.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ── Load config ────────────────────────────────────────────────────────────────
def load_config() -> dict:
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)


# ── Extract: LSQ Leads ─────────────────────────────────────────────────────────
def _lsq_brand_from_url(url: str) -> str:
    """Infer brand from pageUrl domain."""
    if not isinstance(url, str):
        return "Other"
    if "livfast" in url.lower():
        return "Livfast"
    if "livguard" in url.lower():
        return "Livguard"
    return "Other"


def extract_lsq(path: str) -> dict:
    df = pd.read_csv(path, encoding="latin1", sep=None, engine="python")
    total = len(df)

    # Case-insensitive lookup for the page URL column
    page_url_col = next(
        (c for c in df.columns if c.lower().replace(" ", "").replace("_", "") == "pageurl"),
        None,
    )

    brand_stats = {}
    if page_url_col:
        log.info(f"LSQ: using column '{page_url_col}' for brand detection")
        df["_brand"] = df[page_url_col].apply(_lsq_brand_from_url)
        for brand in ["Livguard", "Livfast"]:
            count = int((df["_brand"] == brand).sum())
            brand_stats[f"lsq_{brand.lower()}_total"] = count
        log.info(
            f"LSQ Leads: {total} total | "
            + " | ".join(f"{b}: {brand_stats[f'lsq_{b.lower()}_total']}" for b in ["Livguard", "Livfast"])
        )
    else:
        log.warning(f"LSQ: 'pageUrl' column not found. Available columns: {list(df.columns)}")
        log.info(f"LSQ Leads: {total} total rows")

    return {"lsq_total": total, **brand_stats}


# ── Extract: Livsol Leads ──────────────────────────────────────────────────────
def _normalize_brand(brand_val: str) -> str:
    """Map brand variants to canonical 'Livguard' or 'Livfast'."""
    val = str(brand_val).lower()
    if "livfast" in val:
        return "Livfast"
    if "livguard" in val:
        return "Livguard"
    return "Other"


def extract_livsol(path: str, alloc_col: str = "", allocated_val: str = "") -> dict:
    df = pd.read_csv(path) if path.endswith(".csv") else pd.read_excel(path)
    total = len(df)

    if alloc_col and alloc_col in df.columns:
        allocated = int((df[alloc_col] == allocated_val).sum())
        unallocated = total - allocated
        log.info(f"Livsol: {total} total, {allocated} allocated, {unallocated} unallocated")
    else:
        # Fallback: if no allocation column configured, count non-null phone numbers as allocated
        phone_col = next(
            (c for c in df.columns if "phone" in c.lower()), None
        )
        if phone_col:
            allocated = int(df[phone_col].notna().sum())
            unallocated = total - allocated
            log.warning(
                f"No allocation column configured — using phone number presence as proxy. "
                f"Allocated (has phone): {allocated}, Unallocated: {unallocated}"
            )
        else:
            allocated = total
            unallocated = 0
            log.warning("No allocation column or phone column found — all leads marked as allocated")

    # Brand-wise allocated / unallocated
    brand_stats = {}
    if "Brand" in df.columns and alloc_col and alloc_col in df.columns:
        df["_brand"] = df["Brand"].apply(_normalize_brand)
        for brand in ["Livguard", "Livfast"]:
            bdf = df[df["_brand"] == brand]
            b_alloc = int((bdf[alloc_col] == allocated_val).sum())
            b_unalloc = int(len(bdf) - b_alloc)
            brand_key = brand.lower()
            brand_stats[f"livsol_{brand_key}_total"] = int(len(bdf))
            brand_stats[f"livsol_{brand_key}_allocated"] = b_alloc
            brand_stats[f"livsol_{brand_key}_unallocated"] = b_unalloc
            log.info(f"Livsol {brand}: {len(bdf)} total, {b_alloc} allocated, {b_unalloc} unallocated")

    return {
        "livsol_total": total,
        "livsol_allocated": allocated,
        "livsol_unallocated": unallocated,
        **brand_stats,
    }


# ── Extract: Database Leads ────────────────────────────────────────────────────
def extract_database(path: str) -> dict:
    df = pd.read_csv(path)

    # Parse form_response JSON and extract otp_verified + phoneNumber
    def parse_row(row):
        try:
            return json.loads(row)
        except Exception:
            return {}

    parsed = df["form_response"].apply(parse_row)

    # Filter where otp_verified is False
    not_verified = parsed.apply(lambda x: x.get("otp_verified") is False)
    filtered_parsed = parsed[not_verified]
    filtered_df = df[not_verified].copy()
    filtered_df["_phoneNumber"] = filtered_parsed.apply(lambda x: x.get("phoneNumber", "")).values

    total_submissions = int(not_verified.sum())
    unique_phones = filtered_df["_phoneNumber"].nunique()

    # Brand-wise breakdown (Livguard / Livfast)
    brands = ["Livguard", "Livfast"]
    brand_stats = {}
    for brand in brands:
        brand_mask = filtered_df["brand"].str.strip().str.lower() == brand.lower()
        brand_df = filtered_df[brand_mask]
        brand_key = brand.lower()
        brand_stats[f"db_{brand_key}_submissions"] = int(brand_mask.sum())
        brand_stats[f"db_{brand_key}_unique_phones"] = int(brand_df["_phoneNumber"].nunique())

    log.info(
        f"Database Leads: {total_submissions} submissions (otp_verified=false), "
        f"{unique_phones} unique phone numbers | "
        + " | ".join(
            f"{b}: {brand_stats[f'db_{b.lower()}_submissions']} submissions, "
            f"{brand_stats[f'db_{b.lower()}_unique_phones']} unique phones"
            for b in brands
        )
    )
    return {
        "db_total_submissions": total_submissions,
        "db_unique_phones": int(unique_phones),
        **brand_stats,
    }


# ── Build output DataFrame ─────────────────────────────────────────────────────
def build_output(lsq: dict, livsol: dict, db: dict) -> pd.DataFrame:
    today = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

    def brand_row(brand: str) -> dict:
        key = brand.lower()
        return {
            "Date": "",
            "Brand": brand,
            "LSQ": lsq.get(f"lsq_{key}_total", ""),
            "Livsol": livsol.get(f"livsol_{key}_total", ""),
            "Allocated": livsol.get(f"livsol_{key}_allocated", ""),
            "Unallocated": livsol.get(f"livsol_{key}_unallocated", ""),
            "DB Submissions": db.get(f"db_{key}_submissions", ""),
            "DB Unique Phones": db.get(f"db_{key}_unique_phones", ""),
        }

    rows = [
        {
            "Date": today,
            "Brand": "Overall",
            "LSQ": lsq["lsq_total"],
            "Livsol": livsol["livsol_total"],
            "Allocated": livsol["livsol_allocated"],
            "Unallocated": livsol["livsol_unallocated"],
            "DB Submissions": db["db_total_submissions"],
            "DB Unique Phones": db["db_unique_phones"],
        },
        brand_row("Livguard"),
        brand_row("Livfast"),
    ]
    return pd.DataFrame(rows)


# ── Save output ────────────────────────────────────────────────────────────────
def save_output(df: pd.DataFrame, output_folder: str) -> str:
    today = (datetime.now() - timedelta(days=1)).strftime("%d_%m_%Y")
    filename = f"LeadData-{today}.csv"
    output_path = Path(output_folder) / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    log.info(f"Output saved: {output_path}")
    return str(output_path)


# ── Send to Slack ──────────────────────────────────────────────────────────────
def send_to_slack(output_path: str, summary: dict, slack_cfg: dict):
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        log.warning("SLACK_WEBHOOK_URL not set — skipping Slack notification")
        return

    today = (datetime.now() - timedelta(days=1)).strftime("%d %b %Y")
    mention = slack_cfg.get("mention_user", "")
    mention_str = f"<{mention}> " if mention else ""

    # Build fixed-width table matching the CSV output
    header  = f"{'Brand':<12} {'LSQ':>6} {'Livsol':>7} {'Alloc':>7} {'Unalloc':>9} {'DB Sub':>7} {'DB Uniq':>8}"
    divider = f"{'-'*12} {'-'*6} {'-'*7} {'-'*7} {'-'*9} {'-'*7} {'-'*8}"

    def table_row(brand: str) -> str:
        key = brand.lower()
        lsq_val = summary["lsq_total"]             if brand == "Overall" else summary.get(f"lsq_{key}_total", 0)
        liv_val = summary["livsol_total"]           if brand == "Overall" else summary.get(f"livsol_{key}_total", 0)
        alloc   = summary["livsol_allocated"]       if brand == "Overall" else summary.get(f"livsol_{key}_allocated", 0)
        unalloc = summary["livsol_unallocated"]     if brand == "Overall" else summary.get(f"livsol_{key}_unallocated", 0)
        db_sub  = summary["db_total_submissions"]   if brand == "Overall" else summary.get(f"db_{key}_submissions", 0)
        db_uniq = summary["db_unique_phones"]       if brand == "Overall" else summary.get(f"db_{key}_unique_phones", 0)
        return f"{brand:<12} {lsq_val:>6} {liv_val:>7} {alloc:>7} {unalloc:>9} {db_sub:>7} {db_uniq:>8}"

    table = "\n".join([header, divider] + [table_row(b) for b in ["Overall", "Livguard", "Livfast"]])

    message = (
        f"{mention_str}:bar_chart: *Lead Summary — {today}*\n"
        f"```\n{table}\n```"
    )

    response = requests.post(webhook_url, json={"text": message}, timeout=10)
    if response.status_code == 200:
        log.info("Slack message sent successfully")
    else:
        log.error(f"Slack webhook failed: {response.status_code} — {response.text}")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("Lead Aggregator started")

    cfg = load_config()
    input_folder = cfg["input_folder"]
    files = cfg["files"]

    lsq_path = f"{input_folder}/{files['lsq']}"
    livsol_path = f"{input_folder}/{files['livsol']}"
    db_path = f"{input_folder}/{files['database']}"

    # Validate files exist
    for label, path in [("LSQ", lsq_path), ("Livsol", livsol_path), ("Database", db_path)]:
        if not Path(path).exists():
            log.error(f"{label} file not found: {path}")
            sys.exit(1)

    # Extract data
    lsq = extract_lsq(lsq_path)
    livsol = extract_livsol(
        livsol_path,
        alloc_col=cfg.get("livsol_allocation_column", ""),
        allocated_val=cfg.get("livsol_allocated_value", ""),
    )
    db = extract_database(db_path)

    # Build and save output
    df_out = build_output(lsq, livsol, db)
    output_path = save_output(df_out, cfg["output_folder"])

    print("\n" + "=" * 50)
    print(f"  Date     : {(datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')}")
    print(f"  {'Brand':<12} {'LSQ':>6} {'Livsol':>7} {'Alloc':>7} {'Unalloc':>9} {'DB Sub':>7} {'DB Uniq':>8}")
    print(f"  {'-'*12} {'-'*6} {'-'*7} {'-'*7} {'-'*9} {'-'*7} {'-'*8}")
    for brand in ["Overall", "Livguard", "Livfast"]:
        key = brand.lower()
        lsq_val  = lsq["lsq_total"]        if brand == "Overall" else lsq.get(f"lsq_{key}_total", 0)
        liv_val  = livsol["livsol_total"]   if brand == "Overall" else livsol.get(f"livsol_{key}_total", 0)
        alloc    = livsol["livsol_allocated"]   if brand == "Overall" else livsol.get(f"livsol_{key}_allocated", 0)
        unalloc  = livsol["livsol_unallocated"] if brand == "Overall" else livsol.get(f"livsol_{key}_unallocated", 0)
        db_sub   = db["db_total_submissions"]   if brand == "Overall" else db.get(f"db_{key}_submissions", 0)
        db_uniq  = db["db_unique_phones"]       if brand == "Overall" else db.get(f"db_{key}_unique_phones", 0)
        print(f"  {brand:<12} {lsq_val:>6} {liv_val:>7} {alloc:>7} {unalloc:>9} {db_sub:>7} {db_uniq:>8}")
    print(f"  Output saved : {output_path}")
    print("=" * 50 + "\n")

    # Send to Slack
    summary = {**lsq, **livsol, **db}
    send_to_slack(output_path, summary, cfg["slack"])

    log.info("Lead Aggregator completed successfully")


if __name__ == "__main__":
    main()

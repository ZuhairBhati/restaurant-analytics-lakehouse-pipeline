"""
=============================================================
  CSV → Azure SQL DB Uploader
  Uploads local CSV files into Azure SQL DB tables (dbo schema)
=============================================================
Requirements:
    pip install pandas pyodbc sqlalchemy

    Also install the ODBC Driver for SQL Server:
    https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
=============================================================
"""

import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import logging
import os
import sys

# ─────────────────────────────────────────────────────────────
# 1. CONFIGURE LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("upload_log.txt", mode="w"),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 2. AZURE SQL CONNECTION SETTINGS  ← Edit these
# ─────────────────────────────────────────────────────────────
SERVER   = "server host url"
DATABASE = "databse name"
USERNAME = "user_name"
PASSWORD = "password"
DRIVER   = "ODBC Driver 18 for SQL Server"   # or "ODBC Driver 17 for SQL Server"


# ─────────────────────────────────────────────────────────────
# 3. CSV FOLDER PATH  ← Edit this
# ─────────────────────────────────────────────────────────────
CSV_FOLDER = r"path_to_your_csv_folder"


# ─────────────────────────────────────────────────────────────
# 4. CSV → TABLE MAPPING (pre-filled with your tables)
# ─────────────────────────────────────────────────────────────
CSV_TABLE_MAP = {
    "restaurants.csv"      : "dbo.restaurants",
    "customers.csv"        : "dbo.customers",
    "reviews.csv"          : "dbo.reviews",
    "menu_items.csv"       : "dbo.menu_items",
    "historical_orders.csv": "dbo.historical_orders",
}


# ─────────────────────────────────────────────────────────────
# 5. COLUMN RENAMES PER CSV
#    Fixes mismatches between CSV headers and table column names
# ─────────────────────────────────────────────────────────────
COLUMN_RENAMES = {
    # CSV has 'timestamp' but table column is 'order_timestamp'
    "historical_orders.csv": {
        "timestamp": "order_timestamp"
    }
}


# ─────────────────────────────────────────────────────────────
# 6. UPLOAD MODE
#    "replace" → TRUNCATES the table first, then inserts fresh
#    "append"  → Adds rows without clearing existing data
# ─────────────────────────────────────────────────────────────
UPLOAD_MODE = "replace"


# ─────────────────────────────────────────────────────────────
# 7. CSV READ OPTIONS
# ─────────────────────────────────────────────────────────────
CSV_OPTIONS = {
    "sep"     : ",",       # Change to "\t" if tab-separated
    "encoding": "utf-8",   # Try "latin-1" if utf-8 throws an error
    "header"  : 0,
}


# ─────────────────────────────────────────────────────────────
# HELPER: Build SQLAlchemy engine
# ─────────────────────────────────────────────────────────────
def get_engine():
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
    )
    return engine


# ─────────────────────────────────────────────────────────────
# HELPER: Upload a single CSV to a table
# ─────────────────────────────────────────────────────────────
def upload_csv(engine, csv_file: str, full_table_name: str, mode: str):
    csv_path = os.path.join(CSV_FOLDER, csv_file)
    schema, table = full_table_name.split(".")

    log.info(f" Reading  : {csv_file}")
    df = pd.read_csv(csv_path, **CSV_OPTIONS)

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Apply column renames for this file (if any)
    renames = COLUMN_RENAMES.get(csv_file, {})
    if renames:
        df.rename(columns=renames, inplace=True)
        log.info(f"   Renamed columns: {renames}")
    
    # Convert datetime string columns to proper datetime type
    DATETIME_COLUMNS = {
        "historical_orders.csv": ["order_timestamp"],
        "reviews.csv":           ["review_timestamp"],
    }

    for col in DATETIME_COLUMNS.get(csv_file,[]):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            log.info(f"   Parsed datetime: {col}")

    log.info(f"   Rows     : {len(df):,}  |  Columns: {list(df.columns)}")

    if mode == "replace":
        log.info(f"   Truncating [{full_table_name}] ...")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {full_table_name}"))

    log.info(f"   Uploading → [{full_table_name}] ...")
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=1000
    )

    log.info(f"    Done! {len(df):,} rows loaded into [{full_table_name}]\n")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("  Azure SQL CSV Uploader")
    log.info("=" * 55)
    log.info(f"Server   : {SERVER}")
    log.info(f"Database : {DATABASE}")
    log.info(f"Mode     : {UPLOAD_MODE.upper()}")
    log.info(f"Tables   : {len(CSV_TABLE_MAP)}")
    log.info("=" * 55 + "\n")

    # Connect
    log.info("🔌 Connecting to Azure SQL...")
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("   Connection successful!\n")
    except Exception as e:
        log.error(f"❌ Connection failed: {e}")
        sys.exit(1)

    # Process each CSV
    success, failed = [], []

    for csv_file, table_name in CSV_TABLE_MAP.items():
        csv_path = os.path.join(CSV_FOLDER, csv_file)

        if not os.path.exists(csv_path):
            log.warning(f"  File not found, skipping: {csv_path}")
            failed.append(csv_file)
            continue

        try:
            upload_csv(engine, csv_file, table_name, UPLOAD_MODE)
            success.append(csv_file)
        except Exception as e:
            log.error(f" Failed [{csv_file}]: {e}\n")
            failed.append(csv_file)

    # Summary
    log.info("=" * 55)
    log.info(f"  SUMMARY: {len(success)} succeeded | {len(failed)} failed")
    if success:
        log.info(f"  Uploaded : {', '.join(success)}")
    if failed:
        log.info(f"   Failed   : {', '.join(failed)}")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
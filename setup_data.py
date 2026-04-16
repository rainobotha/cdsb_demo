#!/usr/bin/env python3
"""
setup_data.py — Download TMR open data from QLD CKAN and upload to Snowflake stage.

Run this once before executing the demo notebooks:

    SNOWFLAKE_CONNECTION_NAME=AU_DEMO50 python setup_data.py

The script:
  1. Queries the QLD Open Data CKAN API for the latest TMR registrations CSV
  2. Downloads the CSV (~42 MB)
  3. Converts date format from YYYY-MM-DD to DD-MM-YYYY (matching COPY INTO format)
  4. Uploads to @<DB>.<SCHEMA>.QLD_OPEN_DATA/tmr/ via Snowflake PUT
"""

import os
import sys
import csv
import gzip
import tempfile
from datetime import datetime

CKAN_DATASET = "new-business-registration-transactions-by-service-location"
CKAN_API = f"https://www.data.qld.gov.au/api/3/action/package_show?id={CKAN_DATASET}"

CONN_NAME = os.getenv("SNOWFLAKE_CONNECTION_NAME", "AU_DEMO50")
DATABASE = os.getenv("CDSB_DATABASE", "CDSB_DEMO")
SCHEMA = os.getenv("CDSB_SCHEMA", "RAW")


def get_download_url():
    import urllib.request
    import json

    print(f"Querying CKAN API for dataset: {CKAN_DATASET}")
    with urllib.request.urlopen(CKAN_API) as resp:
        data = json.loads(resp.read())

    resources = data["result"]["resources"]
    if not resources:
        print("ERROR: No resources found in CKAN dataset", file=sys.stderr)
        sys.exit(1)

    url = resources[0]["url"]
    size = resources[0].get("size", "unknown")
    modified = resources[0].get("last_modified", "unknown")
    print(f"  Resource: {resources[0]['name']}")
    print(f"  Size: {size}")
    print(f"  Last modified: {modified}")
    print(f"  URL: {url}")
    return url


def download_csv(url, dest_path):
    import urllib.request

    print(f"\nDownloading CSV...")
    urllib.request.urlretrieve(url, dest_path)
    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    print(f"  Downloaded: {size_mb:.1f} MB -> {dest_path}")


def detect_encoding(path):
    """Detect file encoding from BOM."""
    with open(path, "rb") as f:
        head = f.read(4)
    if head[:2] == b"\xff\xfe":
        return "utf-16-le"
    if head[:2] == b"\xfe\xff":
        return "utf-16-be"
    if head[:3] == b"\xef\xbb\xbf":
        return "utf-8-sig"
    return "utf-8"


def convert_dates(src_path, dest_path):
    """Convert date column from various formats to DD-MM-YYYY for Snowflake COPY INTO."""
    print("Converting date format and encoding to UTF-8...")
    row_count = 0
    encoding = detect_encoding(src_path)
    print(f"  Source encoding: {encoding}")

    with open(src_path, "r", newline="", encoding=encoding) as fin, \
         gzip.open(dest_path, "wt", newline="", encoding="utf-8") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)

        header = next(reader)
        writer.writerow(header)

        date_col = 0

        for row in reader:
            if row[date_col]:
                try:
                    dt = datetime.strptime(row[date_col][:10], "%Y-%m-%d")
                    row[date_col] = dt.strftime("%d-%m-%Y")
                except ValueError:
                    try:
                        datetime.strptime(row[date_col][:10], "%d-%m-%Y")
                    except ValueError:
                        pass
            writer.writerow(row)
            row_count += 1

    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
    print(f"  Rows: {row_count:,}")
    print(f"  Compressed: {size_mb:.1f} MB -> {dest_path}")
    return row_count


def upload_to_stage(local_path):
    import snowflake.connector

    stage = f"@{DATABASE}.{SCHEMA}.QLD_OPEN_DATA/tmr/"

    print(f"\nConnecting to Snowflake ({CONN_NAME})...")
    conn = snowflake.connector.connect(connection_name=CONN_NAME)
    cur = conn.cursor()

    try:
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute(f"USE SCHEMA {SCHEMA}")
        cur.execute("CREATE STAGE IF NOT EXISTS QLD_OPEN_DATA")

        print(f"Uploading to {stage}...")
        cur.execute(
            f"PUT 'file://{local_path}' {stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        result = cur.fetchone()
        print(f"  Status: {result[6] if result else 'OK'}")

        cur.execute(f"LIST {stage}")
        files = cur.fetchall()
        for f in files:
            print(f"  Stage file: {f[0]} ({f[1]:,} bytes)")

        print("\nDone! The notebook can now run:")
        print(f"  COPY INTO TMR_REGISTRATIONS FROM {stage}")
    finally:
        cur.close()
        conn.close()


def main():
    url = get_download_url()

    with tempfile.TemporaryDirectory() as tmpdir:
        raw_csv = os.path.join(tmpdir, "tmr_registrations_raw.csv")
        gz_csv = os.path.join(tmpdir, "tmr_registrations_utf8.csv.gz")

        download_csv(url, raw_csv)
        row_count = convert_dates(raw_csv, gz_csv)

        if row_count < 1000:
            print(f"\nWARNING: Only {row_count:,} rows — expected ~300K+. The CKAN resource may have changed.", file=sys.stderr)

        upload_to_stage(gz_csv)


if __name__ == "__main__":
    main()

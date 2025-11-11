"""
Maryland Counties FRED Data Aggregator (Automated)
==================================================

Pulls monthly & annual FRED/BLS metrics for all 24 Maryland counties
using `fredapi`. Generates:
1. One CSV per county (for Tableau linkage)
2. A master combined dataset
3. A data dictionary summarizing all series IDs
4. A text-based pipeline summary (no Graphviz dependency)

Author: Joshua Kwan
Date: 11/11/2025
"""

import os
import time
import pandas as pd
from fredapi import Fred
from functools import reduce
from tqdm import tqdm

# ======================================================
# CONFIGURATION
# ======================================================

API_KEY = "2ccf5b794d310f8cde1d30c463f8d2d4"  # Replace with your own key
fred = Fred(api_key=API_KEY)
SLEEP_TIME = 0.25

COUNTY_EXPORT_PATH = "data/counties/"
MASTER_EXPORT_PATH = "data/master/"
os.makedirs(COUNTY_EXPORT_PATH, exist_ok=True)
os.makedirs(MASTER_EXPORT_PATH, exist_ok=True)

DATA_DICT_PATH = os.path.join("data", "data_dictionary.csv")
SUMMARY_PATH = os.path.join("data", "pipeline_summary.txt")

# ======================================================
# COUNTY SERIES DEFINITIONS
# ======================================================
# Keep your existing COUNTIES dictionary below exactly as defined.
# (Omitted here for brevity — use your verified mapping.)

from masterdatasetmulticounty_counties import COUNTIES  # Optional import if separated
# Or paste your full COUNTIES = { ... } dictionary here directly.

# ======================================================
# HELPER FUNCTIONS
# ======================================================

def period_to_month_end(series: pd.Series, freq: str) -> pd.DataFrame:
    """Convert a FRED series to a monthly/annual dataframe with month-end timestamps."""
    df = pd.DataFrame({"Date": pd.to_datetime(series.index, errors="coerce"), "Value": series.values})
    df = df.dropna(subset=["Date"])

    if freq == "M":
        df["Date"] = df["Date"].dt.to_period("M").dt.to_timestamp("M")
    else:
        df["Date"] = df["Date"].dt.to_period("Y").dt.to_timestamp("M")
        df = df.set_index("Date").resample("ME").ffill().reset_index()

    return df


def build_county_df(code: str, county_data: dict) -> pd.DataFrame:
    """Fetch and merge all available series for a single county."""
    frames = []

    for col_name, (series_id, freq) in tqdm(county_data["series"].items(), desc=f"Loading {code}", leave=False):
        try:
            time.sleep(SLEEP_TIME)
            series = fred.get_series(series_id)
            if series is None or series.empty:
                print(f"⚠️ {code} {col_name}: empty or invalid series {series_id}")
                continue

            df = period_to_month_end(series, freq).rename(columns={"Value": col_name})
            frames.append(df)
        except Exception as err:
            print(f"⚠️ {code} {col_name}: failed to load {series_id} -> {err}")

    if not frames:
        return pd.DataFrame()

    merged = reduce(lambda l, r: pd.merge(l, r, on="Date", how="outer"), frames)
    merged.insert(1, "County", county_data["County"])
    merged.insert(2, "County_Code", code)

    # Order columns
    metric_cols = sorted([c for c in merged.columns if c not in ["Date", "County", "County_Code"]])
    merged = merged[["Date", "County", "County_Code"] + metric_cols]

    return merged.sort_values("Date").reset_index(drop=True)


def generate_data_dictionary():
    """Generate a CSV dictionary mapping each county & metric to its FRED series ID."""
    rows = []
    for code, meta in COUNTIES.items():
        for metric, (sid, freq) in meta["series"].items():
            rows.append({
                "County_Code": code,
                "County_Name": meta["County"],
                "Metric": metric,
                "Series_ID": sid,
                "Frequency": "Monthly" if freq == "M" else "Annual"
            })
    df = pd.DataFrame(rows)
    df.to_csv(DATA_DICT_PATH, index=False)
    print(f"🗂️ Data dictionary saved to {DATA_DICT_PATH}")


def generate_pipeline_summary():
    """Write a simple text-based summary of the ETL pipeline."""
    summary = """
Maryland FRED Data Pipeline Summary
===================================

Data Source:
    → FRED API (24 Maryland counties, various BLS & Census indicators)

ETL Steps:
    1. Pull raw FRED time-series data for each indicator.
    2. Convert annual/monthly data to month-end timestamps.
    3. Merge county-level metrics into unified DataFrames.
    4. Save per-county CSVs under /data/counties.
    5. Combine all counties into master dataset under /data/master.
    6. Generate data_dictionary.csv documenting series and frequencies.

Output Files:
    - data/counties/<County>.csv
    - data/master/maryland_master.csv
    - data/data_dictionary.csv
    - data/pipeline_summary.txt

Integration:
    - Tableau connects directly to /data/master/maryland_master.csv
    - Optional: Automate monthly GitHub Action refresh
"""
    with open(SUMMARY_PATH, "w") as f:
        f.write(summary)
    print(f"📝 Pipeline summary written to {SUMMARY_PATH}")


# ======================================================
# MAIN EXECUTION
# ======================================================

def main():
    start_time = time.time()
    print("📊 Fetching FRED/BLS data for all Maryland counties...\n")
    all_dfs = []

    for code, meta in COUNTIES.items():
        df = build_county_df(code, meta)

        if df.empty:
            print(f"❗ Skipped {meta['County']} ({code}) - no data found.")
            continue

        file_name = f"{meta['County'].replace(' ', '_')}.csv"
        csv_path = os.path.join(COUNTY_EXPORT_PATH, file_name)
        df.to_csv(csv_path, index=False)
        print(f"✅ Exported: {csv_path}")
        all_dfs.append(df)

    # Merge into master dataset
    if all_dfs:
        master_df = pd.concat(all_dfs, ignore_index=True).sort_values(["County", "Date"])
        master_path = os.path.join(MASTER_EXPORT_PATH, "maryland_master.csv")
        master_df.to_csv(master_path, index=False)
        print(f"\n🎉 Master dataset saved: {master_path}")
        print(f"📊 {master_df.shape[0]} rows × {master_df.shape[1]} columns")
    else:
        print("\n❗ No valid data retrieved. Check FRED connection or series IDs.")

    # Documentation utilities
    generate_data_dictionary()
    generate_pipeline_summary()

    print(f"\n⏱️ Runtime: {time.time() - start_time:.2f} seconds")


# ======================================================
# ENTRY POINT
# ======================================================

if __name__ == "__main__":
    main()

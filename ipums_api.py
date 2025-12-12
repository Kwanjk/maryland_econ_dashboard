# IPUMS NHGIS (National Historical Geographic Information System) API
# Created: 12/2025
# Last Updated: 12/10/2025 

# Things to install if haven't already (only once)
# IPUMS Python Wrapper -- made by IPUMS themselves
#pip install ipumspy  


import re  # For file naming manipulation
from urllib.error import HTTPError  # Handle API request limit
import os
import time
import json
import requests
import yaml
import pandas as pd
from collections import defaultdict

from ipumspy import IpumsApiClient, MicrodataExtract, save_extract_as_json, AggregateDataExtract, NhgisDataset, TimeSeriesTable


# ------------------------- #
# Load API Key from YAML    #
# ------------------------- #
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

with open("api_keys.yaml", "r") as f:
    keys = yaml.safe_load(f)

IPUMS_API_KEY = keys["ipums_api"].strip()
headers = {
    "Authorization": IPUMS_API_KEY,
    "Content-Type": "application/json"
}

# ------------------------- #
# Output folder setup       #
# ------------------------- #
md_demog_output_folder = os.path.join(script_dir, "ipums_csv_outputs", "md_demog")
os.makedirs(md_demog_output_folder, exist_ok=True)
print(f"[INFO] Output folder: {md_demog_output_folder}")

# ------------------------- #
# Load Excel Series IDs     #
# ------------------------- #
file_path = "Indicators Series ID List.xlsx"
state_sheet = "MD IPUMS NHGIS"
series_id_df = pd.read_excel(file_path, sheet_name=state_sheet, skiprows=1)

# Skip first column (header/title)
id_list = series_id_df.iloc[0].dropna().astype(str).tolist()[1:]

# ------------------------- #
# Parse IDs into components #
# ------------------------- #
def split_id(x):
    """Parse NHGIS-style ID into name, specifier, year"""
    name = x[:3]
    specifier = x[3:5]
    year = x[-4:]
    return pd.Series([name, specifier, year])

parsed_df = pd.DataFrame(id_list, columns=["full_id"])
parsed_df[["name", "specifier", "year"]] = parsed_df["full_id"].apply(split_id)
parsed_df["year"] = parsed_df["year"].astype(int)

print("-"*50)
print("Parsed & Deduplicated Dataframe of Series IDs:")
print(parsed_df)


# ------------------------------
# Build time_series_tables dict
# ------------------------------
time_series_tables = defaultdict(lambda: {"years": set(), "geogLevels": ["state"]})

for _, row in parsed_df.iterrows():
    table_name = row["name"]
    year = str(row["year"])
    try:
        # Add year to the table's set of years
        time_series_tables[table_name]["years"].add(year)
    except KeyError:
        # This shouldn't happen with defaultdict, but just in case
        print(f"[WARNING] Table name '{table_name}' not recognized.")

# Convert sets to sorted lists for JSON submission
time_series_tables = {
    table: {"years": sorted(list(values["years"])), "geogLevels": values["geogLevels"]}
    for table, values in time_series_tables.items()
}

# Preview JSON
print(json.dumps(time_series_tables, indent=2))


# ------------------------- #
# Build the full extract JSON
# ------------------------- #

extract_payload = {
    "timeSeriesTables": time_series_tables,
    "shapefiles": [],  # add shapefiles here if needed
    "description": "Maryland NHGIS extract: parsed IDs",
    "dataFormat": "csv_header",
    "breakdownAndDataTypeLayout": "single_file",
    "timeSeriesTableLayout": "time_by_row_layout"
}

# ------------------------- #
# Submit extract to IPUMS API #
# ------------------------- #
url = "https://api.ipums.org/extracts/?collection=nhgis&version=2"

try:
    response = requests.post(url, headers=headers, json=extract_payload)
    response_json = response.json()
    print("-"*50)
    print("Submission response:")
    print(json.dumps(response_json, indent=2))

    if "detail" in response_json:
        print("-"*50)
        print("Warnings / Errors:")
        for d in response_json["detail"]:
            print(d)

except Exception as e:
    print(f"[ERROR] Submission failed: {e}")
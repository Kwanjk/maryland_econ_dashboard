# The Bureau of Labor Statistics (BLS) API
# Created: 11/17/2025
# Last Updated: 11/17/2025 

# Packages to install if haven't already (only once)
# pip install prettytable
# pip install requests

# Note: BLS API uses JSON dumps
import requests
import json
import prettytable
import csv # Save as a CSV
from datetime import datetime  # Formating dates in a standardized way

import pandas as pd  # Data cleaning
from fredapi import Fred  # Accessing data
import os  # File management (reading and saving)
import yaml  # Load API key from a YAML file for security purposes
import re  # For file naming manipulation
import time  # To buffer API requests
from urllib.error import HTTPError  # Handle API request limit

# ------------------------------- #
# Access BLS API with unique key #
# ------------------------------- #

# Ensures that code is being run in the appropriate directory (which also contains the .yaml file)
os.chdir(os.path.dirname(os.path.realpath(__file__)))
current_directory = os.getcwd()
#print(f"The current working directory is: {current_directory}")

# Load API key from YAML -- key hidden for security reasons
with open("api_keys.yaml", "r") as f:
    keys = yaml.safe_load(f)

BLS_API_KEY = keys["bls_api"]

# Initialize BLS client
#bls = ???(api_key=BLS_API_KEY)

# ------------------------------------ #
# Create time buffers between requests #
# ------------------------------------ #

SLEEP_TIME = 0.5  # seconds between requests
MAX_RETRIES = 3   # number of retries if rate limit hit

# ------------------------------- #
# Helper Function for File Naming #
# ------------------------------- #

# Ensure snake_case and proper naming convention
def to_snake_case(s):
    """
    Convert a string to snake_case suitable for filenames:
    - lowercase
    - spaces and hyphens replaced with underscores
    - remove parentheses, slashes, colons, and other special characters
    - collapse multiple underscores
    """
    s = s.lower()                      # lowercase
    s = re.sub(r"[ /\\\-]", "_", s)    # replace space, /, \, - with _
    s = re.sub(r"[^a-z0-9_]", "", s)   # remove all non-alphanumeric and non-underscore chars
    s = re.sub(r"_+", "_", s)          # collapse multiple underscores
    s = s.strip("_")                    # remove leading/trailing underscores
    return s

# ----------------------------------- #
# Helper Function for Data Formatting #
# ----------------------------------- #

month_lookup = {
    "M01": "January", "M02": "February", "M03": "March", "M04": "April",
    "M05": "May", "M06": "June", "M07": "July", "M08": "August",
    "M09": "September", "M10": "October", "M11": "November", "M12": "December"
}

def make_date(year, period):
    if period.startswith("M"):
        month = int(period[1:])
        return datetime(int(year), month, 1).strftime("%Y-%m-%d")
    return None

# -------------------------------- #
# Loading Data with Series ID Info #
# -------------------------------- #

# Load table with series IDs that is downloaded from Google Drive
file_path = "Indicators Series ID List.xlsx"  # Can be updated if Excel file changes

# County Series IDs
county_sheet = "COUNTY BLS"  # Can be updated if Excel file changes
    # Read  Excel file -- Note: NOT skipping first row, since column headings are in row 0
county_series_id_df = pd.read_excel(file_path, sheet_name=f"{county_sheet}", skiprows=0)

# Preview County Series ID Data
#print(county_series_id_df.head())  # County Series IDs

new_col_names = ['COUNTY', 'SERIES ID']
county_series_id_df.columns = new_col_names
print(county_series_id_df)

# Clean County Names for API Search?


# ------------------------------------- #
# Setting Up Saving and File Management #
# ------------------------------------- #

# Save CSV outputs to an a specific output folder

# Determine base directory (works in both scripts & Jupyter)
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.getcwd()

# Build the full path to the output folder
employment_count_dir = os.path.join(
    script_dir,
    "bls_csv_outputs",
    "county_data",
    "employment_count"
)

# Create all folders if they don't exist
os.makedirs(employment_count_dir, exist_ok=True)

print(f"[INFO] Saving files to: {employment_count_dir}")

# --------------------------------- #
# Running BLS API with CSV Outputs  #
# for County-Level Employment Count #
# --------------------------------- #

# ---- BLS API Request ----
headers = {'Content-type': 'application/json'}

# Different datasets have different start years
# For example:
    # LAU (Local Area Unemployment Statistics) typically goes back to 1990
    # CES (Employment) goes back to 1939
    # CPI goes back to 1913
    
data = {
    "seriesid": ['LAUCN240010000000005', 'LAUCN240030000000005'],
    "startyear": "2011",
    "endyear": "2014"
}

response = requests.post(
    'https://api.bls.gov/publicAPI/v2/timeseries/data/',
    data=json.dumps(data),
    headers=headers
)
json_data = response.json()

# ---- Write CSVs ----
for series in json_data['Results']['series']:

    series_id = series['seriesID']
    filepath = os.path.join(employment_count_dir, f"{series_id}.csv")

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["series_id", "year", "month", "date", "value", "footnotes"])

        for item in series["data"]:
            year = item.get("year")
            period = item["period"]

            if period.startswith("M"):
                month_name = month_lookup.get(period, period)
                date_value = make_date(year, period)

                footnotes = ",".join(
                    fn["text"] for fn in item.get("footnotes", []) if fn
                )

                writer.writerow([
                    series_id,
                    year,
                    month_name,
                    date_value,
                    item.get("value"),
                    footnotes
                ])

    print(f"[INFO] Saved: {filepath}")

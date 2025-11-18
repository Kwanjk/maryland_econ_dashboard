# The Bureau of Labor Statistics (BLS) API
# Written by Lily Gates
# Created: 11/17/2025
# Last Updated: 11/17/2025 

# Packages to install if haven't already (only once)
# pip install prettytable
# pip install requests

# Note: BLS API uses JSON dumps
import requests
import json
import prettytable

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

# Base folder relative to script location (or absolute path if you prefer)
script_dir = os.path.dirname(os.path.abspath(__file__))

# Folder for county-level outputs
county_output_folder = os.path.join(script_dir, "bls_csv_outputs", "county_data")
os.makedirs(county_output_folder, exist_ok=True)
    # Base folder for county data
county_output_base = "bls_csv_outputs/county_data"

print(f"[INFO] County output folder created: {county_output_folder}")


"""
headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['CUUR0000SA0','SUUR0000SA0'],"startyear":"2011", "endyear":"2014"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)
for series in json_data['Results']['series']:
    x=prettytable.PrettyTable(["series id","year","period","value","footnotes"])
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        footnotes=""
        for footnote in item['footnotes']:
            if footnote:
                footnotes = footnotes + footnote['text'] + ','
        if 'M01' <= period <= 'M12':
            x.add_row([seriesId,year,period,value,footnotes[0:-1]])
    output = open(seriesId + '.txt','w')
    output.write (x.get_string())
    output.close()

"""
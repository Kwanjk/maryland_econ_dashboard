# IPUMS NHGIS (National Historical Geographic Information System) API
# Created: 12/2025
# Last Updated: 12/10/2025 

# Things to install if haven't already (only once)
    # Install the Fred API "pip install fredapi"
    # Install openpyxl in order to open .xlsx files in pandas through "pip install openpyxl"
    # Install pyyaml in order to retrieve hidden API saved in a YAML file through "pip install PyYAML"

import pandas as pd  # Data cleaning
from fredapi import Fred  # Accessing data
import os  # File management (reading and saving)
import yaml  # Load API key from a YAML file for security purposes
import re  # For file naming manipulation
import time  # To buffer API requests
from urllib.error import HTTPError  # Handle API request limit

# -------------------------------- #
# Setting up for FRED API Requests #
# -------------------------------- #


#pip install ipumspy
# Using Plotly and Dash to Make Interactive Dashboards
# Used for Maryland County-Level Data

# Created: 11/2025
# Last Updated: 12/3/2025 

# -----------------------------------------------------

# Things to install if haven't already (only once)
    # openpxyl = in order to open .xlsx files in pandas
    # PyYAML = in order to retrieve hidden API saved in a YAML file
    # fredapi = in order to access FRED API
    # plotly = in order to make interactive graphs
    # dash = in order to make interactive dashboards using plotly

# Uncomment if needed
#!pip install openpyxl PyYAML fredapi dash plotly

# -----------------------------------------------------

# ---------------------------------------- #
# Import Libraries for Data & File Handling
# ---------------------------------------- #
import os
import re
from pathlib import Path
from urllib.error import HTTPError

import pandas as pd
from fredapi import Fred
import yaml

# ---------------------------------------- #
# Import Libraries for Dash & Visualization
# ---------------------------------------- #
from dash import Dash, html, dcc, callback
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ------------------------------------------- #
# Ensure Current Working Directory is correct #
# ------------------------------------------- #

#print("\nCurrent Working Directory:\n", os.getcwd(), "\n")

# Base directories
state_base_dir = Path("fred_csv_outputs/state_data")
county_base_dir = Path("fred_csv_outputs/county_data")

"""
# Test existence of state and county directories
print("State-Level:", state_base_dir.exists(), state_base_dir.is_dir())
print("County-Level:", county_base_dir.exists(), county_base_dir.is_dir())
"""

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
county_sheet = "COUNTY FRED"  # Can be updated if Excel file changes
    # Read  Excel file -- Note: Skipping first row, since column headings are merged in row 0
county_series_id_df = pd.read_excel(file_path, sheet_name=f"{county_sheet}", skiprows=1)

# Clean the COUNTY column (for any extra spaces) - must match FRED API County Names
county_series_id_df["COUNTY"] = county_series_id_df["COUNTY"].astype(str).str.strip()

# State of Maryland Series IDs
state_sheet = "MD FRED"  # Can be updated if Excel file changes
    # Read  Excel file
state_series_id_df = pd.read_excel(file_path, sheet_name=f"{state_sheet}")

# ------------------------------------------- #

# Organizing list of counties
county_list = county_series_id_df["COUNTY"].unique().tolist()

# Convert to snake_case using the function
county_list_snake = [to_snake_case(c) for c in county_list]

"""
# Keep uncommented to reduce visual clutter in Terminal view when running
print("="*60)
print("ALL COUNTIES IN MARYLAND")
print("="*60)

#print("Original counties:")
for county in county_list:
    print(county)

#print("-"*60)
#print("Snake-case counties:")
#print(county_list_snake)
"""

# ----------------------------------------------- #
# Create set to store unique COUNTY-LEVEL metrics #
# ----------------------------------------------- #
county_metric_set = set()

for file in county_base_dir.rglob("*.csv"):
    folder_lower = file.parent.name.lower()
    
    # Only include counties in your list
    if folder_lower in county_list_snake:
        # Get filename without extension
        name_only = file.stem  # e.g., "baltimore_city_resident_population"
        
        # Ensure we only remove the **exact folder name prefix**
        prefix = folder_lower + "_"
        if name_only.startswith(prefix):
            metric = name_only[len(prefix):]  # remove the prefix safely
            county_metric_set.add(metric)

# Convert to a sorted list
county_metric_list = sorted(list(county_metric_set))

# ---------------------------------------------------------- #
# Compiling All COUNTY-LEVEL Metrics Sorted by Subject Group #
# ---------------------------------------------------------- #

# --- Define regex patterns for groups ---
group_county_patterns = {
    "housing": re.compile(r"house|housing", re.IGNORECASE),
    "labor": re.compile(r"employ|labor|labour", re.IGNORECASE),
    "economy": re.compile(r"poverty|gdp|population", re.IGNORECASE)
}

# --- Assign metrics to groups ---
grouped_county_metrics = {group: [] for group in group_county_patterns.keys()}

for metric in county_metric_list:
    for group, pattern in group_county_patterns.items():
        if pattern.search(metric):
            grouped_county_metrics[group].append(metric)
            break

# --- Convert to a long-form DataFrame ---
rows = []
for group, metrics in grouped_county_metrics.items():
    for metric in metrics:
        rows.append({"group": group, "metric": metric})

county_metrics_df = pd.DataFrame(rows)

# ----------------------------------------------- #
# Dictionary to store COUNTY-LEVEL files by group #
# ----------------------------------------------- #

# Initialize nested dictionary
county_group_file_dict = {}

# Loop through all counties in county_list_snake
for county in county_list_snake:
    county_dir = county_base_dir / county
    if not county_dir.exists():
        continue  # skip if folder doesn't exist

    # Filter metrics by group
    for group in ["housing", "labor", "economy"]:
        # Get metrics in this group
        metrics_in_group = county_metrics_df[county_metrics_df['group'] == group]['metric'].tolist()
        
        # Find all CSVs in the county folder that match metrics in this group
        matching_files = []
        for file in county_dir.rglob("*.csv"):
            file_name = file.stem.lower()
            # Remove county prefix if present
            prefix = county + "_"
            if file_name.startswith(prefix):
                metric_name = file_name[len(prefix):]
            else:
                metric_name = file_name
            if any(metric.lower() == metric_name for metric in metrics_in_group):
                matching_files.append(file)
        
        # Add to nested dictionary
        county_group_file_dict.setdefault(county, {})[group] = matching_files

# ----------------------------------------------- #
# Display Output: Just Group and Files (no County)
# ----------------------------------------------- #

# Collect unique files for each group across all counties
group_files_only = {group: set() for group in ["housing", "labor", "economy"]}

for county_name, county_groups in county_group_file_dict.items():
    for group, files in county_groups.items():
        for f in files:
            file_name = f.name

            # Remove county prefix + underscore if present
            prefix = county_name + "_"

            if file_name.startswith(prefix):
                file_name = file_name[len(prefix):]
            group_files_only[group].add(file_name)

# --- Output ---

print("="*60)
print("COUNTY-LEVEL METRICS IN MARYLAND")
print("="*60)

#print("County-Level Metrics List:")
#print(county_metric_list)

print("County-Level Metrics DataFrame (grouped):")
print(county_metrics_df)

# Print file names sorted by metric group
for group, files in group_files_only.items():
    print(f"\nGroup: {group}")
    for f in sorted(files):
        print(f"  {f}")

# ----------------------------------------------- #
# Create set to store unique STATE-LEVEL metrics #
# ----------------------------------------------- #
state_metric_set = set()

for file in state_base_dir.rglob("*.csv"):
    # Get filename without extension
    metric = file.stem  # e.g., "resident_population"
    state_metric_set.add(metric)

# Convert to a sorted list
state_metric_list = sorted(list(state_metric_set))
state_metric_list

# ---------------------------------------------------------- #
# Compiling All STATE-LEVEL Metrics Sorted by Subject Group #
# ---------------------------------------------------------- #

# --- Define regex patterns for groups ---
group_state_patterns = {
    "housing": re.compile(r"house|housing|zillow", re.IGNORECASE),
    "labor": re.compile(r"employ|labor|labour|workers|unemployed", re.IGNORECASE),
    "economy": re.compile(r"poverty|gdp|population|income|business", re.IGNORECASE)
}

# --- Assign metrics to groups ---
grouped_state_metrics = {group: [] for group in group_state_patterns.keys()}

for metric in state_metric_list:
    for group, pattern in group_state_patterns.items():
        if pattern.search(metric):
            grouped_state_metrics[group].append(metric)
            break

# --- Convert to a long-form DataFrame ---
rows = []
for group, metrics in grouped_state_metrics.items():
    for metric in metrics:
        rows.append({"group": group, "metric": metric})

state_metrics_df = pd.DataFrame(rows)

# --- Output ---

print("="*60)
print("STATE-LEVEL METRICS FOR MARYLAND")
print("="*60)

#print("State-Level Metrics List:")
#print(state_metric_list)

print("State-Level Metrics DataFrame (grouped):")
print(state_metrics_df)

# ----------------------------------------------- #
# Dictionary to store STATE-LEVEL files by group
# ----------------------------------------------- #

state_group_file_dict = {}

# Loop over each group in metrics_df
for group in ["housing", "labor", "economy"]:
    # List of metrics in this group
    metrics_in_group = county_metrics_df[county_metrics_df['group'] == group]['metric'].tolist()
    
    matching_files = []
    for file in state_base_dir.rglob("*.csv"):
        file_name = file.stem.lower()
        # Check if any metric in this group is in the filename
        if any(metric.lower() in file_name for metric in metrics_in_group):
            matching_files.append(file)
    
    state_group_file_dict[group] = matching_files

# --- Output ---
for group, files in state_group_file_dict.items():
    print(f"\nGroup: {group}")
    for f in files:
        print(f)

##############################################################

# ------------------------------------------------------- #
# Create a mapping for aesthetic "friendly" metric labels #
# ------------------------------------------------------- #

def make_friendly_label(metric_name: str) -> str:
    """
    Convert raw metric name to a readable friendly label.
    Example: 'all_transaction_house_price_index' -> 'All Transaction House Price Index'
    """
    # Replace underscores with spaces, capitalize words
    return metric_name.replace("_", " ").title()

# Create dictionary dynamically from county_metrics_df
metric_label_dict = {m: make_friendly_label(m) for m in county_metrics_df["metric"].unique()}

# ----------------------------------- #
# Get Metrics Data for County + Group #
# ----------------------------------- #

def get_group_data_for_county(county_name_pretty: str, group_name: str) -> pd.DataFrame:
    county_snake = to_snake_case(county_name_pretty)
    county_folder = county_base_dir / county_snake

    if not county_folder.exists():
        raise FileNotFoundError(f"No folder found for county: {county_folder}")

    metrics_in_group = county_metrics_df[county_metrics_df['group'] == group_name]['metric'].tolist()
    frames = []

    for metric in metrics_in_group:
        csv_path = county_folder / f"{county_snake}_{metric}.csv"
        if not csv_path.exists():
            continue

        df = pd.read_csv(csv_path)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # Use friendly label for hover
        df["metric"] = metric_label_dict.get(metric, metric)
        frames.append(df)

    if not frames:
        raise ValueError(f"No {group_name} metrics found for county: {county_name_pretty}")

    return pd.concat(frames, ignore_index=True)

##############################################################

# ---------------------- #
# Create Plotly Dash App #
# ---------------------- #

# Initialize app
app = Dash(__name__)

# Dynamic dropdown options
county_options = sorted(county_list)  # From Excel (pretty names)
group_options = sorted(county_metrics_df["group"].unique())  # From grouped metrics

# App layout
app.layout = html.Div([
    html.H2("Maryland County Metrics"),
    
    html.Div([
        html.Label("Select County:"),
        dcc.Dropdown(
            id="county_dropdown",
            options=[{"label": c, "value": c} for c in county_options],
            value=county_options[0],
            clearable=False,
            style={"width": "300px"}
        ),
    ], style={"margin-bottom": "20px"}),
    
    html.Div([
        html.Label("Select Metric Group:"),
        dcc.Dropdown(
            id="group_dropdown",
            options=[{"label": g.title(), "value": g} for g in group_options],
            value=group_options[0],
            clearable=False,
            style={"width": "300px"}
        ),
    ], style={"margin-bottom": "20px"}),
    
    dcc.Graph(id="metrics_graph")
])

# ------------------------- #
# Function to create figure #
# ------------------------- #

def create_group_figure(df, county_name, group_name):
    metrics = sorted(df["metric"].unique())
    n_rows = len(metrics)

    fig = make_subplots(
        rows=n_rows, cols=1,
        shared_xaxes=True,
        subplot_titles=metrics,
        vertical_spacing=0.06
    )

    for i, m in enumerate(metrics, start=1):
        df_m = df[df["metric"] == m]
        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=df_m["value"],
                mode="lines+markers",
                name=m,
                showlegend=False,
                marker=dict(size=6),
                hovertemplate="<b>%{fullData.name}</b><br>Date: %{x|%Y-%m-%d}<br>Value: %{y:.2f}<extra></extra>"
            ),
            row=i, col=1
        )
        fig.update_yaxes(title_text="Value", row=i, col=1)

    fig.update_xaxes(title_text="Date", row=n_rows, col=1)
    fig.update_layout(
        height=250*n_rows,
        title_text=f"{county_name} – {group_name.title()} Metrics Over Time"
    )
    return fig

# ---------------------------------------- #
# Single callback (DRY)
# ---------------------------------------- #
@app.callback(
    Output("metrics_graph", "figure"),
    Input("county_dropdown", "value"),
    Input("group_dropdown", "value")
)
def update_metrics_graph(county_name_pretty, group_name):
    # Fetch data dynamically
    df = get_group_data_for_county(county_name_pretty, group_name)
    return create_group_figure(df, county_name_pretty, group_name)

# ---------------------------------------- #
# Run app
# ---------------------------------------- #
if __name__ == "__main__":
    app.run_server(debug=True)
    
"""

TODO Not sure if this is necessary

# ---------------------------------------------------------- #
# Dash callback already works with friendly labels
# ---------------------------------------------------------- #

@callback(
    Output("metrics_graph", "figure"),
    Input("county_dropdown", "value"),
    Input("group_dropdown", "value")
)
def update_metrics_graph(county_name_pretty, group_name):
    df = get_group_data_for_county(county_name_pretty, group_name)
    fig = create_group_figure(df, county_name_pretty)
    return fig



##############################################################

# ---------------------------------------- #
# Create Dash App
# ---------------------------------------- #
app = Dash(__name__)

# Dynamic dropdown options
county_options = sorted(county_list)  # Pretty names from Excel
group_options = sorted(county_metrics_df["group"].unique())  # All groups dynamically

app.layout = html.Div([
    html.H2("Maryland County Metrics"),
    html.Div([
        html.Label("Select County:"),
        dcc.Dropdown(
            id="county_dropdown",
            options=[{"label": c, "value": c} for c in county_options],
            value=county_options[0],
            clearable=False,
            style={"width": "300px"}
        ),
    ]),
    html.Div([
        html.Label("Select Metric Group:"),
        dcc.Dropdown(
            id="group_dropdown",
            options=[{"label": g.title(), "value": g} for g in group_options],
            value=group_options[0],
            clearable=False,
            style={"width": "300px"}
        ),
    ]),
    dcc.Graph(id="metrics_graph")
])

# ---------------------------------------- #
# Function to create Plotly figure
# ---------------------------------------- #
def create_group_figure(df, county_name, group_name):
    metrics = sorted(df["metric"].unique())
    n_rows = len(metrics)

    fig = make_subplots(
        rows=n_rows, cols=1, shared_xaxes=True,
        subplot_titles=metrics, vertical_spacing=0.06
    )

    for i, m in enumerate(metrics, start=1):
        df_m = df[df["metric"] == m]
        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=df_m["value"],
                mode="lines+markers",
                name=m,
                showlegend=False,
                marker=dict(size=6),
                hovertemplate="<b>%{fullData.name}</b><br>Date: %{x|%Y-%m-%d}<br>Value: %{y:.2f}<extra></extra>"
            ),
            row=i, col=1
        )
        fig.update_yaxes(title_text="Value", row=i, col=1)

    fig.update_xaxes(title_text="Date", row=n_rows, col=1)
    fig.update_layout(
        height=250*n_rows,
        title_text=f"{county_name} – {group_name.title()} Metrics Over Time"
    )
    return fig

# ---------------------------------------- #
# Callback to update figure based on dropdown
# ---------------------------------------- #
@app.callback(
    Output("metrics_graph", "figure"),
    Input("county_dropdown", "value"),
    Input("group_dropdown", "value")
)
def update_metrics_graph(county_name_pretty, group_name):
    # Dynamically fetch metrics for selected county and group
    df = get_group_data_for_county(county_name_pretty, group_name)
    return create_group_figure(df, county_name_pretty, group_name)

# ---------------------------------------- #
# Run app
# ---------------------------------------- #
if __name__ == "__main__":
    app.run_server(debug=True)


######################################################
# ------------ GRAPHING SPECIFIC GRAPHS ------------ #
# ----- HOUSING Metrics for Maryland Counties ------ #
######################################################


HOUSING_METRICS = {
    "all_transaction_house_price_index": "All-Transaction House Price Index",
    "new_private_housing_units_authorized_by_building_permits_count": "New Private Housing Units (Permits)",
    "housing_inventory_active_listing_count": "Active Listings",
    "housing_inventory_median_listing_price_dollars": "Median Listing Price ($)",
}

def get_housing_data_for_county(county_name_pretty: str) -> pd.DataFrame:
    
    county_snake = to_snake_case(county_name_pretty)
    county_folder = county_base_dir / county_snake

    if not county_folder.exists():
        raise FileNotFoundError(f"No folder found for county: {county_folder}")

    frames = []

    for stem, label in HOUSING_METRICS.items():
        csv_path = county_folder / f"{county_snake}_{stem}.csv"
        if not csv_path.exists():
            
            continue

        df = pd.read_csv(csv_path)

       
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        df["metric"] = label
        frames.append(df)

    if not frames:
        raise ValueError(f"No housing metrics found for county: {county_name_pretty}")

    return pd.concat(frames, ignore_index=True)



def plot_housing_for_county_separate(county_name_pretty: str):
    
    df = get_housing_data_for_county(county_name_pretty)

   
    metrics = sorted(df["metric"].unique())
    n_rows = len(metrics)

    fig = make_subplots(
        rows=n_rows,
        cols=1,
        shared_xaxes=True,
        subplot_titles=metrics,   
        vertical_spacing=0.06,
    )

    for i, m in enumerate(metrics, start=1):
        df_m = df[df["metric"] == m]

        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=df_m["value"],
                mode="lines+markers",
                name=m,
                showlegend=False,
                marker=dict(size=6),
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>"
                    "Date: %{x|%Y-%m-%d}<br>"
                    "Value: %{y:.2f}<extra></extra>"
                ),
            ),
            row=i,
            col=1,
        )

        fig.update_yaxes(title_text="Value", row=i, col=1)

    fig.update_xaxes(title_text="Date", row=n_rows, col=1)

    fig.update_layout(
        height=250 * n_rows,
        title_text=f"Housing Indicators Over Time – {county_name_pretty}",
    )

    fig.show()

county_options = [
    "Allegany", "Anne Arundel", "Baltimore", "Baltimore City",
    "Calvert", "Caroline", "Carroll", "Cecil", "Charles",
    "Dorchester", "Frederick", "Garrett", "Harford", "Howard",
    "Kent", "Montgomery", "Prince George's", "Queen Anne's",
    "Somerset", "St. Mary's", "Talbot", "Washington",
    "Wicomico", "Worcester",
]

@interact(county=Dropdown(options=county_options, description="County:"))
def show_housing(county):
    plot_housing_for_county_separate(county)

######################################################
# ------------ GRAPHING SPECIFIC GRAPHS ------------ #
# ----- ECONOMIC Metrics for Maryland Counties ----- #
######################################################



######################################################
# ------------ GRAPHING SPECIFIC GRAPHS ------------ #
# ------ LABOR Metrics for Maryland Counties ------- #
######################################################

"""
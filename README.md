# Maryland Economics Data Dashboard

**Written by:** Donasyl Aho, Zainab Ahmadi, Max Eliker, Lily Gates, Joshua Kwan, and Ansh Rekhi  
_University of Maryland, College Park_

---

## Description
In collaboration with the National Center for Smart Growth Research and Education (NCSG) at the University of Maryland, College Park, this project provides a dashboard and CSV outputs of economic indicators for Maryland counties and the state.  

This project fetches economic data for Maryland counties and the state of Maryland using the Federal Reserve Economic Data (FRED) API.  The [FRED API](https://fred.stlouisfed.org/) includes metrics such as population, housing, employment, GDP, and more.

The output is a set of CSV files containing historical observations for various economic indicators, including population, housing, labor market, and GDP metrics.

Data is organized as:
* **County-level data**: Separate folders for each county in `csv_outputs/county_data/`. Each file is named in `snake_case` format based on county and series name.
* **State-level data**: Stored in `csv_outputs/state_data/`. Each CSV corresponds to a specific state-level economic series.

The script handles:

* Retrieving series metadata (title, source, frequency, observation start/end)
* Fetching historical series data from FRED
* Exponential backoff and retries to handle API rate limits
* Consistent, safe CSV file naming and folder structure

---

## Output
The script generates:
* **County-level CSVs** for each Maryland county with observations from FRED.
    - Saved as: `csv_outputs/county_data/{county_name}/{county}_{series_name}.csv`
* **State-level CSVs** for Maryland.
    - Saved as: `csv_outputs/state_data/{series_name}.csv`
* Each CSV includes two columns:  
  - `date` — the observation date  
  - `value` — the corresponding indicator value

All CSVs are saved in `csv_outputs/county_data` or `csv_outputs/state_data` folders using snake_case filenames (e.g., `montgomery_resident_population.csv`).

---

## Usage
1. Install required dependencies (see below).  
2. Place your FRED API key in a YAML file called `api_keys.yaml`:
```yaml
fred_api: your_api_key_here
```
4. Place the Excel file `Indicators Series ID List.xlsx` in the same folder as the script.
5. Run the main Python script:
```python your_script_name.py```
6. The script will create CSV outputs for both county- and state-level data:
    * County data: csv_outputs/county_data/{county_name}/{county}_{series_name}.csv
    * State data: csv_outputs/state_data/{series_name}.csv
7. Check the console for [INFO] messages showing the relative path of saved files.

---

## Output Structure
```
csv_outputs/
├── county_data/
│   ├── montgomery/
│   │   ├── montgomery_resident_population.csv
│   │   ├── montgomery_unemployed_rate.csv
│   │   └── ...
│   └── prince_georges/
│       └── ...
└── state_data/
    ├── resident_population.csv
    ├── median_household_income.csv
    └── ...
```


Each CSV contains:
* `date` – observation date
* `value` – observed value for the series

## Required Dependencies
This project requires the following Python libraries:
<<<<<<< HEAD
* `pandas` – for data manipulation
* `fredapi` – for accessing the FRED API
* `pyyaml` – for loading API keys securely
* `openpyxl` – for reading .xlsx files
* `os` (standard library) – for file management
* `re` (standard library) – for safe file naming
* `time` (standard library) – for handling delays and retries
* `urllib.error` (standard library) – for handling HTTP errors


---

## Error Handling & Rate Limits
* If the FRED API returns a rate limit error (HTTP 429), the script will wait and retry up to 5 times per series.
* Any series that cannot be fetched will be skipped, and a warning will be printed:
```
[WARN] Could not fetch series {series_id}. Skipping.
```
---

## Future Seteps
* Add caching to prevent repeated API calls for unchanged series.
* Implement interactive dashboards or visualizations for county and state data.
* Include additional economic indicators as new FRED series become available.
* Add logging for API errors, retries, and skipped series.
* Handle partial data fetch resumption automatically if interrupted.
* Incorporate automated scheduling to update data regularly.
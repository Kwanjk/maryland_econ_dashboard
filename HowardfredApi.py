# FRED API Automation Script
# Howard County Housing Example

from fredapi import Fred
import pandas as pd
from datetime import datetime

# Connect to FRED 
fred = Fred(api_key="2ccf5b794d310f8cde1d30c463f8d2d4") # Your FRED API key here 

# Step 2: Choose Indicator Codes
# You can find these codes at https://fred.stlouisfed.org/
# Example: "MEDLISPRIHOWA" is a placeholder — use the actual series ID for Howard County median listing price
series_id = "MEDLISPRIHOWA"  

# Step 3: Fetch Data 
data = fred.get_series(series_id)

# Step 4: Convert to DataFrame 
df = pd.DataFrame(data, columns=["Median_Listing_Price"])
df.index.name = "Date"
df.reset_index(inplace=True)

# Step 5: Format & Save 
df["Date"] = pd.to_datetime(df["Date"])
df.sort_values("Date", inplace=True)
df.to_csv("Housing_Howard_MedianPrice.csv", index=False)

print("✅ Data saved successfully as 'Housing_Howard_MedianPrice.csv'")
print(df.tail())
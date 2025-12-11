import pandas as pd
import glob
import json
import plotly.express as px

import requests

url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
counties = requests.get(url).json()



files = glob.glob("*_all_metrics.csv")

all_rows = []

for f in files:
    county = f.replace("_all_metrics.csv", "")
    df = pd.read_csv(f)
    
   
    latest = df.iloc[-1].copy()
    latest["County"] = county
    all_rows.append(latest)

md = pd.DataFrame(all_rows)


county_to_fips = {
    "allegany": "24001",
    "anne_arundel": "24003",
    "baltimore": "24005",
    "baltimore_city": "24510",
    "calvert": "24009",
    "caroline": "24011",
    "carroll": "24013",
    "cecil": "24015",
    "charles": "24017",
    "dorchester": "24019",
    "frederick": "24021",
    "garrett": "24023",
    "harford": "24025",
    "howard": "24027",
    "kent": "24029",
    "montgomery": "24031",
    "prince_georges": "24033",
    "queen_annes": "24035",
    "somerset": "24039",
    "st_marys": "24037",
    "talbot": "24041",
    "washington": "24043",
    "wicomico": "24045",
    "worcester": "24047"
}

md["fips"] = md["County"].map(county_to_fips)

md

def make_md_map(metric, colorscale):
    fig = px.choropleth_mapbox(
        md,
        geojson=counties,
        locations="fips",
        color=metric,
        color_continuous_scale=colorscale,
        mapbox_style="open-street-map",   
        zoom=6.7,
        center={"lat": 39.0, "lon": -76.7},
        hover_name="County",
        hover_data={metric: ':.2f'},
        labels={metric: metric.replace('_', ' ').title()},
        opacity=0.8,                      
    )

    fig.update_layout(
        title=f"Maryland County {metric.replace('_', ' ').title()}",
        margin={"r":0, "t":40, "l":0, "b":0}
    )
    fig.show()

metric_colors = {
    "Employment": "Blues",
    "Labor Force": "Greens",
    "Unemployment Count": "Reds",
    "Unemployment Rate": "Purples"
}

for metric, colorscale in metric_colors.items():
    make_md_map(metric, colorscale)
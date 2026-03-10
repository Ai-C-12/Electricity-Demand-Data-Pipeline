import os
import requests
import pandas as pd

api_key = os.environ["EIA_API_KEY"]

url = "https://api.eia.gov/v2/electricity/rto/region-data/data/?api_key=1hhUNnaSmhjlKCDFUYWfGnhNIryAWHX6EYcWQJgu&frequency=hourly&data[]=value&facets[respondent][]=%3CNYIS%3E&facets[type][]=D&start=2026-02-20T00&end=2026-02-21T23&sort[0][column]=period&sort[0][direction]=asc&length=10"
params = {
    "api_key": api_key,
    "frequency": "hourly",
    "data[]": "value",
    "facets[respondent][]": "NYIS",
    "facets[type][]": "D",
    "start": "2026-02-20T00",
    "end": "2026-02-21T23",
    "length": 5000,
}

r = requests.get(url, params=params, timeout=30)
r.raise_for_status()
j = r.json()

# print(j.keys())
# print("has response:", "response" in j)
# print("total:", j.get("response", {}).get("total"))
# print("rows:", len(j.get("response", {}).get("data", [])))

# EIA sometimes nests rows under response->data; handle both cases:
rows = (j.get("response") or {}).get("data") or j.get("data")
eia_df = pd.DataFrame(rows)

# EIA commonly returns numeric values as strings → parse when present
if "value" in eia_df.columns:
    eia_df["value"] = pd.to_numeric(eia_df["value"], errors="coerce")

eia_df["period"] = pd.to_datetime(eia_df["period"], format="%Y-%m-%dT%H", utc=True)

# print(eia_df["period"].min(), eia_df["period"].max())
print(eia_df.shape)
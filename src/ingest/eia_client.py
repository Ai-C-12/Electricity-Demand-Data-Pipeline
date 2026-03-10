import os
import requests
import pandas as pd


BASE_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"


def fetch_eia_data(
    respondent: str,
    data_type: str,
    start: str,
    end: str,
    length: int = 5000,
) -> pd.DataFrame:
    api_key = os.environ["EIA_API_KEY"]

    params = {
        "api_key": api_key,
        "frequency": "hourly",
        "data[]": "value",
        "facets[respondent][]": respondent,
        "facets[type][]": data_type,
        "start": start,
        "end": end,
        "length": length,
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    rows = (payload.get("response") or {}).get("data") or payload.get("data") or []
    df = pd.DataFrame(rows)

    if df.empty:
        return df

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"], format="%Y-%m-%dT%H", utc=True)

    return df

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
) -> tuple[pd.DataFrame, dict, dict]:
    api_key = os.environ["EIA_API_KEY"]

    all_rows = []
    page_payload = []
    page_request_meta = []

    offset = 0
    page_size = length
    page_number = 0

    if page_size > 5000:
        raise ValueError("EIA page size cannot exceed 5000.")

    while True:
        params = {
            "api_key": api_key,
            "frequency": "hourly",
            "data[]": "value",
            "facets[respondent][]": respondent,
            "facets[type][]": data_type,
            "start": start,
            "end": end,
            "length": page_size,
            "offset": offset,
        }

        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        rows = (payload.get("response") or {}).get("data") or payload.get("data") or []

        all_rows.extend(rows)

        page_payload.append({
            "page_number": page_number,
            "offset": offset,
            "length": page_size,
            "rows_returned": len(rows),
            "payload": payload,
        })

        page_request_meta.append({
            "page_number": page_number,
            "offset": offset,
            "length": page_size,
            "rows_returned": len(rows),
        })

        if len(rows) < page_size:
            break

        offset += page_size
        page_number += 1

    raw_payload = {
        "pages": page_payload,
    }

    request_meta = {
        "source": "eia",
        "frequency": "hourly",
        "respondent": respondent,
        "type": data_type,
        "start": start,
        "end": end,
        "page_size": page_size,
        "total_rows": len(all_rows),
        "pages": page_request_meta
    }

    df = pd.DataFrame(all_rows)

    if df.empty:
        return df, raw_payload, request_meta

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if "period" in df.columns:
        df["period"] = pd.to_datetime(df["period"], format="%Y-%m-%dT%H", utc=True)

    return df, raw_payload, request_meta
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from src.config import (
    HISTORICAL_WEATHER_API_URL,
    FORECAST_WEATHER_API_URL,
    WEATHER_LATITUDE,
    WEATHER_LONGITUDE,
)


def _fetch_weather_data(
    url: str,
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    hourly_variable: str = "temperature_2m",
    model: str | None = None,
) -> tuple[pd.DataFrame, dict, dict]:
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": hourly_variable,
        "timezone": "UTC",
    }

    if model is not None:
        params["models"] = model

    request_meta = {
        "source": "openmeteo",
        "endpoint": url,
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly_variable": hourly_variable,
        "model": model,
        "timezone": "UTC",
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    raw_response = retry_session.get(url, params=params, timeout=30)
    raw_response.raise_for_status()
    payload = raw_response.json()

    hourly = response.Hourly()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }

    hourly_data[hourly_variable] = hourly.Variables(0).ValuesAsNumpy()

    df = pd.DataFrame(hourly_data)

    if df.empty:
        return df, payload, request_meta

    df["latitude"] = latitude
    df["longitude"] = longitude

    return df, payload, request_meta

def fetch_historical_weather(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, dict, dict]:
    
    return _fetch_weather_data(
        url = HISTORICAL_WEATHER_API_URL,
        latitude = latitude,
        longitude = longitude,
        start_date = start_date,
        end_date = end_date,
        hourly_variable = "temperature_2m",
    )

def fetch_archived_forecast_weather(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, dict, dict]:
    
    return _fetch_weather_data(
        url = FORECAST_WEATHER_API_URL,
        latitude = latitude,
        longitude = longitude,
        start_date = start_date,
        end_date = end_date,
        hourly_variable = "temperature_2m_previous_day1",
        model = "gfs_seamless",
    )
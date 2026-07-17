import pandas as pd

def transform_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    return transform_historical_weather(df)


def transform_historical_weather(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required_columns = ["date", "temperature_2m", "latitude", "longitude"]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    df = df.rename(columns={"date": "timestamp_utc"})

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df["temperature_2m"] = pd.to_numeric(df["temperature_2m"], errors="coerce")

    df = df.dropna(subset=["timestamp_utc", "temperature_2m"])
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    df = df[
        [
            "timestamp_utc",
            "temperature_2m",
            "latitude",
            "longitude",
        ]
    ]

    return df


def transform_archived_forecast_weather(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required_columns = ["date", "temperature_2m_previous_day1", "latitude", "longitude"]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    df = df.rename(columns={"date": "timestamp_utc", "temperature_2m_previous_day1": "temperature_forecast_24h"})

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df["temperature_forecast_24h"] = pd.to_numeric(df["temperature_forecast_24h"], errors="coerce")

    df = df.dropna(subset=["timestamp_utc", "temperature_forecast_24h"])
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    df = df[
        [
            "timestamp_utc",
            "temperature_forecast_24h",
            "latitude",
            "longitude",
        ]
    ]

    return df
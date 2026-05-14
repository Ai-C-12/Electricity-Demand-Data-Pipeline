import pandas as pd

def transform_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required_columns = ["date", "temperature_2m", "longitude", "latitude"]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    df = df.rename(columns={"date": "timestamp_utc"})

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df["temperature_2m"] = pd.to_numeric(df["temperature_2m"], errors="coerce")

    df = df.dropna(subset=["timestamp_utc", "temperature_2m"])

    df = df[
        [
            "timestamp_utc",
            "latitude",
            "longitude",
            "temperature_2m",
        ]
    ]

    return df
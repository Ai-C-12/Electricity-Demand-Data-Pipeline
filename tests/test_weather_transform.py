import pandas as pd

from src.transform.weather_transform import transform_weather_data

def test_transform_weather_data_renames_and_converts_columns():
    raw_df = pd.DataFrame({
        "date": ["2025-01-01T00:00"],
        "temperature_2m": ["4.5"],
        "latitude": [40.7128],
        "longitude": [-74.0060],
    })

    result = transform_weather_data(raw_df)

    assert list(result.columns) == [
        "timestamp_utc",
        "latitude",
        "longitude",
        "temperature_2m",
    ]

    assert pd.api.types.is_datetime64_any_dtype(result["timestamp_utc"])
    assert pd.api.types.is_numeric_dtype(result["temperature_2m"])

    assert result["latitude"].iloc[0] == 40.7128
    assert result["longitude"].iloc[0] == -74.0060
    assert result["temperature_2m"].iloc[0] == 4.5
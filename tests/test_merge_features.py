import pandas as pd

from src.transform.merge_features import merge_df
from src.transform.eia_transform import transform_eia_data
from src.transform.weather_transform import transform_weather_data

def test_merge_df_combines_demand_and_weather_and_adds_time_features():
    demand_df = pd.DataFrame({
        "timestamp_utc": pd.to_datetime([
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
        ]),
        "region": ["NYIS", "NYIS"],
        "demand_mwh": [18000, 18500],
    })

    weather_df = pd.DataFrame({
        "timestamp_utc": pd.to_datetime([
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
        ]),
        "temperature_2m": [2.5, 3.0],
    })

    result = merge_df(demand_df, weather_df)

    assert list(result.columns) == [
        "timestamp_utc",
        "region",
        "demand_mwh",
        "temperature_2m",
        "hour",
        "day_of_week",
        "month",
    ]

    assert len(result) == 2

    assert pd.api.types.is_datetime64_any_dtype(result["timestamp_utc"])
    assert pd.api.types.is_numeric_dtype(result["demand_mwh"])
    assert pd.api.types.is_numeric_dtype(result["temperature_2m"])

    assert result["hour"].tolist() == [0, 1]
    assert result["day_of_week"].tolist() == [2, 2]  # Wednesday
    assert result["month"].tolist() == [1, 1]  # January

    assert result["region"].iloc[0] == "NYIS"
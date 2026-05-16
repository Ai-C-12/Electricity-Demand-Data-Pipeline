import pandas as pd

from src.transform.eia_transform import transform_eia_data

def test_transform_eia_data_renames_and_converts_columns():
    raw_df = pd.DataFrame({
        "period": ["2025-01-01T00"],
        "respondent": ["NYIS"],
        "type": ["D"],
        "value": ["12345"],
    })

    result = transform_eia_data(raw_df)

    assert list(result.columns) == [
        "timestamp_utc",
        "region",
        "type",
        "demand_mwh",
    ]

    assert result["region"].iloc[0] == "NYIS"
    assert result["type"].iloc[0] == "D"
    assert result["demand_mwh"].iloc[0] == 12345

    assert pd.api.types.is_datetime64_any_dtype(result["timestamp_utc"])
    assert pd.api.types.is_numeric_dtype(result["demand_mwh"])
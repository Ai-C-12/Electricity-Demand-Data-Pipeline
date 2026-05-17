import pandas as pd
import pytest

from src.validation.checks import (
    check_not_empty,
    check_required_columns,
    check_no_missing_values,
    check_timestamp_format,
    check_demand_values,
    check_temperature_values,
    check_duplicate_timestamps_region,
    check_merge_retention,
    check_hourly_timestamp_coverage,
)


def make_valid_feature_df() -> pd.DataFrame:
    return pd.DataFrame({
        "timestamp_utc": pd.to_datetime([
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
        ]),
        "region": ["NYIS", "NYIS"],
        "demand_mwh": [18000, 18500],
        "temperature_2m": [2.5, 3.0],
    })


def test_validation_checks_pass_on_valid_dataframe():
    df = make_valid_feature_df()
    dataset_name = "Test feature dataset"

    check_not_empty(df, dataset_name)
    check_required_columns(
        df,
        ["timestamp_utc", "region", "demand_mwh", "temperature_2m"],
        dataset_name,
    )
    check_no_missing_values(df, dataset_name)
    check_timestamp_format(df, dataset_name)
    check_demand_values(df, dataset_name)
    check_temperature_values(df, dataset_name)
    check_duplicate_timestamps_region(df, dataset_name)


def test_check_not_empty_raises_on_empty_dataframe():
    df = pd.DataFrame()

    with pytest.raises(ValueError):
        check_not_empty(df, "Empty dataset")


def test_check_required_columns_raises_on_missing_columns():
    df = make_valid_feature_df().drop(columns=["demand_mwh"])

    with pytest.raises(ValueError):
        check_required_columns(
            df,
            ["timestamp_utc", "region", "demand_mwh", "temperature_2m"],
            "Test Feature Dataset",
        )


def test_check_no_missing_values_raises_on_missing_values():
    df = make_valid_feature_df()
    df.loc[0, "demand_mwh"] = None

    with pytest.raises(ValueError):
        check_no_missing_values(df, "Test Feature Dataset")


def test_check_timestamp_format_raises_on_non_datetime():
    df = make_valid_feature_df()
    df["timestamp_utc"] = df["timestamp_utc"].astype(str)

    with pytest.raises(ValueError):
        check_timestamp_format(df, "Test Feature Dataset")


# Negative Value Check
def test_check_demand_values_raises_on_negative_values():
    df = make_valid_feature_df()
    df.loc[0, "demand_mwh"] = -100

    with pytest.raises(ValueError):
        check_demand_values(df, "Test Feature Dataset")


# Non-numeric Value Check
def test_check_demand_values_raises_on_non_numeric():
    df = make_valid_feature_df()
    df["demand_mwh"] = ["high", "low"]

    with pytest.raises(ValueError):
        check_demand_values(df, "Test Feature Dataset")


def test_check_temperature_values_raises_on_non_numeric():
    df = make_valid_feature_df()
    df["temperature_2m"] = ["cold", "warm"]

    with pytest.raises(ValueError):
        check_temperature_values(df, "Test Feature Dataset")


def test_check_duplicate_timestamps_region_raises_on_duplicates():
    df = make_valid_feature_df()
    df.loc[1, "timestamp_utc"] = df.loc[0, "timestamp_utc"]
    df.loc[1, "region"] = df.loc[0, "region"]

    with pytest.raises(ValueError):
        check_duplicate_timestamps_region(df, "Test Feature Dataset")


# Checks for high retention
def test_check_merge_retention_when_retention_is_high():
    demand_df = make_valid_feature_df()
    weather_df = make_valid_feature_df()
    merged_df = make_valid_feature_df()

    check_merge_retention(
        merged_df=merged_df,
        demand_df=demand_df,
        weather_df=weather_df,
        dataset_name="Test Feature Dataset",
    )


# Checks for low retention
def test_check_merge_retention_raises_when_retention_is_low():
    demand_df = make_valid_feature_df()
    weather_df = make_valid_feature_df()
    merged_df = make_valid_feature_df().iloc[:1]  # Only 1 row retained

    with pytest.raises(ValueError):
        check_merge_retention(
            merged_df=merged_df,
            demand_df=demand_df,
            weather_df=weather_df,
            dataset_name="Test Feature Dataset",
            min_retention=0.99,
        )


def test_check_hourly_timestamp_coverage_passes_for_continuous_hours():
    df = make_valid_feature_df()

    check_hourly_timestamp_coverage(df, "Test Feature Dataset")


def test_check_hourly_timestamp_coverage_raises_for_missing_hours():
    df = pd.DataFrame({
        "timestamp_utc": pd.to_datetime([
            "2025-01-01T00:00:00Z",
            "2025-01-01T02:00:00Z",
        ]),
        "region": ["NYIS", "NYIS"],
        "demand_mwh": [18000, 19000],
        "temperature_2m": [2.5, 3.0],
    })

    with pytest.raises(ValueError):
        check_hourly_timestamp_coverage(df, "Test Feature Dataset")
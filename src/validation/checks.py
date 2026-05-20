import pandas as pd

def check_not_empty(df: pd.DataFrame, dataset_name: str) -> None:
    if df.empty:
        raise ValueError(f"{dataset_name} is empty.")


def check_required_columns(df: pd.DataFrame, required_columns: list[str], dataset_name: str,) -> None:
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"{dataset_name} is missing required columns: {missing_columns}")
    

def check_no_missing_values(df: pd.DataFrame, dataset_name: str) -> None:
    if df.isnull().sum().sum() > 0:
        raise ValueError(f"{dataset_name} contains {df.isnull().sum().sum()} missing values.")
    

def check_timestamp_format(df: pd.DataFrame, dataset_name: str) -> None:
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp_utc"]):
        raise ValueError(f"{dataset_name}'s timestamp_utc column must be in datetime format.")


def check_demand_values(df: pd.DataFrame, dataset_name: str) -> None:
    if not pd.api.types.is_numeric_dtype(df["demand_mwh"]):
        raise ValueError(f"{dataset_name}'s demand_mwh column must be numeric.")

    if (df["demand_mwh"] < 0).any():
        raise ValueError(f"{dataset_name} contains negative values in demand_mwh column.")
    

def check_temperature_values(df: pd.DataFrame, dataset_name: str) -> None:
    if not pd.api.types.is_numeric_dtype(df["temperature_2m"]):
        raise ValueError(f"{dataset_name}'s temperature_2m column must be numeric.")
    

def check_duplicate_timestamps_region(df: pd.DataFrame, dataset_name: str) -> None:
    if df.duplicated(subset=["timestamp_utc", "region"]).any():
        raise ValueError(f"{dataset_name} contains duplicate timestamp-region pairs.")
    

def check_merge_retention(
    merged_df: pd.DataFrame,
    demand_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    dataset_name: str,
    min_retention: float = 0.99,
) -> None:
    expected_rows = min(len(demand_df), len(weather_df))

    if expected_rows == 0:
        raise ValueError("One of the datasets is empty, cannot perform merge retention check.")
    
    retention_rate = len(merged_df) / expected_rows

    if retention_rate < min_retention:
        raise ValueError(
            f"{dataset_name}: merge retention too low. "
            f"Expected at least {min_retention:.2%}, got {retention_rate:.2%}. "
            f"Demand rows: {len(demand_df)}, weather rows: {len(weather_df)}, merged rows: {len(merged_df)}."
        )


def check_hourly_timestamp_coverage(
    df: pd.DataFrame,
    dataset_name: str,
    timestamp_col: str = "timestamp_utc",
    allowed_missing_hours = 0,
) -> None:
    if timestamp_col not in df.columns:
        raise ValueError(f"{dataset_name}: missing required timestamp column '{timestamp_col}'.")

    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        raise ValueError(f"{dataset_name}: '{timestamp_col}' must be a datetime column.")

    timestamps = df[timestamp_col].dropna().drop_duplicates().sort_values()

    if timestamps.empty:
        raise ValueError(f"{dataset_name}: no timestamps available for coverage check.")

    expected_timestamps = pd.date_range(
        start=timestamps.min(),
        end=timestamps.max(),
        freq="h",
    )

    missing_timestamps = expected_timestamps.difference(pd.DatetimeIndex(timestamps))

    if len(missing_timestamps) > allowed_missing_hours:
        raise ValueError(
            f"{dataset_name}: missing {len(missing_timestamps)} hourly timestamps "
            f"between {timestamps.min()} and {timestamps.max()}."
            f"Allowed missing hours: {allowed_missing_hours}"
        )
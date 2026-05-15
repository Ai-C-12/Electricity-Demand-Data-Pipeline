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
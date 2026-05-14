import pandas as pd

def transform_eia_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required_columns = ["period", "respondent", "type", "value"]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    df = df.rename(columns={"period": "timestamp_utc"})
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    df = df.rename(columns={"value": "demand_mwh"})
    df["demand_mwh"] = pd.to_numeric(df["demand_mwh"], errors="coerce")

    df = df[
        [
            "timestamp_utc",
            "respondent",
            "type",
            "demand_mwh",
        ]
    ]

    return df
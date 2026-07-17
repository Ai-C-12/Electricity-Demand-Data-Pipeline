import pandas as pd

def add_demand_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    df["demand_lag_168"] = df["demand_mwh"].shift(168)

    return df
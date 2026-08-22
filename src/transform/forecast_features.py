import pandas as pd
import numpy as np

from sklearn.preprocessing import OneHotEncoder

def add_demand_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    df["demand_lag_168"] = df["demand_mwh"].shift(168)

    return df

def add_calendar_encoding(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    day_of_week = [[0, 1, 2, 3, 4, 5, 6]]
    
    encoder = OneHotEncoder(categories=day_of_week, sparse_output=False)
    encoder.set_output(transform="pandas")
    
    train_df_encoded = train_df.copy()
    validation_df_encoded = validation_df.copy()
    
    # Encoding Days of the Week
    train_days_encoded = encoder.fit_transform(train_df[["day_of_week"]])
    validation_days_encoded = encoder.transform(validation_df[["day_of_week"]])
    
    train_df_encoded = pd.concat([train_df_encoded, train_days_encoded], axis=1)
    validation_df_encoded = pd.concat([validation_df_encoded, validation_days_encoded], axis=1)
    
    
    # Encoding Hours
    train_df_encoded["hour_sin"] = np.sin(2 * np.pi * train_df_encoded["hour"] / 24)
    train_df_encoded["hour_cos"] = np.cos(2 * np.pi * train_df_encoded["hour"] / 24)
    
    validation_df_encoded["hour_sin"] = np.sin(2 * np.pi * validation_df_encoded["hour"] / 24)
    validation_df_encoded["hour_cos"] = np.cos(2 * np.pi * validation_df_encoded["hour"] / 24)
    
    
    # Encoding Months
    train_df_encoded["month_sin"] = np.sin(2 * np.pi * train_df_encoded["month"] / 12)
    train_df_encoded["month_cos"] = np.cos(2 * np.pi * train_df_encoded["month"] / 12)
    
    validation_df_encoded["month_sin"] = np.sin(2 * np.pi * validation_df_encoded["month"] / 12)
    validation_df_encoded["month_cos"] = np.cos(2 * np.pi * validation_df_encoded["month"] / 12)
        
    return train_df_encoded, validation_df_encoded
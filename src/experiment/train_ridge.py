import pandas as pd
import numpy as np

from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, root_mean_squared_error

from src.experiment.prepare_data import prepare_experiment_data


def train_and_evaluate_ridge(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    feature_columns: list[str],
) -> dict:
    X_train = train_df[feature_columns]
    y_train = train_df["demand_mwh"]

    X_validation = validation_df[feature_columns]
    y_validation = validation_df["demand_mwh"]

    ridge_pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("ridge", Ridge(alpha=1.0)),
        ]
    )

    ridge_pipeline.fit(X_train, y_train)

    y_prediction = ridge_pipeline.predict(X_validation)

    mae = mean_absolute_error(y_validation, y_prediction)
    rmse = root_mean_squared_error(y_validation, y_prediction)
    mean_error = np.mean(y_prediction - y_validation)

    mae_percent = (mae / y_validation.mean()) * 100

    return {
        "mae_mwh": float(mae),
        "rmse_mwh": float(rmse),
        "mean_signed_error_mwh": float(mean_error),
        "normalized_mae_percent": float(mae_percent),
    }

if __name__ == "__main__":
    # Reference Prediction
    train_df, validation_df, test_df = prepare_experiment_data()


    # Ridge A
    ridge_a_feature_columns = ["demand_lag_168"]
        
    results_A = train_and_evaluate_ridge(
        train_df,
        validation_df,
        ridge_a_feature_columns,
    )
    
    print(f"Ridge A mean validation metrics: {results_A}")


    # Ridge B
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

    ridge_b_feature_columns = [
        "demand_lag_168", 
        "hour_sin",
        "hour_cos",
        "day_of_week_0",
        "day_of_week_1",
        "day_of_week_2",
        "day_of_week_3",
        "day_of_week_4",
        "day_of_week_5",
        "day_of_week_6", 
        "month_sin",
        "month_cos",
    ]
    
    results_B = train_and_evaluate_ridge(
        train_df_encoded,
        validation_df_encoded,
        ridge_b_feature_columns,
    )

    print(f"Ridge B mean validation metrics: {results_B}")


    # Ridge C
    ridge_c_feature_columns = [
        "demand_lag_168",
        "hour_sin",
        "hour_cos",
        "day_of_week_0",
        "day_of_week_1",
        "day_of_week_2",
        "day_of_week_3",
        "day_of_week_4",
        "day_of_week_5",
        "day_of_week_6",
        "month_sin",
        "month_cos",
        "temperature_forecast_24h",
    ]

    results_C = train_and_evaluate_ridge(
        train_df_encoded,
        validation_df_encoded,
        ridge_c_feature_columns,
    )

    print(f"Ridge C mean validation metrics: {results_C}")
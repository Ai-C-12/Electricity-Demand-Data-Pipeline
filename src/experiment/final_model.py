import pandas as pd
import numpy as np

from src.experiment.prepare_data import prepare_experiment_data

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, root_mean_squared_error


train_df, validation_df, test_df = prepare_experiment_data()

final_train_df = pd.concat([train_df, validation_df], ignore_index=True)

feature_columns = [
    "demand_lag_168",
    "day_of_week",
    "hour",
    "month",
    "temperature_forecast_24h",
]

X_final_train = final_train_df[feature_columns]
Y_final_train = final_train_df["demand_mwh"]

X_test = test_df[feature_columns]
Y_test = test_df["demand_mwh"]


model = GradientBoostingRegressor(random_state=42)

model.fit(X_final_train, Y_final_train)

Y_prediction = model.predict(X_test)

mae = mean_absolute_error(Y_test, Y_prediction)
rmse = root_mean_squared_error(Y_test, Y_prediction)
mean_error = np.mean(Y_prediction - Y_test)

mae_percent = (mae / Y_test.mean()) * 100


results = {
    "mae_mwh": float(mae),
    "rmse_mwh": float(rmse),
    "mean_signed_error_mwh": float(mean_error),
    "normalized_mae_percent": float(mae_percent),
}

print(f"Results: {results}")
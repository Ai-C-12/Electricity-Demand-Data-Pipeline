# Results
## Model Results
| Method | Features | Calendar representation | MAE | RMSE | Bias | NMAE |
|---|---|---|---:|---:|---:|---:|
| Historical mean | Calendar baseline | Weekday/hour groups | 2532.11 | 3212.98 | -77.82 | 14.61% |
| Weekly naive | 168h lag | — | 1538.74 | 2320.07 | -180.55 | 8.88% |
| Ridge A | Lag | — | 1547.83 | 2274.67 | -136.27 | 8.93% |
| Ridge B | Lag + calendar | Encoded | 1544.22 | 2208.77 | -84.39 | 8.91% |
| Ridge C | Lag + calendar + weather | Encoded | 1542.04 | 2219.71 | -86.85 | 8.90% |
| Gradient Boost A | Lag | — | 1550.62 | 2296.25 | -161.29 | 8.95% |
| Gradient Boost B | Lag + calendar | Raw | 1375.83 | 1896.83 | -120.02 | 7.94% |
| **Gradient Boost C** | **Lag + calendar + weather** | **Raw** | **963.01** | **1334.80** | **+22.55** | **5.56%** |

## Final Model Selection
Based on validation performance, the selected final model is:

- Model: GradientBoostingRegressor
- Features:
  - demand_lag_168
  - day_of_week
  - hour
  - month
  - temperature_forecast_24h
- Calendar representation: raw integer features
- random_state: 42

This was selected using training and validation data only.
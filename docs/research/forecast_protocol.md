# Forecasting Protocol

## 1. Forecast Definition

- Geographic region: New York Independent System Operator (NYIS)
- Target variable: Hourly electricity demand
- Target unit: Megawatt-hours (MWh) per hourly interval
- Prediction frequency: One prediction per hour
- Forecast horizon: 24 hours ahead
- Forecast origin: For demand at target timestamp t, the prediction is made at t - 24 hours
- Information available at prediction time: Historical demand data published and available by t - 24 hours, known calendar information, and weather forecasts available by t - 24 hours
- Storage timezone: UTC
- Calendar-feature timezone: America/New_York
- Weather information used: Archived weather forecasts issued approximately 24 hours before the target timestamp

## 2. Feature Availability Rules

| Candidate feature | Available at forecast time? | Include? | Reason |
|---|---|---|---|
| Hour of day for target timestamp | Yes | Yes | The target timestamp is known in advance, so its hour is known. |
| Day of week for target timestamp | Yes | Yes | The target date is known in advance, so its day of the week is known. |
| Demand 1 hour before target (`t - 1`) | No | No | This demand occurs 23 hours after the forecast is made, so using it would leak future information. |
| Demand 24 hours before target (`t - 24`) | Uncertain | Maybe | This timestamp occurs at the forecast origin, but the demand value can only be used if it has already been published and is available. |
| Demand 168 hours before target (`t - 168`) | Yes | Yes | This value occurred seven days before the target and should already be available when the forecast is made. |
| Actual temperature at target time | No | No | The actual temperature occurs after the forecast is made, so using it would leak future information. |
| Weather forecast for target time issued by `t - 24` | Yes | Yes | This forecast would have been available when the electricity-demand prediction was made. |

## 3. Data Period Policy

- The forecasting will use a fixed historical date range rather than automatically updating to the current date.
- Training, validation, and test data will be separated chronologically.
- Complete calendar years will be preferred to simplify seasonal analysis.
- Raw demand data will begin at least 168 hours before the first study target so that weekly lag features can be created.
- The final test period will remain unchanged while features and models are selected.
- Live or recently updated data may be used separately for the dashboard, but not for the reported experiment results.

## 4. Timezone and Daylight-Saving-Time Policy

* UTC will remain the canonical timezone used for storing, joining, sorting, and validating hourly records.
* Calendar features such as hour of day, day of week, weekend status, and holidays will be calculated after converting each timestamp to the `America/New_York` timezone.
* Timestamps will not be converted to timezone-naive values.
* During the fall daylight-saving transition, repeated local hours will remain distinguishable using their unique UTC timestamps and timezone offsets.
* During the spring daylight-saving transition, the missing local hour will not be artificially inserted.
* Lag features will be calculated using the ordered UTC hourly timeline so that `t - 24` always represents exactly 24 elapsed hours before the target timestamp.
* Any analysis based on local clock time will use the converted New York timestamp rather than the raw UTC hour.


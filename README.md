# Electricity Demand Data Pipeline

A Python data pipeline that combines hourly electricity demand data with historical weather data for New York.

The project collects data from public APIs, cleans and validates it, joins the two datasets by timestamp, and saves the results for analysis. It also includes optional PostgreSQL and Azure Blob Storage support, along with a small Streamlit dashboard.

The project also includes a 24-hour-ahead electricity demand forecasting experiment that evaluates whether archived weather forecasts improve prediction accuracy.

## Pipeline Overview

The pipeline uses two data sources:

- **U.S. Energy Information Administration (EIA):** hourly electricity demand for the NYIS region
- **Open-Meteo:** hourly historical temperature data for New York City

The main pipeline follows these steps:

```text
EIA API -----------------> Clean and validate demand data ----\
                                                            \
                                                             -> Merge by timestamp -> Save feature dataset
                                                            /
Open-Meteo API ----------> Clean and validate weather data --/
```

The merged dataset contains electricity demand, temperature, and basic time information such as hour, day of the week, and month.

A separate forecasting pipeline uses the same electricity demand data but joins it with archived temperature forecasts from the Open-Meteo Previous Runs API instead of observed temperatures. The historical weather dataset is used for analysis and the dashboard, while the archived forecast dataset is used for the forecasting experiment.

Both pipelines use UTC for joins and storage, and New York local time for calendar features. Validation checks include missing values, duplicate timestamps, hourly timestamp coverage, and a minimum merge retention rate of 99%.

## Main Features

- Downloads hourly electricity demand data from the EIA API
- Downloads historical hourly temperature data from Open-Meteo
- Stores the original API responses as JSON
- Cleans and validates both datasets
- Joins demand and weather records by UTC timestamp
- Saves processed data as partitioned CSV files
- Saves the merged dataset as CSV and Parquet
- Writes a JSON summary for each pipeline run
- Can optionally load the merged data into PostgreSQL
- Can optionally upload generated files to Azure Blob Storage
- Includes tests for the main transformation, validation, and storage functions
- Includes a basic Streamlit dashboard for exploring the data
- Builds a separate dataset using archived 24-hour-ahead temperature forecasts
- Includes weekly demand lag features, chronological data splitting, and baseline and machine learning model comparisons
- Runs the test suite through GitHub Actions on pushes and pull requests

## Output Columns

The final dataset contains the following columns:

| Column | Description |
|---|---|
| `timestamp_utc` | Hourly timestamp in UTC |
| `region` | Electricity market region |
| `demand_mwh` | Hourly electricity demand in MWh |
| `temperature_2m` | Hourly temperature in degrees Celsius |
| `hour` | Hour in New York local time |
| `day_of_week` | Day of the week, where Monday is 0 |
| `month` | Month number |

The forecasting input dataset has the same columns, except `temperature_2m` is replaced by `temperature_forecast_24h` (archived forecast temperature in degrees Celsius). The experiment also adds `demand_lag_168`, which represents demand 168 hourly records before the target (seven days on a complete hourly timeline).

## Project Structure

```text
Electricity-Demand-Data-Pipeline/
├── .github/workflows/      # Automated tests
├── configs/                # Forecasting experiment configuration
├── dashboard/              # Streamlit dashboard
├── data/                   # Generated raw and processed data
├── docs/                   # Dashboard images and forecasting research
├── logs/                   # Logs and pipeline run summaries
├── scripts/                # Manual checks for external storage
├── sql/                    # PostgreSQL table definition
├── src/
│   ├── experiment/         # Data preparation, baselines, and model training
│   ├── ingest/             # EIA and weather API clients
│   ├── orchestration/      # Prefect flow
│   ├── pipeline/           # Pipeline entry points
│   ├── storage/            # Local, PostgreSQL, and Azure writers
│   ├── transform/          # Data cleaning and merge logic
│   ├── utils/              # Logging and run-summary helpers
│   └── validation/         # Data quality checks
├── tests/                  # Pytest test suite
├── .env.example
├── requirements.txt
└── README.md
```

## Setup

Python 3.11 is used by the project's CI workflow. An EIA API key and internet access are required to download the data.

### 1. Clone the repository

```bash
git clone https://github.com/Ai-C-12/Electricity-Demand-Data-Pipeline.git
cd Electricity-Demand-Data-Pipeline
```

### 2. Create a virtual environment

```bash
python -m venv venv
```

Activate it on Windows:

```bash
venv\Scripts\activate
```

Activate it on macOS or Linux:

```bash
source venv/bin/activate
```

### 3. Install the dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure the EIA API key

Create a `.env` file in the project root:

```env
EIA_API_KEY=your_eia_api_key_here
```

The PostgreSQL and Azure settings in `.env.example` are optional.

## Running the Core Pipeline

Run the complete demand and historical-weather pipeline with:

```bash
python -c "from src.pipeline.feature_pipeline import run_feature_pipeline; run_feature_pipeline()"
```

This command:

1. Downloads electricity demand data.
2. Downloads historical weather data.
3. Cleans and validates both datasets.
4. Merges them by timestamp.
5. Writes the final CSV and Parquet files.
6. Writes a JSON run summary.

The default date range and location settings are stored in `src/config.py`.

By default, the core pipelines request data for January 1 to December 31, 2025.

## Running the Forecasting Input Pipeline

To build the demand and archived weather forecast dataset separately, run:

```bash
python -c "from src.pipeline.feature_pipeline import run_forecasting_input_pipeline; run_forecasting_input_pipeline()"
```

To run both the historical weather and forecasting input pipelines together, run:

```bash
python -m src.pipeline.full_pipeline
```

Both datasets are saved locally as partitioned CSV and Parquet files. The optional PostgreSQL load applies only to the historical weather feature dataset.

The repository also includes a Prefect flow in `src/orchestration/flows.py` for orchestrating the historical pipeline with task retries. It currently references the old `run_weather_pipeline` function and needs to be updated to use `run_historical_weather_pipeline` before it can run.

## Generated Files

Raw API responses are stored under:

```text
data/raw/
```

Processed source data and the merged feature dataset are stored under:

```text
data/processed/
```

The files are partitioned by year, month, and day. For example:

```text
data/processed/demand_weather_features/
└── year=2025/
    └── month=01/
        └── day=01/
            ├── part-<run_id>.csv
            └── part-<run_id>.parquet
```

Run summaries are stored under:

```text
logs/run_summaries/
```

Each summary records information such as the row count, timestamp range, validation result, output formats, and pipeline duration.

The raw and processed source directories include `eia_region_data`, `weather_historical`, and `weather_forecast_24h`. Raw responses and request metadata are stored as `raw.json` and `request.json` under each source's `_runs/<run_id>/` directory.

The merged forecasting input files are stored under `data/processed/forecasting_inputs/`, using the same daily partition structure. Repeated runs write files with different run IDs.

## Optional PostgreSQL Load

To load the final dataset into PostgreSQL, add the following values to `.env`:

```env
ENABLE_POSTGRES_LOAD=true
DATABASE_URL=postgresql+psycopg2://username:password@localhost:5432/electricity_pipeline
```

The pipeline creates the required table and upserts records using the region and timestamp as the key.

PostgreSQL loading is disabled by default.

The PostgreSQL server must be running and the database in `DATABASE_URL` must already exist.

## Optional Azure Upload

To upload generated files to Azure Blob Storage, add the following values to `.env`:

```env
ENABLE_AZURE_UPLOAD=true
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_STORAGE_CONTAINER=electricity-pipeline
```

Azure uploads are disabled by default.

## Dashboard

The Streamlit dashboard reads the merged dataset from PostgreSQL.

After loading the data into PostgreSQL, run:

```bash
streamlit run dashboard/app.py
```

The dashboard includes summary metrics, demand trends, temperature comparisons, and tables of high-demand periods.

The sidebar allows filtering by date, month, and temperature range. See the [dashboard overview](docs/images/dashboard_overview.png) and [demand analysis](docs/images/dashboard_demand_analysis.png).

## Forecasting Experiment

The experiment tests whether archived weather forecasts improve hourly electricity demand predictions for NYIS at a 24-hour forecast horizon. It uses weekly demand lags and known calendar features rather than demand or observed temperature values that would not be available at forecast time.

The experiment settings are defined in `configs/forecast_experiment.yaml`. The configured data starts on January 20, 2024, to provide warm-up data for the 168-hour lag. The study is split chronologically (start dates inclusive, end dates exclusive, in UTC):

| Split | Start | End |
|---|---|---|
| Training | 2024-02-01 | 2025-02-01 |
| Validation | 2025-02-01 | 2025-08-01 |
| Test | 2025-08-01 | 2026-02-01 |

The experiment compares historical mean and weekly naive baselines with Ridge regression and Gradient Boosting regression. The models compare three feature sets: weekly demand lag only, lag with calendar features, and lag with calendar and archived weather forecasts.

### Running the Experiment

Run the following commands from the project root as needed:

```bash
# Prepare data and evaluate the historical mean baseline
python -m src.experiment.prepare_data

# Train and compare Ridge regression models
python -m src.experiment.train_ridge

# Train and compare Gradient Boosting models
python -m src.experiment.train_gradient_boosting

# Retrain the selected model and evaluate the held-out test set
python -m src.experiment.final_model
```

Each command fetches and prepares the data using the experiment's date range, independently of the core pipeline's default date range. The scripts print evaluation metrics to the terminal.

### Results

The selected model is Gradient Boosting with the weekly demand lag, raw calendar features (hour, day of week, and month), and archived temperature forecasts. The recorded validation results show an MAE of 963.01 MWh, an RMSE of 1,334.80 MWh, and a normalized MAE of 5.56%. The weekly naive baseline has an MAE of 1,538.74 MWh.

The final model script trains on the combined training and validation sets and evaluates on the held-out test set. Final test results are not yet recorded in the repository.

See the [forecasting protocol](docs/research/forecast_protocol.md) and [model results](docs/research/results.md) for more details.

## Tests

Run the test suite with:

```bash
pytest
```

The tests cover the main transformation, validation, merging, run-summary, and local-storage functions.

GitHub Actions also runs `pytest` automatically on pushes and pull requests using Python 3.11.

## Purpose

This project was created to practice building a complete data pipeline using public APIs. Its main focus is collecting data, checking its quality, combining related datasets, and producing files that can be used for further analysis.

The forecasting experiment extends this work by investigating the value of weather forecast data for day-ahead electricity demand prediction.

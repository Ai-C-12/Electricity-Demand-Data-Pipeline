# Electricity Demand Data Pipeline

A Python data pipeline that combines hourly electricity demand data with historical weather data for New York.

The project collects data from public APIs, cleans and validates it, joins the two datasets by timestamp, and saves the results for analysis. It also includes optional PostgreSQL and Azure Blob Storage support, along with a small Streamlit dashboard.

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

## Project Structure

```text
Electricity-Demand-Data-Pipeline/
├── dashboard/              # Streamlit dashboard
├── data/                   # Generated raw and processed data
├── logs/                   # Logs and pipeline run summaries
├── scripts/                # Manual checks for external storage
├── sql/                    # PostgreSQL table definition
├── src/
│   ├── ingest/             # EIA and weather API clients
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

## Optional PostgreSQL Load

To load the final dataset into PostgreSQL, add the following values to `.env`:

```env
ENABLE_POSTGRES_LOAD=true
DATABASE_URL=postgresql+psycopg2://username:password@localhost:5432/electricity_pipeline
```

The pipeline creates the required table and upserts records using the region and timestamp as the key.

PostgreSQL loading is disabled by default.

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

## Tests

Run the test suite with:

```bash
pytest
```

The tests cover the main transformation, validation, merging, run-summary, and local-storage functions.

## Purpose

This project was created to practice building a complete data pipeline using public APIs. Its main focus is collecting data, checking its quality, combining related datasets, and producing files that can be used for further analysis.
# Electricity Demand Data Pipeline

A Python data engineering project that ingests electricity demand and weather data, validates and transforms the data, writes analytics-ready outputs, and optionally loads the final feature table into PostgreSQL and Azure Blob Storage.

The project was built to practice production-style data engineering patterns such as API ingestion, validation, repeatable loads, orchestration, logging, testing, and basic observability.

## Overview

This pipeline pulls hourly electricity demand data from the U.S. Energy Information Administration API and hourly weather data from Open-Meteo. It cleans both sources, validates the outputs, merges them by timestamp, and produces a demand-weather feature dataset for analytics and future forecasting work.

The repository also includes an initial Streamlit dashboard backed by PostgreSQL.

## Reviewer TL;DR

This project demonstrates:

- API ingestion from EIA and Open-Meteo
- Raw API response storage for reproducibility
- Data cleaning and transformation with Python and pandas
- Validation checks for missing values, duplicate keys, timestamp coverage, and merge retention
- CSV and Parquet outputs partitioned by date
- JSON run summaries and structured logging
- Optional PostgreSQL loading with upsert logic
- Optional Azure Blob Storage uploads
- Prefect orchestration for the feature pipeline
- Pytest test coverage and GitHub Actions CI
- A basic PostgreSQL-backed Streamlit dashboard

## Tech Stack

| Area | Tools |
|---|---|
| Language | Python |
| Data processing | pandas, NumPy |
| Storage formats | JSON, CSV, Parquet |
| Database | PostgreSQL |
| Orchestration | Prefect |
| Cloud storage | Azure Blob Storage |
| Testing / CI | Pytest, GitHub Actions |
| Dashboard | Streamlit |

## Data Sources

### EIA Electricity API

The pipeline uses the U.S. Energy Information Administration API to collect hourly electricity demand data.

| Field | Value |
|---|---|
| Region | `NYIS` |
| Data type | Demand |
| Frequency | Hourly |
| Current development range | `2025-01-01` to `2025-12-31` |

The EIA API limits responses to 5,000 rows per request, so the ingestion client uses offset-based pagination and stores request metadata for each page.

### Open-Meteo API

The pipeline uses Open-Meteo to collect hourly NYC weather data, currently focused on `temperature_2m`.

| Field | Value |
|---|---|
| Location | New York City |
| Latitude | `40.7128` |
| Longitude | `-74.0060` |
| Variable | `temperature_2m` |

The weather data is used as a temperature proxy for exploring demand-weather relationships in the NYIS dataset.

## Pipeline Flow

```text
EIA API -----------\
                   \
                    -> Ingest -> Raw Storage -> Transform -> Validate -> Processed Data
                   /
Open-Meteo API ----/

Processed EIA Data + Processed Weather Data
                    -> Merge -> Validate -> Feature Dataset
                    -> CSV + Parquet
                    -> Optional PostgreSQL Load
                    -> Optional Azure Blob Upload
                    -> JSON Run Summary
                    -> Streamlit Dashboard
```

## Current Features

### Ingestion

- Fetches hourly electricity demand data from the EIA API
- Fetches hourly weather data from Open-Meteo
- Handles EIA pagination beyond the 5,000-row response limit
- Saves raw API payloads and request metadata

### Transformation and Feature Generation

- Cleans and standardizes raw EIA data
- Cleans and standardizes weather data
- Merges demand and weather data by timestamp
- Adds basic time-based features:
  - `hour`
  - `day_of_week`
  - `month`

### Validation

The pipeline includes validation checks for:

- Empty DataFrames
- Required columns
- Missing values
- Timestamp format
- Non-negative demand values
- Numeric temperature values
- Duplicate timestamp-region pairs
- Merge retention
- Continuous hourly timestamp coverage

### Outputs

The pipeline writes:

- Raw JSON payloads
- Request metadata JSON files
- Partitioned processed CSV files
- Final feature datasets in CSV and Parquet
- JSON run summaries
- Optional PostgreSQL table output
- Optional Azure Blob Storage artifacts

### Testing and CI

The project includes Pytest coverage for core local logic. Tests avoid live API calls and external services, making them suitable for local development and GitHub Actions CI.

Current test areas include:

- EIA transformation
- Weather transformation
- Feature merge logic
- Validation checks
- Run summary writing
- Raw storage writing
- CSV and Parquet partition writers
- Storage writer behavior

## Project Structure

```text
Electricity-Demand-Data-Pipeline/
├─ .github/workflows/
│  └─ tests.yml
├─ dashboard/
│  └─ app.py
├─ data/
│  ├─ raw/
│  └─ processed/
├─ docs/images/
├─ logs/run_summaries/
├─ scripts/
│  ├─ manual_postgres_writer_check.py
│  └─ manual_azure_upload_check.py
├─ sql/
│  └─ demand_weather_features.sql
├─ src/
│  ├─ ingest/
│  ├─ orchestration/
│  ├─ pipeline/
│  ├─ storage/
│  ├─ transform/
│  ├─ utils/
│  ├─ validation/
│  ├─ cli.py
│  └─ config.py
├─ tests/
├─ README.md
├─ requirements.txt
├─ .gitignore
└─ .env.example
```

## Output Details

### Raw Data

Raw API responses are saved by run ID.

```text
data/raw/eia_region_data/_runs/<run_id>/raw.json
data/raw/eia_region_data/_runs/<run_id>/request.json

data/raw/weather_data/_runs/<run_id>/raw.json
data/raw/weather_data/_runs/<run_id>/request.json
```

Raw payloads are stored separately from request metadata so that the original API responses can be inspected later.

### Processed Data

Processed source data is saved as partitioned CSV files by date.

```text
data/processed/eia_region_data/year=2025/month=01/day=01/part-<run_id>.csv
data/processed/weather_data/year=2025/month=01/day=01/part-<run_id>.csv
```

### Feature Dataset

The final demand-weather feature table is written in both CSV and Parquet format.

```text
data/processed/demand_weather_features/year=2025/month=01/day=01/part-<run_id>.csv
data/processed/demand_weather_features/year=2025/month=01/day=01/part-<run_id>.parquet
```

Feature columns include:

| Column | Description |
|---|---|
| `timestamp_utc` | Hourly timestamp in UTC |
| `region` | Electricity region |
| `demand_mwh` | Electricity demand |
| `temperature_2m` | Hourly temperature from Open-Meteo |
| `hour` | Hour extracted from timestamp |
| `day_of_week` | Day of week extracted from timestamp |
| `month` | Month extracted from timestamp |

### Run Summaries

Each pipeline run writes a JSON summary with metadata such as:

- Row counts
- Column counts
- Timestamp range
- Validation status
- Output formats
- Merge retention rate
- Timestamp coverage status
- Pipeline duration
- PostgreSQL load status, if enabled
- Azure upload status, if enabled

Example paths:

```text
logs/run_summaries/eia_region_data/<run_id>.json
logs/run_summaries/weather_data/<run_id>.json
logs/run_summaries/demand_weather_features/<run_id>.json
```

## PostgreSQL Load

The project supports an optional PostgreSQL load for the final `demand_weather_features` table.

The schema is defined in:

```text
sql/demand_weather_features.sql
```

When enabled, the pipeline creates the table if needed and upserts records using `(region, timestamp_utc)` as the primary key. This allows rerunning the pipeline without creating duplicate hourly records.

```env
ENABLE_POSTGRES_LOAD=false
DATABASE_URL=postgresql+psycopg2://username:password@localhost:5432/electricity_pipeline
```

PostgreSQL loading is disabled by default so local development, tests, and CI do not require a running database or database credentials.

## Azure Blob Storage

The project supports optional Azure Blob Storage uploads for generated artifacts.

When enabled, the pipeline uploads:

- Raw API payloads
- Request metadata
- Processed CSV outputs
- Feature CSV and Parquet outputs
- Run summary JSON files

```env
ENABLE_AZURE_UPLOAD=false
AZURE_STORAGE_CONNECTION_STRING=your_azure_storage_connection_string_here
AZURE_STORAGE_CONTAINER=electricity-pipeline
```

Azure upload is disabled by default. Secrets should be stored only in a local `.env` file or secure environment variable and should never be committed to GitHub.

## Streamlit Dashboard

The repository includes a basic PostgreSQL-backed Streamlit dashboard in:

```text
dashboard/app.py
```

The dashboard reads from the PostgreSQL `demand_weather_features` table and provides a first analytics layer for exploring the merged dataset.

Current dashboard features include:

- Summary metric cards
- Temperature vs. demand scatter chart
- Daily average demand trend
- Highest-demand hours table
- Highest-demand days table
- Temperature bucket analysis
- Basic outlier checks
- Sidebar filters for date, month, and temperature range

Run the dashboard locally after the PostgreSQL table has been loaded:

```bash
streamlit run dashboard/app.py
```

## Orchestration

The project includes a Prefect flow wrapper for the end-to-end feature pipeline.

```bash
python -m src.orchestration.flows
```

The current flow is split into separate tasks for:

1. EIA ingestion
2. Weather ingestion
3. Feature dataset creation

This provides more visibility into each stage of the pipeline than running everything as one large script.

## Quickstart

### 1. Clone the repository

```bash
git clone <repo-url>
cd Electricity-Demand-Data-Pipeline
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
```

Windows:

```bash
venv\Scripts\activate
```

macOS/Linux:

```bash
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a local `.env` file or set the variables in your shell.

Required:

```env
EIA_API_KEY=your_eia_api_key_here
```

Optional PostgreSQL load:

```env
ENABLE_POSTGRES_LOAD=false
DATABASE_URL=postgresql+psycopg2://username:password@localhost:5432/electricity_pipeline
```

Optional Azure upload:

```env
ENABLE_AZURE_UPLOAD=false
AZURE_STORAGE_CONNECTION_STRING=your_azure_storage_connection_string_here
AZURE_STORAGE_CONTAINER=electricity-pipeline
```

Do not commit API keys, database URLs, or cloud credentials.

## Running the Project

| Task | Command |
|---|---|
| Run EIA pipeline | `python -m src.pipeline.eia_pipeline` |
| Run weather pipeline | `python -m src.pipeline.weather_pipeline` |
| Run both source pipelines | `python -m src.pipeline.full_pipeline` |
| Run full feature pipeline | `python -m src.pipeline.feature_pipeline` |
| Run Prefect-orchestrated feature pipeline | `python -m src.orchestration.flows` |
| Run Streamlit dashboard | `streamlit run dashboard/app.py` |
| Run tests | `pytest` |

## Current Development Range

The current dataset covers one year of hourly data for the `NYIS` region.

```text
2025-01-01 00:00 UTC -> 2025-12-31 23:00 UTC
```

A larger 3-year scale test from 2023 to 2025 produced 26,304 merged hourly feature rows.

## Roadmap

Planned next steps:

1. Add a forecasting-ready machine learning workflow.
2. Add Docker support or scheduled Prefect deployments.
3. Improve production-oriented orchestration and monitoring patterns.
4. Expand the dashboard after the core pipeline and analytics layer are stable.

## Why I Built This

I built this project to practice the core pieces of a production-style data engineering workflow:

- ingesting data from external APIs
- preserving raw source data
- validating and transforming records
- creating analytics-ready feature tables
- writing repeatable database loads
- adding test coverage and CI
- using orchestration for pipeline visibility
- connecting pipeline output to a basic analytics interface

The project is intended as a portfolio-grade foundation for data engineering, analytics, and future forecasting work.

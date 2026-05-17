# Electricity Demand Data Pipeline ![Tests](https://github.com/Ai-C-12/Electricity-Demand-Data-Pipeline/actions/workflows/tests.yml/badge.svg)
A Python data engineering pipeline that ingests hourly electricity demand data from the EIA API and hourly weather data from Open-Meteo, stores raw API responses, transforms the data into processed datasets, validates schemas, and creates an analytics-ready demand-weather feature table.

## Project Status
The default development configuration currently runs one full year of hourly data for 2025. The pipeline has also been successfully tested on a 3-year 2023–2025 range, producing 26,304 merged hourly feature rows.

Current capabilities:

- Fetch hourly electricity demand data from the EIA API
- Fetch hourly weather data from Open-Meteo
- Save raw API payloads for reproducibility
- Save request metadata separately from raw data
- Transform raw API data into cleaned processed datasets
- Validate processed datasets before saving
- Merge electricity demand and weather data by timestamp
- Save a partitioned analytics-ready feature dataset in both CSV and Parquet format
- Supports paginated EIA ingestion beyond the 5,000-row API response limit
- Write per-run JSON summaries for EIA, weather, and merged feature outputs, including merge-quality metadata for the feature dataset
- Test core pipeline logic with pytest
- Run automated tests on GitHub Actions for every push and pull request
- Validate merge retention and continuous hourly timestamp coverage
- Run the end-to-end feature pipeline through an initial Prefect orchestration wrapper

Future planned work:

- Split the initial Prefect wrapper into more granular tasks
- Add PostgreSQL storage for analytics-ready tables
- Add cloud storage for raw/processed pipeline artifacts
- Add a dashboard or ML forecasting layer

## Data Sources

### EIA Electricity API

The pipeline pulls hourly electricity demand data from the U.S. Energy Information Administration API.

Current development configuration:

- Respondent: `NYIS`
- Data type: `D` demand
- Frequency: hourly
- Current development range: January 1, 2025 to December 31, 2025

#### EIA Pagination
The EIA API response is limited to 5,000 rows per request, so the EIA ingestion client supports pagination using offset-based requests. Each page is collected into a combined DataFrame, while request metadata records the offset, page size, and number of rows returned for each page.

### Open-Meteo API

The pipeline pulls hourly weather data from Open-Meteo.

Current development configuration:

- Location: New York City
- Latitude: `40.7128`
- Longitude: `-74.0060`
- Variable: `temperature_2m`

## Pipeline Architecture

<pre>
EIA API -----------\
                   \
                    → Ingest → Raw Storage → Transform → Validate → Processed Data
                   /
Open-Meteo API ----/

Processed EIA Data + Processed Weather Data
                    → Merge → Validate → Feature Dataset
</pre>

## Project Structure
<pre>
Electricity-Demand-Data-Pipeline/
├─ .github/
│  └─ workflows/
│     └─ tests.yml
│
├─ data/
│  ├─ raw/
│  │  ├─ eia_region_data/
│  │  └─ weather_data/
│  └─ processed/
│     ├─ eia_region_data/
│     ├─ weather_data/
│     └─ demand_weather_features/
│
├─ logs/
│  └─ run_summaries/
│     ├─ demand_weather_features/
│     ├─ eia_region_data/
│     └─ weather_data/
│
├─ src/
│  ├─ ingest/
│  │  ├─ __init__.py
│  │  ├─ eia_client.py
│  │  └─ weather_client.py
│  │
│  ├─ orchestration/
│  │  ├─ __init__.py
│  │  └─ flows.py
│  │
│  ├─ pipeline/
│  │  ├─ __init__.py
│  │  ├─ eia_pipeline.py
│  │  ├─ weather_pipeline.py
│  │  ├─ full_pipeline.py
│  │  └─ feature_pipeline.py
│  │
│  ├─ storage/
│  │  ├─ __init__.py
│  │  ├─ paths.py
│  │  └─ write_raw.py
│  │
│  ├─ transform/
│  │  ├─ __init__.py
│  │  ├─ eia_transform.py
│  │  ├─ weather_transform.py
│  │  └─ merge_features.py
│  │
│  ├─ utils/
│  │  ├─ __init__.py
│  │  ├─ logger.py
│  │  └─ run_summary.py
│  │
│  ├─ validation/
│  │  ├─ __init__.py
│  │  └─ checks.py
│  │
│  ├─ __init__.py
│  ├─ cli.py
│  └─ config.py
│
├─ tests/
│  ├─ __init__.py
│  ├─ test_eia_transform.py
│  ├─ test_weather_transform.py
│  ├─ test_merge_features.py
│  ├─ test_validation_checks.py
│  ├─ test_run_summary.py
│  ├─ test_storage_writers.py
│  └─ test_raw_storage.py
│
├─ README.md
├─ requirements.txt
├─ .gitignore
└─ .env.example
</pre>

## Output Datasets
### Raw Data
Raw API responses are saved by run ID.

Example:
```
data/raw/eia_region_data/_runs/<run_id>/raw.json
data/raw/eia_region_data/_runs/<run_id>/request.json

data/raw/weather_data/_runs/<run_id>/raw.json
data/raw/weather_data/_runs/<run_id>/request.json
```
The raw payload is saved separately from request metadata so the pipeline can preserve original API responses.

For EIA data, raw payloads include paginated API responses. The request metadata tracks each page’s offset, page size, and number of rows returned.

### Processed Data
Processed data is saved as partitioned CSV files by date.

Example:
```
data/processed/eia_region_data/year=2025/month=01/day=01/part-<run_id>.csv
data/processed/weather_data/year=2025/month=01/day=01/part-<run_id>.csv
```

### Feature Dataset
The feature pipeline currently writes the merged dataset in both CSV and Parquet format.

Example:
```
data/processed/demand_weather_features/year=2025/month=01/day=01/part-<run_id>.csv
data/processed/demand_weather_features/year=2025/month=01/day=01/part-<run_id>.parquet
```

Current feature columns:
- timestamp_utc
- region
- demand_mwh
- temperature_2m
- hour
- day_of_week
- month

CSV output is kept for readability during development. Parquet output is added for more production-style analytics storage because it preserves schema better and is commonly used in data engineering workflows.

### Run Summaries

Each pipeline run writes a JSON summary containing row counts, column counts, timestamp range, output formats, validation status, and generation time.

For the merged feature dataset, the run summary also includes merge-quality metadata such as source row counts, merged row count, expected merge rows, merge retention rate, merge retention status, timestamp coverage status, pipeline duration, and compact output metadata with partition counts.

Examples:
```text
logs/run_summaries/eia_region_data/<run_id>.json
logs/run_summaries/weather_data/<run_id>.json
logs/run_summaries/demand_weather_features/<run_id>.json
```

## Validation
The pipeline validates data before saving processed outputs.

### check_not_empty
Checks if the DataFrame is empty or not

### check_required_columns
Checks whether the required columns exist in the DataFrame.

### check_no_missing_values
Checks whether there are missing values and outputs how many there are.

### check_timestamp_format
Checks whether timestamp_utc is in the correct datetime format.

### check_demand_values
Checks whether demand_mwh is numeric and non-negative.

### check_temperature_values
Checks whether temperature_2m is numeric.

### check_duplicate_timestamps_region
Checks whether duplicate timestamp-region pairs exist in the merged feature dataset.

### check_merge_retention
Checks whether the merged feature dataset kept enough rows from the source demand/weather inputs.

### check_hourly_timestamp_coverage
Checks whether the merged feature dataset has a continuous hourly timestamp range with no missing hours.

## Testing
The project includes a lightweight pytest suite for the core local logic. These tests avoid live API calls and use small in-memory DataFrames or temporary folders.

Current tests cover:
- EIA transform behavior
- Weather transform behavior
- Demand-weather merge logic
- Validation checks
- Run summary JSON writing, including optional extra metadata
- Raw storage JSON writing
- Partitioned CSV and Parquet writers

Run tests locally:
```bash
pytest
```

## Orchestration

The project includes an initial Prefect flow wrapper for the end-to-end feature pipeline.

Run the Prefect flow locally:
```bash
python -m src.orchestration.flows
```

The current Prefect flow wraps the existing feature pipeline as one tracked flow/task. This confirms the pipeline can be orchestrated and monitored through Prefect. A future improvement is to split the flow into smaller tasks for EIA ingestion, weather ingestion, feature generation, validation, output writing, and run summary generation.

## Setup
### 1. Clone the Repository
```
git clone <repo-url>
cd Electricity-Demand-Data-Pipeline
```

### 2. Create a Virtual Environment
```
python -m venv venv
```
Activate it:
```
venv\Scripts\activate
```
On macOS/Linux:
```
source venv/bin/activate
```

### 3. Install Dependencies
```
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Create a local .env file or set the variable in your shell. Do not commit real API keys.

Required variable:
```
EIA_API_KEY=your_eia_api_key_here
```

## Running the Pipeline
Run EIA pipeline:
```
python -m src.pipeline.eia_pipeline
```
Run weather pipeline:
```
python -m src.pipeline.weather_pipeline
```
Run both source pipelines:
```
python -m src.pipeline.full_pipeline
```
Run full feature pipeline:
```
python -m src.pipeline.feature_pipeline
```
Run Prefect-orchestrated feature pipeline:
```bash
python -m src.orchestration.flows
```

The feature pipeline runs the EIA pipeline, runs the weather pipeline, merges the processed outputs, validates the merged dataset, checks merge retention and hourly timestamp coverage, saves the final feature table as both CSV and Parquet, and writes JSON run summaries for the source and feature datasets.

## Current Development Range
The current dataset covers one year of hourly data, producing approximately 8,760 merged feature rows for the NYIS region.

## Next Steps
1. Split the Prefect wrapper into more granular pipeline tasks
2. Add PostgreSQL storage for the final analytics-ready feature dataset
3. Add cloud storage for raw and processed artifacts
4. Build a dashboard or forecasting-ready ML workflow
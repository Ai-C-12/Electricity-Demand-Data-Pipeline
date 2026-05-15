# Electricity Demand Data Pipeline

A Python data engineering pipeline that ingests hourly electricity demand data from the EIA API and hourly weather data from Open-Meteo, stores raw API responses, transforms the data into processed datasets, validates schemas, and creates an analytics-ready demand-weather feature table.

## Project Status

This project currently supports a local end-to-end pipeline for a 90-day dataset.

Current capabilities:

- Fetch hourly electricity demand data from the EIA API
- Fetch hourly weather data from Open-Meteo
- Save raw API payloads for reproducibility
- Save request metadata separately from raw data
- Transform raw API data into cleaned processed datasets
- Validate processed datasets before saving
- Merge electricity demand and weather data by timestamp
- Save a partitioned analytics-ready feature dataset

Future planned work:

- Add stronger logging
- Add larger historical date ranges
- Add Parquet output support
- Add orchestration with Prefect
- Add PostgreSQL or cloud storage
- Build dashboard or ML forecasting layer

## Data Sources

### EIA Electricity API

The pipeline pulls hourly electricity demand data from the U.S. Energy Information Administration API.

Current development configuration:

- Respondent: `NYIS`
- Data type: `D` demand
- Frequency: hourly
- Current test range: January 1, 2026 to March 31, 2026

### Open-Meteo API

The pipeline pulls hourly weather data from Open-Meteo.

Current development configuration:

- Location: New York City
- Latitude: `40.7128`
- Longitude: `-74.0060`
- Variable: `temperature_2m`

## Pipeline Architecture

```
EIA API -----------\
                   \
                    → Ingest → Raw Storage → Transform → Validate → Processed Data
                   /
Open-Meteo API ----/

Processed EIA Data + Processed Weather Data
                    → Merge → Validate → Feature Dataset
```

## Project Structure
<pre>
Electricity-Demand-Data-Pipeline/
├─ data/
│  ├─ raw/
│  │  ├─ eia_region_data/
│  │  └─ weather_data/
│  └─ processed/
│     ├─ eia_region_data/
│     ├─ weather_data/
│     └─ demand_weather_features/
│
├─ src/
│  ├─ ingest/
│  │  ├─ eia_client.py
│  │  └─ weather_client.py
│  │
│  ├─ transform/
│  │  ├─ eia_transform.py
│  │  ├─ weather_transform.py
│  │  └─ merge_features.py
│  │
│  ├─ storage/
│  │  ├─ paths.py
│  │  └─ write_raw.py
│  │
│  ├─ validation/
│  │  └─ checks.py
│  │
│  └─ pipeline/
│     ├─ eia_pipeline.py
│     ├─ weather_pipeline.py
│     ├─ full_pipeline.py
│     └─ feature_pipeline.py
│
├─ .env.example
├─ .gitignore
├─ requirements.txt
└─ README.md
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

### Processed Data
Processed data is saved as partitioned CSV files by date.

Example:
```
data/processed/eia_region_data/year=2026/month=01/day=01/part-<run_id>.csv
data/processed/weather_data/year=2026/month=01/day=01/part-<run_id>.csv
```

### Feature Dataset
The final merged feature dataset is saved under data/processed/demand_weather_features/

Current feature columns:
- timestamp_utc
- region
- demand_mwh
- temperature_2m
- hour
- day_of_week
- month

## Validation
The pipeline validates data before saving processed outputs.

### check_not_empty
Checks if the dataframe is empty or not

### check_required_columns
Checks whether the required columns exits in the dataframe.

### check_no_missing_values
Checks whether there are missing values and outputs how many there are.

### check_timestamp_format
Checks whether the time is in corret UTC format.

### check_demand_values
Checks whether or not demand_mwh is in proper numeric value.

### check_temperature_values
Checks whether or not temperature_2m is in proper numeric value.

### check_duplicate_timestamps_region
Checks whether the merged feature pipeline contains data within the same time and region.

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
The feature pipeline runs the EIA pipeline, runs the weather pipeline, merges the processed outputs, validates the merged dataset, and saves the final feature table.

## Current Development Range
The current dataset covers 90 days of data from 2026-01-01 to 2026-03-31

## Next Steps
1. Improve logging and pipeline run summaries
2. Add Parquet output support
3. Test larger historical ranges
4. Add Prefect orchestration
5. Add PostgreSQL or cloud storage
6. Build dashboard or forecasting-ready ML workflow
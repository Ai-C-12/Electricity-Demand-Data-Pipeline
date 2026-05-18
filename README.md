<div align="center">

# ⚡ Electricity Demand Data Pipeline

### A production-style data engineering pipeline for hourly electricity demand, weather enrichment, cloud storage, orchestration, and analytics-ready feature generation.

<br />

[![Tests](https://github.com/Ai-C-12/Electricity-Demand-Data-Pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/Ai-C-12/Electricity-Demand-Data-Pipeline/actions/workflows/tests.yml)
![Python](https://img.shields.io/badge/Python-Data%20Pipeline-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Transforms-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Prefect](https://img.shields.io/badge/Prefect-Orchestration-070E10?style=for-the-badge&logo=prefect&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Optional%20Load-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Azure](https://img.shields.io/badge/Azure%20Blob-Optional%20Upload-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Pytest](https://img.shields.io/badge/Pytest-Tested-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-Analytics%20Storage-50ABF1?style=for-the-badge&logo=apache&logoColor=white)

<br />

> **From raw APIs to analytics-ready features:** EIA electricity demand + Open-Meteo weather → validated, partitioned, summarized, optionally loaded to PostgreSQL and Azure Blob Storage.

<br />

</div>

---

## ✨ Project Snapshot

| Category | Details |
|---|---|
| **Pipeline type** | Python data engineering pipeline |
| **Primary use case** | Build an analytics-ready demand-weather feature table |
| **Electricity source** | U.S. Energy Information Administration API |
| **Weather source** | Open-Meteo API |
| **Current region** | `NYIS` |
| **Current development range** | January 1, 2025 → December 31, 2025 |
| **Successful scale test** | 3-year range, 2023–2025, producing **26,304 merged hourly feature rows** |
| **Storage formats** | Raw JSON, partitioned CSV, partitioned Parquet |
| **Optional infrastructure** | PostgreSQL upsert, Azure Blob Storage upload |
| **Orchestration** | Granular Prefect flow |
| **Testing** | Pytest + GitHub Actions |

> [!NOTE]
> Python 3.10+ is recommended. Python 3.12 may be more stable for local Prefect usage if newer Python versions cause environment issues.

---

## 🌎 What This Pipeline Does

This project ingests hourly electricity demand data and hourly weather data, cleans and validates both sources, merges them by timestamp, and produces a feature dataset suitable for analytics, dashboards, and future forecasting workflows.

<div align="center">

### Raw APIs → Clean Data → Validated Features → Analytics Layer

</div>

```mermaid
flowchart LR
    A[EIA Electricity API] --> B[Ingest]
    C[Open-Meteo API] --> B

    B --> D[Raw JSON Storage]
    D --> E[Transform]
    E --> F[Validation]
    F --> G[Partitioned Processed Data]

    G --> H[Feature Merge]
    H --> I[Merge Quality Checks]
    I --> J[CSV + Parquet Feature Dataset]

    J --> K[(Optional PostgreSQL Upsert)]
    D --> L[(Optional Azure Blob Upload)]
    G --> L
    J --> L

    B --> M[JSON Run Summaries]
    J --> M
    M --> L
```

---

## 🧭 Current Capabilities

### ✅ Built and Working

| Area | Capability |
|---|---|
| **Ingestion** | Fetch hourly electricity demand data from the EIA API |
| **Ingestion** | Fetch hourly weather data from Open-Meteo |
| **Pagination** | Supports EIA pagination beyond the 5,000-row API response limit |
| **Raw storage** | Saves raw API payloads for reproducibility |
| **Metadata** | Saves request metadata separately from raw data |
| **Transformation** | Cleans and standardizes raw EIA and weather data |
| **Validation** | Validates processed datasets before saving |
| **Feature engineering** | Merges demand and weather by timestamp |
| **Feature engineering** | Adds `hour`, `day_of_week`, and `month` |
| **Storage** | Saves partitioned CSV and Parquet outputs |
| **Run summaries** | Writes per-run JSON summaries for EIA, weather, and feature outputs |
| **Quality checks** | Validates merge retention and continuous hourly timestamp coverage |
| **Testing** | Tests core pipeline logic with pytest |
| **CI** | Runs automated tests on GitHub Actions for every push and pull request |
| **Orchestration** | Runs the feature pipeline through a granular Prefect flow |
| **Database** | Optionally loads the final feature dataset into PostgreSQL using upsert logic |
| **Cloud** | Optionally uploads artifacts to Azure Blob Storage |

### 🛣️ Planned Next

| Priority | Future Work |
|---|---|
| 1 | Add a dashboard or analytics layer on top of the PostgreSQL, Parquet, or Azure-backed feature dataset |
| 2 | Add a forecasting-ready machine learning workflow |
| 3 | Add stronger deployment options such as Docker or scheduled Prefect deployments |
| 4 | Expand production-oriented orchestration patterns once the analytics layer is useful |

---

## 🗂️ Data Sources

### ⚡ EIA Electricity API

The pipeline pulls hourly electricity demand data from the U.S. Energy Information Administration API.

| Setting | Value |
|---|---|
| Respondent | `NYIS` |
| Data type | `D` demand |
| Frequency | Hourly |
| Current development range | `2025-01-01` → `2025-12-31` |

#### EIA Pagination

The EIA API response is limited to 5,000 rows per request, so the ingestion client uses offset-based pagination.

Each page is collected into a combined DataFrame, while request metadata records:

- page offset
- page size
- number of rows returned per page

---

### 🌦️ Open-Meteo API

The pipeline pulls hourly weather data from Open-Meteo.

| Setting | Value |
|---|---|
| Location | New York City |
| Latitude | `40.7128` |
| Longitude | `-74.0060` |
| Variable | `temperature_2m` |

---

## 🏗️ Architecture

### Pipeline Flow

```text
EIA API -----------\
                   \
                    → Ingest → Raw Storage → Transform → Validate → Processed Data
                   /
Open-Meteo API ----/

Processed EIA Data + Processed Weather Data
                    → Merge → Validate → Feature Dataset
                    → CSV + Parquet
                    → Optional PostgreSQL
                    → Optional Azure Blob Storage
                    → Run Summary JSON
```

### Project Structure

```text
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
├─ scripts/
│  ├─ __init__.py
│  ├─ manual_postgres_writer_check.py
│  └─ manual_azure_upload_check.py
│
├─ sql/
│  └─ demand_weather_features.sql
│
├─ src/
│  ├─ ingest/
│  │  ├─ eia_client.py
│  │  └─ weather_client.py
│  │
│  ├─ orchestration/
│  │  └─ flows.py
│  │
│  ├─ pipeline/
│  │  ├─ eia_pipeline.py
│  │  ├─ weather_pipeline.py
│  │  ├─ full_pipeline.py
│  │  └─ feature_pipeline.py
│  │
│  ├─ storage/
│  │  ├─ azure_blob_writer.py
│  │  ├─ paths.py
│  │  ├─ postgres_writer.py
│  │  └─ write_raw.py
│  │
│  ├─ transform/
│  │  ├─ eia_transform.py
│  │  ├─ weather_transform.py
│  │  └─ merge_features.py
│  │
│  ├─ utils/
│  │  ├─ logger.py
│  │  └─ run_summary.py
│  │
│  ├─ validation/
│  │  └─ checks.py
│  │
│  ├─ cli.py
│  └─ config.py
│
├─ tests/
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
```

---

## 📦 Output Datasets

### 1. Raw Data

Raw API responses are saved by run ID.

```text
data/raw/eia_region_data/_runs/<run_id>/raw.json
data/raw/eia_region_data/_runs/<run_id>/request.json

data/raw/weather_data/_runs/<run_id>/raw.json
data/raw/weather_data/_runs/<run_id>/request.json
```

The raw payload is saved separately from request metadata so the original API responses remain reproducible.

For EIA data, raw payloads include paginated API responses. The request metadata tracks each page’s offset, page size, and number of rows returned.

---

### 2. Processed Data

Processed data is saved as partitioned CSV files by date.

```text
data/processed/eia_region_data/year=2025/month=01/day=01/part-<run_id>.csv
data/processed/weather_data/year=2025/month=01/day=01/part-<run_id>.csv
```

---

### 3. Feature Dataset

The feature pipeline writes the merged dataset in both CSV and Parquet format.

```text
data/processed/demand_weather_features/year=2025/month=01/day=01/part-<run_id>.csv
data/processed/demand_weather_features/year=2025/month=01/day=01/part-<run_id>.parquet
```

#### Feature Columns

| Column | Description |
|---|---|
| `timestamp_utc` | Hourly timestamp in UTC |
| `region` | Electricity region/respondent |
| `demand_mwh` | Electricity demand in megawatt-hours |
| `temperature_2m` | Hourly temperature from Open-Meteo |
| `hour` | Hour extracted from timestamp |
| `day_of_week` | Day of week extracted from timestamp |
| `month` | Month extracted from timestamp |

CSV output is kept for readability during development. Parquet output is included for more production-style analytics storage because it preserves schema better and is commonly used in data engineering workflows.

---

### 4. Run Summaries

Each pipeline run writes a JSON summary containing row counts, column counts, timestamp range, output formats, validation status, and generation time.

```text
logs/run_summaries/eia_region_data/<run_id>.json
logs/run_summaries/weather_data/<run_id>.json
logs/run_summaries/demand_weather_features/<run_id>.json
```

For the merged feature dataset, the run summary also includes:

| Metadata | Purpose |
|---|---|
| Source row counts | Shows how many rows came from demand and weather inputs |
| Merged row count | Shows final merged feature size |
| Expected merge rows | Helps compare actual vs. expected output |
| Merge retention rate | Tracks how much data survived the merge |
| Merge retention status | Flags whether merge quality is acceptable |
| Timestamp coverage status | Confirms continuous hourly coverage |
| Pipeline duration | Measures runtime |
| Output metadata | Tracks partition counts and generated formats |

For optional infrastructure steps, run summaries record whether Azure Blob upload was enabled, including uploaded file counts when applicable. The feature run summary also records whether PostgreSQL loading was enabled and which table was used.

---

## 🐘 PostgreSQL Output

The project supports an optional PostgreSQL load for the final `demand_weather_features` table.

The schema is defined in:

```text
sql/demand_weather_features.sql
```

The feature dataset can be loaded into PostgreSQL with an upsert strategy using `(region, timestamp_utc)` as the primary key. This allows rerunning the pipeline without creating duplicate hourly records.

```env
DATABASE_URL=postgresql+psycopg2://username:password@localhost:5432/electricity_pipeline
ENABLE_POSTGRES_LOAD=false
```

When `ENABLE_POSTGRES_LOAD=true`, the feature pipeline creates the table if needed and upserts the merged feature dataset into PostgreSQL.

> [!IMPORTANT]
> PostgreSQL loading is disabled by default so local development, pytest, and GitHub Actions do not require a running database or database credentials.

---

## ☁️ Azure Blob Storage Output

The project supports optional Azure Blob Storage uploads for generated pipeline artifacts.

When enabled, the pipeline uploads:

- raw API payloads
- request metadata
- processed CSV outputs
- feature CSV and Parquet outputs
- run summary JSON files

### Example Azure Blob Paths

```text
raw/eia_region_data/_runs/<run_id>/raw.json
raw/eia_region_data/_runs/<run_id>/request.json

raw/weather_data/_runs/<run_id>/raw.json
raw/weather_data/_runs/<run_id>/request.json

processed/eia_region_data/year=2025/month=01/day=01/part-<run_id>.csv
processed/weather_data/year=2025/month=01/day=01/part-<run_id>.csv
processed/demand_weather_features/year=2025/month=01/day=01/part-<run_id>.csv
processed/demand_weather_features/year=2025/month=01/day=01/part-<run_id>.parquet

run_summaries/eia_region_data/<run_id>.json
run_summaries/weather_data/<run_id>.json
run_summaries/demand_weather_features/<run_id>.json
```

Azure upload is controlled by environment variables:

```env
ENABLE_AZURE_UPLOAD=false
AZURE_STORAGE_CONNECTION_STRING=your_azure_storage_connection_string_here
AZURE_STORAGE_CONTAINER=electricity-pipeline
```

> [!IMPORTANT]
> Azure upload is disabled by default so local development, pytest, and GitHub Actions do not require cloud credentials.  
> The Azure connection string should only be stored in a local `.env` file or secure environment variable. It should never be committed to GitHub.

---

## 🧪 Validation Layer

The pipeline validates data before saving processed outputs.

| Check | What It Verifies |
|---|---|
| `check_not_empty` | DataFrame is not empty |
| `check_required_columns` | Required columns exist |
| `check_no_missing_values` | Missing values are counted and flagged |
| `check_timestamp_format` | `timestamp_utc` is a valid datetime column |
| `check_demand_values` | `demand_mwh` is numeric and non-negative |
| `check_temperature_values` | `temperature_2m` is numeric |
| `check_duplicate_timestamps_region` | No duplicate timestamp-region pairs exist |
| `check_merge_retention` | Merged dataset kept enough source rows |
| `check_hourly_timestamp_coverage` | Hourly timestamp range is continuous |

---

## ✅ Testing

The project includes a lightweight pytest suite for core local logic. Tests avoid live API calls and use small in-memory DataFrames or temporary folders.

### Current Test Coverage

| Test Area | Covered |
|---|---|
| EIA transformation | ✅ |
| Weather transformation | ✅ |
| Demand-weather merge logic | ✅ |
| Validation checks | ✅ |
| Run summary JSON writing | ✅ |
| Optional run summary metadata | ✅ |
| Raw storage JSON writing | ✅ |
| Partitioned CSV writer | ✅ |
| Partitioned Parquet writer | ✅ |

Run tests locally:

```bash
pytest
```

Live PostgreSQL and Azure checks are kept as manual scripts under `scripts/` instead of pytest tests, because CI should not require external services or secrets.

```text
scripts/manual_postgres_writer_check.py
scripts/manual_azure_upload_check.py
```

---

## 🔁 Orchestration

The project includes an initial Prefect flow wrapper for the end-to-end feature pipeline.

```bash
python -m src.orchestration.flows
```

The current Prefect flow is split into separate tasks for:

1. EIA ingestion
2. Weather ingestion
3. Feature dataset building

This gives Prefect more useful visibility into each stage of the pipeline.

---

## 🚀 Quickstart

### 1. Clone the Repository

```bash
git clone <repo-url>
cd Electricity-Demand-Data-Pipeline
```

### 2. Create a Virtual Environment

```bash
python -m venv venv
```

Activate it on Windows:

```bash
venv\Scripts\activate
```

Activate it on macOS/Linux:

```bash
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a local `.env` file or set the variables in your shell.

#### Required

```env
EIA_API_KEY=your_eia_api_key_here
```

#### Optional: PostgreSQL Load

```env
ENABLE_POSTGRES_LOAD=false
DATABASE_URL=postgresql+psycopg2://username:password@localhost:5432/electricity_pipeline
```

Set `ENABLE_POSTGRES_LOAD=true` only when a local PostgreSQL database is running and you want the feature pipeline to load the final dataset into PostgreSQL.

#### Optional: Azure Blob Upload

```env
ENABLE_AZURE_UPLOAD=false
AZURE_STORAGE_CONNECTION_STRING=your_azure_storage_connection_string_here
AZURE_STORAGE_CONTAINER=electricity-pipeline
```

Set `ENABLE_AZURE_UPLOAD=true` only when Azure Blob Storage is configured and you want the feature pipeline to upload generated artifacts.

> [!CAUTION]
> Do not commit real API keys, database URLs, or Azure connection strings.

---

## ▶️ Running the Pipeline

| Task | Command |
|---|---|
| Run EIA pipeline | `python -m src.pipeline.eia_pipeline` |
| Run weather pipeline | `python -m src.pipeline.weather_pipeline` |
| Run both source pipelines | `python -m src.pipeline.full_pipeline` |
| Run full feature pipeline | `python -m src.pipeline.feature_pipeline` |
| Run Prefect-orchestrated feature pipeline | `python -m src.orchestration.flows` |

The feature pipeline runs the EIA pipeline, runs the weather pipeline, saves raw API payloads and request metadata, saves processed source outputs, merges the processed outputs, validates the merged dataset, checks merge retention and hourly timestamp coverage, saves the final feature table as both CSV and Parquet, optionally upserts the final feature table into PostgreSQL, optionally uploads generated artifacts to Azure Blob Storage, and writes JSON run summaries for the source and feature datasets.

---

## 📊 Current Development Range

The current dataset covers one year of hourly data, producing approximately **8,760 merged feature rows** for the `NYIS` region.

```text
2025-01-01 00:00 UTC  →  2025-12-31 23:00 UTC
```

---

## 🧱 Development Roadmap

```mermaid
flowchart TD
    A[Core ingestion] --> B[Raw + processed storage]
    B --> C[Validation layer]
    C --> D[Feature merge]
    D --> E[Run summaries]
    E --> F[Parquet support]
    F --> G[PostgreSQL load]
    G --> H[Azure Blob upload]
    H --> I[Dashboard / analytics layer]
    I --> J[Forecasting-ready ML workflow]
    J --> K[Production deployment polish]
```

### Next Steps

1. Build a dashboard or analytics layer using the Parquet, PostgreSQL, or Azure-backed feature dataset.
2. Add a forecasting-ready ML workflow.
3. Add production deployment polish such as Docker or scheduled Prefect deployments.
4. Add more robust cloud and orchestration patterns only after the analytics layer is useful.

---

## 🧠 Why This Project Matters

This project is designed to look and behave like a real data engineering workflow:

- reproducible raw data storage
- separated ingestion, transformation, validation, and storage layers
- partitioned analytics outputs
- testable local logic
- cloud-ready artifact uploads
- database loading with upsert behavior
- orchestration visibility with Prefect
- clean future path toward dashboards and ML forecasting

<div align="center">

### Built as a portfolio-grade foundation for analytics, forecasting, and production data workflows.

</div>
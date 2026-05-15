from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

RAW_EIA_DIR = RAW_DIR / "eia_region_data"
RAW_WEATHER_DIR = RAW_DIR / "weather"

PROCESSED_EIA_DIR = PROCESSED_DIR / "eia_region_data"
PROCESSED_WEATHER_DIR = PROCESSED_DIR / "weather"
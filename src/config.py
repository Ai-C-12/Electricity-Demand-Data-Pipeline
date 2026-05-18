from dotenv import load_dotenv
import os

load_dotenv()

# EIA data parameters
DEFAULT_RESPONDENT = "NYIS"
DEFAULT_EIA_TYPE = "D"
EIA_START = "2025-01-01T00"
EIA_END = "2025-12-31T23"

# Weather data parameters
WEATHER_LATITUDE = 40.7128
WEATHER_LONGITUDE = -74.0060
WEATHER_START_DATE = "2025-01-01"
WEATHER_END_DATE = "2025-12-31"
WEATHER_HOURLY_VARIABLE = "temperature_2m"

# Source identifiers
EIA_SOURCE = "eia_region_data"
WEATHER_SOURCE = "weather_data"
FEATURE_SOURCE = "demand_weather_features"

# Load environment for API key
def get_eia_api_key() -> str:
    api_key = os.environ.get("EIA_API_KEY")

    if not api_key:
        raise ValueError("Missing EIA_API_KEY. Set it in your environment or .env file.")
    
    return api_key

# Load Database URL
DATABASE_URL = os.getenv("DATABASE_URL")
ENABLE_POSTGRES_LOAD = os.getenv("ENABLE_POSTGRES_LOAD", "false").lower() == "true"

# Azure Blob Storage config
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_STORAGE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "electricity-pipeline")
ENABLE_AZURE_UPLOAD = os.getenv("ENABLE_AZURE_UPLOAD", "false").lower() == "true"
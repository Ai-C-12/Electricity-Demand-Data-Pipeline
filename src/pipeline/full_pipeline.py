from src.pipeline.eia_pipeline import run_eia_pipeline
from src.pipeline.weather_pipeline import run_weather_pipeline

def run_full_pipeline() -> None:
    run_eia_pipeline()
    run_weather_pipeline()
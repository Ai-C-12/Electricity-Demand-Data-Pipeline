from src.pipeline.eia_pipeline import run_eia_pipeline
from src.pipeline.weather_pipeline import run_historical_weather_pipeline, run_archived_forecast_weather_pipeline
from src.pipeline.feature_pipeline import build_feature_dataset, build_forecasting_input_dataset

def run_full_pipeline() -> None:
    demand_df = run_eia_pipeline()
    historical_weather_df = run_historical_weather_pipeline()
    forecast_weather_df = run_archived_forecast_weather_pipeline()

    build_feature_dataset(demand_df=demand_df, weather_df=historical_weather_df)
    build_forecasting_input_dataset(demand_df=demand_df, forecast_weather_df=forecast_weather_df)

if __name__ == "__main__":
    run_full_pipeline()
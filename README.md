# Electricity Demand Data Pipeline

End-to-end data pipeline ingesting electricity demand and weather data.

## Function
- fetch EIA electricity demand data
- fetch Open-Meteo weather data
- save raw API payloads
- save request metadata
- transform raw data into processed datasets
- merge demand and weather into an analytics-ready feature table
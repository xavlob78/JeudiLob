import argparse
import dlt
import requests
from urllib.parse import quote_plus

API_KEY = dlt.secrets.get("OPENWEATHERMAP_API_KEY")

CURRENT_WEATHER_URL_TEMPLATE = dlt.config.get("sources.openweather.base_url")+"/weather?q={city}&appid={key}"
FORECAST_URL_TEMPLATE = dlt.config.get("sources.openweather.base_url")+"/forecast?q={city}&appid={key}"

@dlt.resource(name="current_weather", write_disposition="append")
def get_current_weather(city: str):
    url = CURRENT_WEATHER_URL_TEMPLATE.format(city=quote_plus(city), key=API_KEY)
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    data["city"] = city
    yield data

@dlt.resource(name="forecasts", write_disposition="append")
def get_forecasts(city: str):
    url = FORECAST_URL_TEMPLATE.format(city=quote_plus(city), key=API_KEY)
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    for forecast in data.get("list", []):
        forecast["city"] = city
        yield forecast

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DLT OpenWeatherMap pipeline multi-ville")
    parser.add_argument("--cities", default="Montrouge", help="Liste de villes séparées par des virgules, ex: Paris,Lyon,Marseille")
    args = parser.parse_args()

    cities = [c.strip() for c in args.cities.split(",") if c.strip()]

    pipeline = dlt.pipeline(
        pipeline_name="weather_pipeline",
        destination=dlt.destinations.duckdb("./Db/meteo.duckdb"),
        dataset_name="weather_data",
        progress="tqdm"
    )

    for city in cities:
        print(f"Chargement météo pour {city}...")

        load_info_current = pipeline.run(get_current_weather(city))
        print(load_info_current)

        load_info_forecast = pipeline.run(get_forecasts(city))
        print(load_info_forecast)

    print("Terminé.")
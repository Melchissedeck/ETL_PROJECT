import pathlib
from datetime import datetime, date
import calendar
import requests
import pandas as pd

from src.utils.logger import get_logger

BASE_DIR = pathlib.Path(__file__).resolve().parents[3]
OUTPUT_DIR = BASE_DIR / "data" / "raw"
logger = get_logger(__name__)

EUROPE_CITIES = [
    {"country": "France", "city": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"country": "Germany", "city": "Berlin", "lat": 52.52, "lon": 13.4050},
    {"country": "Spain", "city": "Madrid", "lat": 40.4168, "lon": -3.7038},
    {"country": "Italy", "city": "Rome", "lat": 41.9028, "lon": 12.4964},
    {"country": "Belgium", "city": "Brussels", "lat": 50.8503, "lon": 4.3517},
    {"country": "Netherlands", "city": "Amsterdam", "lat": 52.3676, "lon": 4.9041},
    {"country": "Portugal", "city": "Lisbon", "lat": 38.7223, "lon": -9.1393},
    {"country": "Switzerland", "city": "Bern", "lat": 46.9480, "lon": 7.4474},
    {"country": "Austria", "city": "Vienna", "lat": 48.2082, "lon": 16.3738},
    {"country": "Sweden", "city": "Stockholm", "lat": 59.3293, "lon": 18.0686},
    {"country": "Norway", "city": "Oslo", "lat": 59.9139, "lon": 10.7522},
    {"country": "Finland", "city": "Helsinki", "lat": 60.1699, "lon": 24.9384},
    {"country": "Denmark", "city": "Copenhagen", "lat": 55.6761, "lon": 12.5683},
    {"country": "Ireland", "city": "Dublin", "lat": 53.3498, "lon": -6.2603},
    {"country": "Poland", "city": "Warsaw", "lat": 52.2297, "lon": 21.0122},
    {"country": "Czech Republic", "city": "Prague", "lat": 50.0755, "lon": 14.4378},
    {"country": "Hungary", "city": "Budapest", "lat": 47.4979, "lon": 19.0402},
    {"country": "Greece", "city": "Athens", "lat": 37.9838, "lon": 23.7275},
    {"country": "Romania", "city": "Bucharest", "lat": 44.4268, "lon": 26.1025},
    {"country": "Bulgaria", "city": "Sofia", "lat": 42.6977, "lon": 23.3219},
    {"country": "Croatia", "city": "Zagreb", "lat": 45.8150, "lon": 15.9819},
    {"country": "Serbia", "city": "Belgrade", "lat": 44.7866, "lon": 20.4489},
    {"country": "Slovakia", "city": "Bratislava", "lat": 48.1486, "lon": 17.1077},
    {"country": "Slovenia", "city": "Ljubljana", "lat": 46.0511, "lon": 14.5051},
    {"country": "Luxembourg", "city": "Luxembourg", "lat": 49.6116, "lon": 6.1319},
    {"country": "Iceland", "city": "Reykjavik", "lat": 64.1466, "lon": -21.9426},
    {"country": "Estonia", "city": "Tallinn", "lat": 59.4370, "lon": 24.7536},
    {"country": "Latvia", "city": "Riga", "lat": 56.9496, "lon": 24.1052},
    {"country": "Lithuania", "city": "Vilnius", "lat": 54.6872, "lon": 25.2797},
    {"country": "Malta", "city": "Valletta", "lat": 35.8989, "lon": 14.5146},
    {"country": "Cyprus", "city": "Nicosia", "lat": 35.1856, "lon": 33.3823},
]

# fenêtre de 3 ans
START_DATE = date(2022, 11, 6)   # à adapter
END_DATE = date(2025, 11, 6)     # aujourd'hui


def month_range(start: date, end: date):
    """Génère (start_of_month, end_of_month) entre 2 dates."""
    year = start.year
    month = start.month
    while True:
        last_day = calendar.monthrange(year, month)[1]
        start_m = date(year, month, 1)
        end_m = date(year, month, last_day)
        if end_m > end:
            end_m = end
        yield start_m, end_m
        if end_m >= end:
            break
        # incrément mois
        month += 1
        if month > 12:
            month = 1
            year += 1


def fetch_weather_archive(lat: float, lon: float, start_d: date, end_d: date) -> pd.DataFrame:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "hourly": (
            "temperature_2m,relativehumidity_2m,precipitation,cloudcover,"
            "windspeed_10m,winddirection_10m,pressure_msl,dewpoint_2m,visibility"
        ),
        "timezone": "auto",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return pd.DataFrame(data.get("hourly", {}))


def fetch_air_quality(lat: float, lon: float, start_d: date, end_d: date) -> pd.DataFrame:
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "hourly": (
            "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,"
            "ozone,european_aqi,european_aqi_pm2_5,european_aqi_pm10"
        ),
        "timezone": "auto",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return pd.DataFrame(data.get("hourly", {}))


def main():
    all_frames = []

    for city in EUROPE_CITIES:
        country = city["country"]
        city_name = city["city"]
        lat = city["lat"]
        lon = city["lon"]

        logger.info(f"[CITY] {city_name}, {country}")

        # on parcourt chaque mois de la fenêtre
        for start_m, end_m in month_range(START_DATE, END_DATE):
            logger.info(f"  > période {start_m} -> {end_m}")

            try:
                meteo_df = fetch_weather_archive(lat, lon, start_m, end_m)
                air_df = fetch_air_quality(lat, lon, start_m, end_m)

                # enrichir
                meteo_df["country"] = country
                meteo_df["city"] = city_name

                air_df["country"] = country
                air_df["city"] = city_name

                merged = pd.merge(
                    meteo_df,
                    air_df,
                    on=["time", "country", "city"],
                    how="inner"
                )
                all_frames.append(merged)
            except Exception as e:
                logger.error(f"Erreur {city_name} {start_m} -> {end_m}: {e}")

    if not all_frames:
        logger.error("Aucune donnée récupérée sur 3 ans.")
        return

    final_df = pd.concat(all_frames, ignore_index=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_path = OUTPUT_DIR / f"open_meteo_europe_weather_pollution_3years_{ts}.csv"
    final_df.to_csv(out_path, index=False)
    logger.info(f"Fichier 3 ans sauvegardé : {out_path}")


if __name__ == "__main__":
    main()

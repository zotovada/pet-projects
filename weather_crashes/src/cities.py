import pandas as pd
import os
import json
import requests
import src.config
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import logging

# ================================
# Получение городов
# ================================

def fetch_cities(url):
    """
    Загружает таблицы городов с указанного URL.
    Возвращает DataFrame с исходными колонками.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; DataParser/1.0; +https://example.com)"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    tables = pd.read_html(response.text)
    cities_df = tables[0]  # Берем первую таблицу

    logging.info(f"fetch_cities: загружено {len(cities_df)} городов с колонками: {list(cities_df.columns)}")
    return cities_df

# ================================
# Координаты городов
# ================================

def normalize_city_region(cities_df):
    """
    Нормализует названия городов и регионов
    """
    cities_df["city"] = cities_df["city"].str.replace(r"не призн\.$", "", regex=True).str.strip()
    cities_df["region"] = cities_df["region"].str.replace(r"\bАО\b$", "автономный округ", regex=True).str.strip()
    return cities_df

def load_or_create_cache(pickle_file):
    """
    Загружает или создаёт pickle-файл с координатами городов
    """
    if os.path.exists(pickle_file):
        cities_cached = pd.read_pickle(pickle_file)
        logging.info(f"load_or_create_cache: загружено {len(cities_cached)} городов из pkl")
    else:
        cities_cached = pd.DataFrame(columns=["city", "region", "lat", "lon"])
        logging.info("load_or_create_cache: pkl не найден, будет создан заново")
    return cities_cached

def geocode_new_cities(cities_df, cities_cached):
    """
    Геокодирование новых городов и обновление кэша
    """
    new_cities = cities_df.merge(
        cities_cached[["city"]],
        on="city",
        how="left",
        indicator=True
    )
    new_cities = new_cities[new_cities["_merge"] == "left_only"].drop(columns="_merge")
    logging.info(f"geocode_new_cities: новых городов для геокодинга: {len(new_cities)}")

    if not new_cities.empty:
        geolocator = Nominatim(user_agent="cities_coordinates", timeout=10)
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, max_retries=3, error_wait_seconds=5)

        def get_coords(city, region):
            try:
                location = geocode(f"{city}, {region}, Россия")
                if location:
                    return location.latitude, location.longitude
                return None, None
            except Exception as e:
                logging.error(f"geocode_new_cities: ошибка геокодирования {city}: {e}")
                return None, None

        new_cities[["lat", "lon"]] = pd.DataFrame(
            new_cities.apply(lambda r: get_coords(r["city"], r["region"]), axis=1).tolist(),
            index=new_cities.index
        )

        cities_df = pd.concat([cities_cached, new_cities], ignore_index=True)
    else:
        cities_df = cities_cached
        logging.info("geocode_new_cities: новых городов нет, pkl актуален")

    return cities_df

def add_manual_coords(cities_df):
    """
    Добавление координат вручную для проблемных городов
    """
    manual_coords = {
        "Алупка": (44.4180, 34.0450),
        "Армянск": (45.1350, 33.5980),
        "Бахчисарай": (44.7500, 33.8600),
        "Белогорск": (50.9170, 128.4670),
        "Биробиджан": (48.8000, 132.9330),
        "Судак": (44.8500, 34.9800),
        "Щёлкино": (45.2817, 35.7973),
        "Облучье": (49.0000, 131.0500),
        "Обнинск": (55.0968, 36.6101),
        "Суджа": (51.1976, 35.2723)
    }

    for city, (lat, lon) in manual_coords.items():
        mask = cities_df["city"] == city
        cities_df.loc[mask, "lat"] = lat
        cities_df.loc[mask, "lon"] = lon

    logging.info("add_manual_coords: ручные координаты добавлены")
    return cities_df

# ================================
# Коды ГИБДД
# ================================

JSON_FILE = src.config.REGIONS_JSON_FILE

def fetch_regions_from_gibdd():
    """
    Запрос регионов и районов из API ГИБДД и сохранение в JSON
    """
    logging.info("fetch_regions_from_gibdd: файл не найден, запрашиваем данные с API ГИБДД")

    now = datetime.now()
    year = now.year
    month = now.month - 1 if now.month > 1 else 12
    if month == 12:
        year -= 1

    url = "http://stat.gibdd.ru/map/getMainMapData"
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
    rf_payload = {
        "maptype": 1,
        "region": "877",
        "date": f'["MONTHS:{month}.{year}"]',
        "pok": "1",
    }

    response = requests.post(url, json=rf_payload, headers=headers, timeout=30)
    response.raise_for_status()

    result = response.json()
    metabase = json.loads(result["metabase"])
    maps_data = json.loads(metabase[0]["maps"])

    regions = [{"id": r["id"], "name": r["name"], "districts": []} for r in maps_data]

    for region in regions:
        region_payload = {
            "maptype": 1,
            "region": region["id"],
            "date": f'["MONTHS:{month}.{year}"]',
            "pok": "1",
        }
        reg_response = requests.post(url, json=region_payload, headers=headers, timeout=30)
        reg_response.raise_for_status()
        reg_result = reg_response.json()
        reg_metabase = json.loads(reg_result["metabase"])
        reg_maps_data = json.loads(reg_metabase[0]["maps"])
        region["districts"] = [{"id": d["id"], "name": d["name"]} for d in reg_maps_data]

    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(regions, f, ensure_ascii=False, indent=2)

    logging.info(f"fetch_regions_from_gibdd: файл создан {JSON_FILE}")
    return regions

def load_regions_json():
    """
    Загружает regions_all.json, если нет — создаёт через API
    """
    if os.path.exists(JSON_FILE):
        logging.info(f"load_regions_json: файл {JSON_FILE} найден, читаем его")
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            regions_all = json.load(f)
    else:
        regions_all = fetch_regions_from_gibdd()

    logging.info(f"load_regions_json: загружено регионов {len(regions_all)}")
    return regions_all

def add_gibdd_codes(cities_df):
    """
    Добавляет district_id и region_id для всех городов
    """
    regions_all = load_regions_json()

    def remove_municipality_prefix(name):
        prefixes = ["г. ", "г.", "ГО "]
        for p in prefixes:
            if name.startswith(p):
                return name[len(p):]
        return name

    def normalize_region_name(name):
        if name.startswith("Республика "):
            name = name[len("Республика "):]
        return name

    def city_matches(city, muni_name):
        muni_name = remove_municipality_prefix(muni_name)
        city = city.strip()
        if city == muni_name:
            return True
        suffixes = ["ский район", "ий район"]
        for suf in suffixes:
            if muni_name.startswith(city) and muni_name[len(city):].startswith(suf):
                return True
        return False

    region_dict = {normalize_region_name(r["name"]).lower(): r for r in regions_all}

    cities_df["district_id"] = None
    cities_df["region_id"] = None

    for idx, row in cities_df.iterrows():
        city = row["city"]
        region_name = row["region"].lower()
        region_info = region_dict.get(region_name)
        if not region_info:
            logging.warning(f"add_gibdd_codes: регион '{row['region']}' не найден")
            continue

        found = False
        for municipality in region_info["districts"]:
            if city_matches(city, municipality["name"]):
                district_id = str(municipality["id"]).zfill(5)
                region_id = str(region_info["id"]).zfill(2)
                cities_df.at[idx, "district_id"] = district_id
                cities_df.at[idx, "region_id"] = region_id
                found = True
                break

        if not found:
            logging.warning(f"add_gibdd_codes: город '{row['city']}' в регионе '{row['region']}' не найден")

    logging.info("add_gibdd_codes: коды ГИБДД добавлены")
    return cities_df




import logging
import pandas as pd
import numpy as np
import requests_cache
import src.config
import openmeteo_requests

from src.load import insert_to_buffer, load_from_supabase
from src.bootstrap import init_logging, init_supabase
from retry_requests import retry


# Этап 0. Инициализация
init_logging()
supabase = init_supabase()
logging.info("update_weather_buffer: запуск")


try:
    # Этап 1. Загрузка списка городов
    cities_df = load_from_supabase("cities", supabase=supabase)
    target_cities_df = cities_df[cities_df['city'].isin(src.config.TARGET_CITIES)].copy()
    logging.info(f"update_weather_buffer: целевых городов: {len(target_cities_df)}")

    # Этап 2. Настройка клиента API с кэшированием и повторными попытками
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Этап 3. Обработка каждого города
    for _, row in target_cities_df.iterrows():
        city = row['city']
        region = row['region']
        lat = row['lat']
        lon = row['lon']

        logging.info(f"update_weather_buffer: обработка города {city} ({region})")

        # Этап 3.1. Определение периода дозагрузки
        last_row = (
            supabase.table("weather_buffer")
                   .select("raw_json")
                   .eq("city", city)
                   .eq("region", region)
                   .order("id", desc=True)
                   .limit(1)
                   .execute()
        )

        if last_row.data:
            raw_json = last_row.data[0]["raw_json"]
            if raw_json.get("is_error"):
                last_loaded_dt = pd.to_datetime(src.config.START_DATE)
            else:
                last_loaded_dt = pd.to_datetime(raw_json["date"])
        else:
            last_loaded_dt = pd.to_datetime(src.config.START_DATE)

        start_dt = last_loaded_dt + pd.Timedelta(hours=1)
        end_dt = pd.Timestamp.utcnow().floor('D') - pd.Timedelta(hours=1)

        start_date = start_dt.strftime("%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")

        logging.info(f"update_weather_buffer: {city}, период {start_date} — {end_date}")

        if pd.to_datetime(start_date) > pd.to_datetime(end_date):
            logging.info(f"update_weather_buffer: {city}, новых данных нет")
            continue

        try:
            # Этап 3.2. Запрос данных из API
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": src.config.WEATHER_VARIABLES,
                "start_date": start_date,
                "end_date": end_date,
                "timezone": "auto"
            }

            url = "https://archive-api.open-meteo.com/v1/archive"
            responses = openmeteo.weather_api(url, params=params)

            response = responses[0]
            hourly = response.Hourly()

            # Этап 3.3. Формирование DataFrame
            hourly_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time() + response.UtcOffsetSeconds(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd() + response.UtcOffsetSeconds(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                )
            }

            for i, var_name in enumerate(src.config.WEATHER_VARIABLES):
                hourly_data[var_name] = hourly.Variables(i).ValuesAsNumpy()

            df = pd.DataFrame(hourly_data)

            # Этап 3.4. Подготовка записей для буфера
            buffer_rows = []
            for _, r in df.iterrows():
                row_dict = r.to_dict()
                row_dict["date"] = row_dict["date"].strftime("%Y-%m-%dT%H:%M:%SZ")

                for k, v in row_dict.items():
                    if isinstance(v, np.ndarray):
                        row_dict[k] = v.tolist()
                    elif isinstance(v, (np.integer, np.floating)):
                        row_dict[k] = v.item()

                buffer_rows.append({
                    "city": city,
                    "region": region,
                    "raw_json": row_dict,
                    "is_error": False
                })

            # Этап 3.5. Вставка данных в буфер
            buffer_df = pd.DataFrame(buffer_rows)
            insert_to_buffer(buffer_df, supabase, "weather_buffer", chunk_size=src.config.CHUNK_SIZE)
            logging.info(f"update_weather_buffer: {city}, добавлено строк: {len(buffer_df)}")

        except Exception as e:
            logging.exception(f"update_weather_buffer: ошибка загрузки для {city}")
            error_df = pd.DataFrame([{
                "city": city,
                "region": region,
                "raw_json": {"error": str(e), "is_error": True},
                "is_error": True
            }])
            insert_to_buffer(error_df, supabase, "weather_buffer", chunk_size=1)

    logging.info("update_weather_buffer: завершено успешно")

except Exception:
    logging.exception("update_weather_buffer: завершено с ошибкой")
    raise
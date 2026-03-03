import pandas as pd
import requests
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging

import src.config
from src.load import insert_to_buffer, load_from_supabase
from src.bootstrap import init_logging, init_supabase


# Этап 0. Инициализация
init_logging()
supabase = init_supabase()
logging.info("update_crashes_buffer: запуск")


try:
    # Этап 1. Загрузка списка городов
    cities_df = load_from_supabase("cities", supabase=supabase)
    target_cities_df = cities_df[cities_df['city'].isin(src.config.TARGET_CITIES)].copy()
    logging.info(f"update_crashes_buffer: целевых городов: {len(target_cities_df)}")

    # Этап 2. Функция получения карточек ДТП
    def get_dtp_cards(region_id, district_id, year, month, start=1, end=100):
        """
        Запрос карточек ДТП за конкретный месяц.
        Возвращает список карточек или пустой список при ошибке.
        """
        url = "http://stat.gibdd.ru/map/getDTPCardData"
        payload = {
            "data": {
                "date": [f"MONTHS:{month}.{year}"],
                "ParReg": region_id,
                "order": {"type": "1", "fieldName": "dat"},
                "reg": district_id,
                "ind": "1",
                "st": str(start),
                "en": str(end),
                "fil": {"isSummary": False},
                "fieldNames": [
                    "dat", "time", "coordinates", "infoDtp", "k_ul", "dor", "ndu",
                    "k_ts", "ts_info", "pdop", "pog", "osv", "s_pch", "s_pog",
                    "n_p", "n_pg", "obst", "sdor", "t_osv", "t_p", "t_s", "v_p", "v_v"
                ]
            }
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        try:
            request_data = {"data": json.dumps(payload["data"], separators=(',', ':'))}
            response = requests.post(url, json=request_data, headers=headers, timeout=30)

            if response.status_code == 200:
                response_data = json.loads(response.text)
                return json.loads(response_data["data"]).get("tab", [])
            else:
                logging.warning(f"update_crashes_buffer: HTTP {response.status_code}")
                return []
        except Exception as e:
            logging.exception(f"update_crashes_buffer: ошибка запроса данных: {str(e)}")
            return []

    # Этап 3. Обработка каждого города
    for _, row in target_cities_df.iterrows():
        city = row['city']
        region = row['region']

        logging.info(f"update_crashes_buffer: обработка города {city} ({region})")

        # Этап 3.1. Определение периода дозагрузки
        last_date_response = (
            supabase.table("crashes_buffer")
            .select("raw_json")
            .eq("city", city)
            .eq("region", region)
            .order("raw_json->>year", desc=True)
            .order("raw_json->>month", desc=True)
            .limit(1)
            .execute()
        )

        if last_date_response.data:
            last_json = last_date_response.data[0]["raw_json"]
            last_year = last_json.get("year", pd.to_datetime(src.config.START_DATE).year)
            last_month = last_json.get("month", pd.to_datetime(src.config.START_DATE).month)
            last_loaded_date = datetime(year=int(last_year), month=int(last_month), day=1)
            start_date = last_loaded_date + relativedelta(months=1)
        else:
            start_date = pd.to_datetime(src.config.START_DATE)

        end_date = datetime.utcnow().replace(day=1) - relativedelta(months=1)

        logging.info(f"update_crashes_buffer: {city}, период {start_date.date()} — {end_date.date()}")

        if start_date > end_date:
            logging.info(f"update_crashes_buffer: {city}, новых данных нет")
            continue

        current_dt = start_date
        while current_dt <= end_date:
            year = current_dt.year
            month = current_dt.month

            try:
                cards = get_dtp_cards(row['region_id'], row['district_id'], year, month)

                if not cards:
                    logging.info(f"update_crashes_buffer: {city}, нет ДТП за {month}.{year}")
                    current_dt += relativedelta(months=1)
                    continue

                buffer_payload = {
                    "year": year,
                    "month": month,
                    "cards_count": len(cards),
                    "cards": cards
                }

                buffer_df = pd.DataFrame([{
                    "city": city,
                    "region": region,
                    "raw_json": buffer_payload,
                    "is_error": False
                }])
                insert_to_buffer(buffer_df, supabase, "crashes_buffer", chunk_size=1)

                logging.info(f"update_crashes_buffer: {city}, {month}.{year}, ДТП: {len(cards)}")

            except Exception as e:
                error_payload = {"year": year, "month": month, "error": str(e)}
                error_df = pd.DataFrame([{
                    "city": city,
                    "region": region,
                    "raw_json": error_payload,
                    "is_error": True
                }])
                insert_to_buffer(error_df, supabase, "crashes_buffer", chunk_size=1)
                logging.exception(f"update_crashes_buffer: ошибка {month}.{year} для {city}")

            current_dt += relativedelta(months=1)

    logging.info("update_crashes_buffer: завершено успешно")

except Exception:
    logging.exception("update_crashes_buffer: завершено с ошибкой")
    raise
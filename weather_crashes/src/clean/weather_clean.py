import pandas as pd
import logging
import src.config

from src.bootstrap import init_logging, init_supabase
from src.load import upsert_clean, get_last_processed_id, update_last_processed_id


# Этап 0. Инициализация
init_logging()
supabase = init_supabase()

PIPELINE_NAME = "weather"

logging.info("update_weather_clean: запуск")


try:
    batch_size = 500
    insert_batch_size = src.config.CHUNK_SIZE
    all_rows_processed = 0

    wmo_df = pd.read_csv('data/wmo_weather_codes.csv')

    # Этап 1. Определение позиции последней обработанной записи
    last_id = get_last_processed_id(supabase, PIPELINE_NAME)
    if last_id is None:
        last_id = 0

    while True:
        # Этап 2. Загрузка очередного батча из buffer
        response = (
            supabase.table("weather_buffer")
            .select("*")
            .gt("id", last_id)
            .order("id")
            .limit(batch_size)
            .execute()
        )

        rows = response.data or []
        if not rows:
            break

        buffer_df = pd.DataFrame(rows)
        if buffer_df.empty:
            last_id = rows[-1]['id']
            update_last_processed_id(supabase, PIPELINE_NAME, last_id)
            continue

        # Этап 3. Преобразование raw_json в clean-структуру
        all_records = []

        for _, row in buffer_df.iterrows():
            raw = row['raw_json']
            if not isinstance(raw, dict):
                continue

            record = {
                "city": row['city'],
                "region": row['region'],
                "weather_date": raw.get("date"),
                "temperature_2m": raw.get("temperature_2m"),
                "weather_code": raw.get("weather_code"),
                "rain": raw.get("rain"),
                "snowfall": raw.get("snowfall"),
                "wind_speed_10m": raw.get("wind_speed_10m"),
                "is_day": raw.get("is_day"),
                "precipitation": raw.get("precipitation"),
                "dew_point_2m": raw.get("dew_point_2m")
            }

            # Этап 3.1. Разделение даты и времени
            if record["weather_date"]:
                dt = pd.to_datetime(record["weather_date"], errors='coerce')
                record["weather_date"] = dt.strftime('%Y-%m-%d') if pd.notnull(dt) else None
                record["weather_time"] = dt.strftime('%H:%M:%S') if pd.notnull(dt) else None
            else:
                record["weather_time"] = None

            all_records.append(record)

        if all_records:
            clean_df = pd.DataFrame(all_records)
            clean_df = clean_df.merge(wmo_df, on='weather_code', how='left')

            # Этап 4. Очистка и дедупликация
            clean_df = clean_df.dropna(subset=["city", "region", "weather_date", "weather_time"])

            if clean_df.empty:
                last_id = rows[-1]['id']
                update_last_processed_id(supabase, PIPELINE_NAME, last_id)
                continue

            clean_df = clean_df.drop_duplicates(
                subset=["city", "region", "weather_date", "weather_time"],
                keep="last"
            )

            # Этап 5. Upsert в clean-таблицу
            upsert_clean(
                clean_df,
                supabase,
                table_name="weather",
                on_conflict_columns=["city", "region", "weather_date", "weather_time"],
                chunk_size=insert_batch_size
            )

            all_rows_processed += len(clean_df)
            logging.info(f"update_weather_clean: добавлено строк: {len(clean_df)}")

        # Этап 6. Обновление прогресса обработки
        last_id = rows[-1]['id']
        update_last_processed_id(supabase, PIPELINE_NAME, last_id)

    logging.info(
        f"update_weather_clean: завершено успешно, всего обработано: {all_rows_processed}"
    )

except Exception:
    logging.exception("update_weather_clean: завершено с ошибкой")
    raise
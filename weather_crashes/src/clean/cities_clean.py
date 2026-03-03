import logging
import pandas as pd
import os
import src.config

from src.bootstrap import init_logging, init_supabase
from src.load import upsert_clean, load_from_supabase
from src.cities import (
    normalize_city_region,
    load_or_create_cache,
    geocode_new_cities,
    add_manual_coords,
    add_gibdd_codes
)


# Этап 0. Инициализация
init_logging()
supabase = init_supabase()
logging.info("update_cities_clean: запуск")

PICKLE_FILE = src.config.CITIES_CACHE_FILE


try:
    # Этап 1. Загрузка данных из буферной таблицы
    buffer_df = load_from_supabase("cities_buffer", supabase)
    logging.info(f"update_cities_clean: загружено строк из buffer: {len(buffer_df)}")

    # Этап 2. Нормализация названий города и региона
    cities_df = buffer_df.rename(columns={
        "Город": "city",
        "Регион": "region"
    })[["city", "region"]]

    cities_df = normalize_city_region(cities_df)

    # Этап 3. Обогащение координатами
    cities_cached = load_or_create_cache(PICKLE_FILE)

    coords_df = geocode_new_cities(cities_df, cities_cached)
    coords_df = add_manual_coords(coords_df)

    coords_df.to_pickle(PICKLE_FILE)
    logging.info("update_cities_clean: кэш координат обновлён")

    # Этап 4. Добавление кодов ГИБДД
    final_df = add_gibdd_codes(coords_df)

    # Этап 5. Формирование итогового набора колонок
    final_df = final_df[[
        "city",
        "region",
        "lat",
        "lon",
        "district_id",
        "region_id"
    ]]

    # Этап 6. Загрузка данных в clean-таблицу
    upsert_clean(
        df=final_df,
        supabase=supabase,
        table_name="cities",
        on_conflict_columns=["city", "region"],
        chunk_size=500
    )

    logging.info(f"update_cities_clean: завершено успешно, записей: {len(final_df)}")

except Exception:
    logging.exception("update_cities_clean: завершено с ошибкой")
    raise
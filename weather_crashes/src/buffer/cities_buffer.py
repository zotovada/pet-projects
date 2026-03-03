import logging
import pandas as pd
import numpy as np
import src.config

from src.cities import fetch_cities
from src.load import insert_to_buffer
from src.bootstrap import init_logging, init_supabase


# Этап 0. Инициализация
init_logging()
supabase = init_supabase()
logging.info("update_cities_buffer: запуск")


# Этап 1. Очистка буферной таблицы перед полной перезагрузкой
supabase.rpc("truncate_table", {"p_table_name": "cities_buffer"}).execute()
logging.info("update_cities_buffer: таблица cities_buffer очищена")


def sanitize_for_json(row):
    """
    Приведение значений строки DataFrame к JSON-сериализуемым типам
    для корректной записи в raw_json.
    """
    row_dict = row.to_dict()
    for k, v in row_dict.items():
        if pd.isna(v):
            row_dict[k] = None
        elif isinstance(v, (np.integer, np.floating)):
            try:
                row_dict[k] = float(v)
            except OverflowError:
                row_dict[k] = None
        else:
            if not isinstance(v, (str, dict, list, type(None))):
                row_dict[k] = str(v)
    return row_dict


try:
    # Этап 2. Загрузка исходных данных
    url = "https://ru.wikipedia.org/wiki/Список_городов_России"
    raw_df = fetch_cities(url)
    logging.info(f"update_cities_buffer: загружено строк: {len(raw_df)}")

    # Этап 3. Формирование буферных записей
    buffer_rows = []
    for _, row in raw_df.iterrows():
        try:
            buffer_rows.append({
                "city": row.get("Город", "unknown"),
                "region": row.get("Регион", "unknown"),
                "raw_json": sanitize_for_json(row),
                "is_error": False
            })
        except Exception as e_row:
            logging.warning(f"update_cities_buffer: ошибка подготовки строки: {e_row}")
            buffer_rows.append({
                "city": "unknown",
                "region": "unknown",
                "raw_json": {"error": str(e_row)},
                "is_error": True
            })

    buffer_df = pd.DataFrame(buffer_rows)

    # Этап 4. Вставка данных в буферную таблицу
    insert_to_buffer(buffer_df, supabase, "cities_buffer")
    logging.info(f"update_cities_buffer: завершено успешно, записей: {len(buffer_df)}")

except Exception as e:
    # Этап 5. Глобальная ошибка — запись в буфер с флагом is_error
    logging.exception("update_cities_buffer: критическая ошибка выполнения")
    error_df = pd.DataFrame([{
        "city": "unknown",
        "region": "unknown",
        "raw_json": {"error": str(e)},
        "is_error": True
    }])
    insert_to_buffer(error_df, supabase, "cities_buffer", chunk_size=1)
    logging.info("update_cities_buffer: информация об ошибке записана в буфер")
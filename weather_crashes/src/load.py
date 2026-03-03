import logging
import pandas as pd
import src.config
from datetime import datetime, timezone
from src.bootstrap import init_logging, init_supabase

# ================================
# Инициализация
# ================================
init_logging()
supabase = init_supabase()
logging.info("load: запуск ETL-функций")


def load_from_supabase(table_name, supabase, step=1000):
    """
    Загружает все строки из Supabase с постраничной навигацией (по step),
    чтобы обойти ограничение бесплатного REST API (макс 1000 строк за раз).
    """
    try:
        all_data = []
        offset = 0

        while True:
            response = (
                supabase.table(table_name)
                        .select("*")
                        .range(offset, offset + step - 1)
                        .execute()
            )
            data = response.data or []
            all_data.extend(data)

            if len(data) < step:
                break  # дошли до конца таблицы

            offset += step

        df = pd.DataFrame(all_data)
        logging.info(f"load_from_supabase: таблица '{table_name}' загружена, строк: {len(df)}")
        return df

    except Exception as e:
        logging.exception(f"load_from_supabase: ошибка загрузки таблицы '{table_name}'")
        raise


def insert_to_buffer(df, supabase, table_name, chunk_size=None):
    """
    Универсальная функция для записи сырых данных в буферную таблицу Supabase.
    Все остальные данные сохраняются в raw_json.
    """
    now = datetime.now(timezone.utc).isoformat()
    records = []

    for _, row in df.iterrows():
        raw_dict = row.to_dict()
        raw_json = raw_dict.get("raw_json", {})  # берём только raw_json

        records.append({
            "city": row.get("city"),
            "region": row.get("region"),
            "raw_json": raw_json,
            "is_error": bool(raw_json.get("is_error", False)),
            "date_update": now
        })

    if chunk_size:
        for i in range(0, len(records), chunk_size):
            supabase.table(table_name).insert(records[i:i + chunk_size]).execute()
    else:
        supabase.table(table_name).insert(records).execute()

    logging.info(f"insert_to_buffer: вставлено строк в '{table_name}': {len(records)}")


def upsert_clean(df, supabase, table_name, on_conflict_columns=None, chunk_size=None):
    """
    Универсальная функция для записи чистой таблицы в Supabase.
    Обновляет строки по ключам on_conflict_columns или вставляет новые.
    """
    if df.empty:
        logging.info(f"upsert_clean: DataFrame пуст, таблица '{table_name}' не обновлена")
        return

    records = df.to_dict(orient="records")
    conflict = ",".join(on_conflict_columns) if on_conflict_columns else None

    if chunk_size:
        for i in range(0, len(records), chunk_size):
            supabase.table(table_name).upsert(
                records[i:i + chunk_size],
                on_conflict=conflict
            ).execute()
    else:
        supabase.table(table_name).upsert(records, on_conflict=conflict).execute()

    logging.info(f"upsert_clean: таблица '{table_name}' обновлена, строк: {len(records)}")


def get_last_processed_id(supabase, pipeline_name):
    """
    Получает последний обработанный id для конкретного pipeline.
    Если запись отсутствует — создаёт с last_processed_id = 0
    """
    response = (
        supabase.table("clean_progress")
        .select("last_processed_id")
        .eq("table_name", pipeline_name)
        .execute()
    )

    data = response.data
    if not data:
        supabase.table("clean_progress").insert({
            "table_name": pipeline_name,
            "last_processed_id": 0
        }).execute()
        logging.info(f"get_last_processed_id: создана запись для '{pipeline_name}' с last_processed_id=0")
        return 0

    return data[0]["last_processed_id"]


def update_last_processed_id(supabase, pipeline_name, last_id):
    """
    Обновляет последний обработанный id для конкретного pipeline.
    """
    supabase.table("clean_progress").upsert({
        "table_name": pipeline_name,
        "last_processed_id": last_id,
        "updated_at": datetime.utcnow().isoformat()
    }).execute()
    logging.info(f"update_last_processed_id: '{pipeline_name}' обновлён до last_processed_id={last_id}")
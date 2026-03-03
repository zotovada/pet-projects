import pandas as pd
import logging
import src.config

from src.bootstrap import init_logging, init_supabase
from src.load import upsert_clean, get_last_processed_id, update_last_processed_id


# Этап 0. Инициализация
init_logging()
supabase = init_supabase()

PIPELINE_NAME = "crashes"

logging.info("update_crashes_clean: запуск")


try:
    batch_size = 50
    insert_batch_size = src.config.CHUNK_SIZE
    all_rows_processed = 0

    # Этап 1. Определение позиции последней обработанной записи
    last_id = get_last_processed_id(supabase, PIPELINE_NAME)
    if last_id is None:
        last_id = 0

    while True:
        # Этап 2. Загрузка очередного батча из buffer
        response = (
            supabase.table("crashes_buffer")
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
        buffer_df = buffer_df[buffer_df['is_error'] == False]

        if buffer_df.empty:
            last_id = rows[-1]['id']
            update_last_processed_id(supabase, PIPELINE_NAME, last_id)
            continue

        # Этап 3. Преобразование raw_json в clean-структуру
        all_cards = []

        for _, row in buffer_df.iterrows():
            raw = row['raw_json']
            if not isinstance(raw, dict):
                continue

            cards = raw.get('cards', [])
            if not cards:
                continue

            for c in cards:
                clean_card = {
                    'kart_id': c.get('KartId'),
                    'accident_date': c.get('date'),
                    'accident_time': c.get('Time'),
                    'accident_type': c.get('DTP_V'),
                    'fatalities': c.get('POG'),
                    'injured': c.get('RAN'),
                    'vehicles_count': c.get('K_TS'),
                    'participants': c.get('K_UCH'),
                    'city': row['city'],
                    'region': row['region']
                }
                all_cards.append(clean_card)

        if all_cards:
            clean_df = pd.DataFrame(all_cards)

            # Этап 4. Приведение типов и очистка данных
            clean_df["kart_id"] = pd.to_numeric(clean_df["kart_id"], errors="coerce")

            clean_df['accident_date'] = pd.to_datetime(
                clean_df['accident_date'],
                format='%d.%m.%Y',
                errors='coerce'
            ).dt.strftime('%Y-%m-%d')

            clean_df['accident_time'] = pd.to_datetime(
                clean_df['accident_time'],
                format='%H:%M',
                errors='coerce'
            ).dt.round('h').dt.strftime('%H:%M:%S')

            clean_df = clean_df.dropna(
                subset=["kart_id", "accident_date", "city", "region"]
            )

            clean_df = clean_df.drop_duplicates(
                subset=["kart_id"],
                keep="last"
            )

            if clean_df.empty:
                last_id = rows[-1]['id']
                update_last_processed_id(supabase, PIPELINE_NAME, last_id)
                continue

            # Этап 5. Upsert в clean-таблицу
            upsert_clean(
                clean_df,
                supabase,
                table_name="crashes",
                on_conflict_columns=["kart_id"],
                chunk_size=insert_batch_size
            )

            all_rows_processed += len(clean_df)
            logging.info(f"update_crashes_clean: добавлено строк: {len(clean_df)}")

        # Этап 6. Обновление прогресса обработки
        last_id = rows[-1]['id']
        update_last_processed_id(supabase, PIPELINE_NAME, last_id)

    logging.info(
        f"update_crashes_clean: завершено успешно, всего обработано: {all_rows_processed}"
    )

except Exception:
    logging.exception("update_crashes_clean: завершено с ошибкой")
    raise
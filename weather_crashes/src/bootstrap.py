import logging
from supabase import create_client
import src.config


def init_logging():
    """
    Инициализация конфигурации логирования для ETL-проекта.
    """
    logging.basicConfig(
        filename="etl.log",
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def init_supabase():
    """
    Создание клиента Supabase на основе параметров конфигурации.
    """
    return create_client(
        src.config.SUPABASE_URL,
        src.config.SUPABASE_KEY
    )
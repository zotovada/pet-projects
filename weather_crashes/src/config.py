# ======================
# Supabase настройки
# ======================

SUPABASE_URL = "https://mwytwbqaafxoylxcetfu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im13eXR3YnFhYWZ4b3lseGNldGZ1Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njk0MzcxMjUsImV4cCI6MjA4NTAxMzEyNX0.E6uhrUodqRrJUs6G6nbD23hj1YhNs6q_-7k0ZQo6Fxs"

# ======================
# Города для анализа
# ======================

TARGET_CITIES = [
    "Волгоград",
    "Тюмень",
]

# ======================
# Период анализа
# ======================

START_DATE = "2015-01-01"

# ======================
# ETL параметры
# ======================

CHUNK_SIZE = 500

# ======================
# Пути к файлам
# ======================

CITIES_CACHE_FILE = "data/cities_with_coords.pkl"
REGIONS_JSON_FILE = "data/regions_all.json"

# ======================
# Погодные переменные для API
# ======================

WEATHER_VARIABLES = [
    "temperature_2m",
    "weather_code",
    "rain",
    "snowfall",
    "wind_speed_10m",
    "is_day",
    "precipitation",
    "dew_point_2m"
]
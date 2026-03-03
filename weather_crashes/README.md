# Weather & Crashes ETL Project

Проект представляет собой набор ETL-скриптов для загрузки, обработки и хранения данных о городах России, погоде и ДТП.  

Собранные скриптами данные сохраняются в буферные и clean-таблицы Supabase и могут быть использованы в дальнейшем для:

- анализа влияния погодных условий на ДТП,
- построения аналитических панелей в BI-системах или средствами Python.

## Структура проекта

```
weather_crashes/
│
├─ etl.log # Логи выполнения
│
├─ data/ # Локальные файлы данных
│ ├─ cities_with_coords.pkl
│ ├─ regions_all.json
│ └─ wmo_weather_codes.csv
│
├─ src/ # Исходный код
│ ├─ init.py
│ ├─ bootstrap.py # Инициализация логов и Supabase
│ ├─ load.py # Универсальные функции загрузки и вставки
│ ├─ cities.py # Функции работы с городами и кодами ГИБДД
│ ├─ config.py # Конфигурация проекта
│
│ ├─ buffer/ # Буферные скрипты
│ │ ├─ cities_buffer.py
│ │ ├─ weather_buffer.py
│ │ └─ crashes_buffer.py
│ │
│ └─ clean/ # Скрипты для clean-таблиц
│ ├─ cities_clean.py
│ ├─ weather_clean.py
│ └─ crashes_clean.py
```

## Установка

1. Клонируем репозиторий:

```bash
git clone <repo_url>
cd <repo_folder>
```

2. Создаём виртуальное окружение:

```
python -m venv venv
source venv/bin/activate      # Linux/Mac
venv\Scripts\activate         # Windows
```

3. Устанавливаем зависимости:

```
pip install -r requirements.txt
```

## Конфигурация

- Все ключи и параметры хранятся в `src/config.py`
- Буферные и clean-скрипты используют параметры из этого файла.
- Локальные файлы данных находятся в папке `data/`.

## Запуск ETL

Буферные скрипты загружают исходные данные в Supabase:

```
python -m src.buffer.cities_buffer
python -m src.buffer.weather_buffer
python -m src.buffer.crashes_buffer
```

Чистые скрипты обрабатывают буферные данные и вставляют в финальные таблицы:

```
python -m src.clean.cities_clean
python -m src.clean.weather_clean
python -m src.clean.crashes_clean
```

## Логи

Все события записываются в `etl.log`.

Для очистки файла используйте:

```
> etl.log   # Windows
: > etl.log # Linux/Mac
```
# Boston Crimes Data Mart

Сборка витрины данных по криминогенной обстановке в районах Бостона на Apache Spark (PySpark).

## Описание задачи

Программа читает два исходных датасета, очищает их, соединяет справочник кодов преступлений с основным датасетом инцидентов и собирает агрегированную витрину по районам (поле `district`).

## Датасет

Источник: [Crimes in Boston (Kaggle)](https://www.kaggle.com/datasets/AnalyzeBoston/crimes-in-boston)

Используются два файла:
- `crime.csv` — основной датасет инцидентов (~320 тыс. строк);
- `offense_codes.csv` — справочник кодов преступлений.

Поместите оба файла в папку `data/` в корне проекта.

## Метрики витрины

| Колонка | Тип | Описание |
|---|---|---|
| `district` | string | Идентификатор района Бостона (A1, A7, A15, B2, B3, C6, C11, D4, D14, E5, E13, E18) |
| `crimes_total` | bigint | Общее количество преступлений в районе |
| `crimes_monthly` | bigint | Медиана числа преступлений в месяц (по всем годам и месяцам) |
| `frequent_crime_types` | string | Топ-3 самых частых типа преступлений через `", "` в порядке убывания частоты |
| `lat` | double | Средняя широта всех инцидентов района |
| `lng` | double | Средняя долгота всех инцидентов района |

## Логика обработки

1. **Чтение** обоих CSV с автоматическим определением схемы (`inferSchema=True`).
2. **Очистка справочника `offense_codes`**:
   - удаление дубликатов по `CODE` (`dropDuplicates`);
   - извлечение `crime_type` — первой части поля `NAME` до разделителя `-`;
   - удаление пробелов по краям через `trim`.
3. **Очистка `crime.csv`**:
   - удаление полных дубликатов строк;
   - удаление строк с `DISTRICT = null` (без района витрина не строится).
4. **Join** основного датасета со справочником по `OFFENSE_CODE` (left join).
5. **Обработка координат**: значения `-1` (заглушка «неизвестное местоположение») заменяются на `null`, чтобы не искажать среднее.
6. **Расчёт метрик**:
   - `crimes_total` — `count(*)` по районам;
   - `crimes_monthly` — `percentile_approx(count, 0.5)` по группам (район, год, месяц);
   - `frequent_crime_types` — оконная функция `row_number` с `partitionBy("DISTRICT")` + `collect_list` + `sort_array` + `concat_ws`;
   - `lat`, `lng` — `avg("Lat")`, `avg("Long")` по районам.
7. **Сборка** всех метрик через `join` по `DISTRICT`.
8. **Сохранение** результата в один Parquet-файл через `coalesce(1).write.parquet(...)`.

## Запуск

Через `spark-submit`:

```bash
spark-submit boston_crimes_etl.py data/crime.csv data/offense_codes.csv output/boston_crimes
```

Через Python из виртуального окружения:

```bash
python boston_crimes_etl.py data/crime.csv data/offense_codes.csv output/boston_crimes
```

Аргументы:
1. Путь к `crime.csv`.
2. Путь к `offense_codes.csv`.
3. Путь к папке, куда сохранится витрина (в формате Parquet).



## Результат

На выходе — папка `output/boston_crimes/` с одним Parquet-файлом:

```
output/boston_crimes/
├── _SUCCESS
└── part-00000-....snappy.parquet
```

Витрина содержит 12 строк — по одной на каждый район Бостона.

## Пример результата

| district | crimes_total | crimes_monthly | frequent_crime_types | lat | lng |
|---|---|---|---|---|---|
| A1 | 35710 | 904 | PROPERTY, ASSAULT SIMPLE, DRUGS | 42.3567 | -71.0611 |
| B2 | 49940 | 1298 | M/V, M/V ACCIDENT, VERBAL DISPUTE | 42.3216 | -71.0848 |
| C11 | 42528 | 1115 | M/V, SICK/INJURED/MEDICAL, INVESTIGATE PERSON | 42.3000 | -71.0633 |
| D4 | 41914 | 1084 | LARCENY SHOPLIFTING, PROPERTY, LARCENY THEFT FROM BUILDING | 42.3435 | -71.0809 |

## Структура проекта

```
boston_crimes_project/
├── boston_crimes_etl.py    # основной ETL-скрипт
├── boston_crimes.ipynb     # ноутбук для разработки и отладки
├── README.md
├── data/
│   ├── crime.csv
│   └── offense_codes.csv
└── output/
    └── boston_crimes/
        └── part-00000-....parquet
```

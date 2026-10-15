# Boston Crimes Data Mart

Сборка витрины данных по криминогенной обстановке в районах Бостона на Spark.

## Запуск

```bash
spark-submit boston_crimes_etl.py <crimes_csv_path> <offense_codes_csv_path> <output_parquet_path>
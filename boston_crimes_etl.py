import sys
import os

# Указываем Spark использовать тот же Python, что и запускает скрипт
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    collect_list,
    sort_array,
    struct,
    transform,
    concat_ws,
)


def build_mart(spark, crimes_path, codes_path):
    """
    Читает данные о преступлениях и справочник кодов,
    собирает витрину по районам Бостона.
    """
    # ---------- 1. Чтение ----------
    crimes_df = spark.read.csv(crimes_path, header=True, inferSchema=True)
    offense_codes_df = spark.read.csv(codes_path, header=True, inferSchema=True)

    # ---------- 2. Очистка справочника кодов ----------
    # Убираем дубликаты по CODE, извлекаем crime_type (первая часть NAME до "-")
    unique_codes = (
        offense_codes_df
        .dropDuplicates(["CODE"])
        .withColumn("crime_type", f.trim(f.split(f.col("NAME"), "-").getItem(0)))
        .select("CODE", "crime_type")
        .withColumnRenamed("CODE", "OFFENSE_CODE")
    )

    # ---------- 3. Очистка основного датасета ----------
    crimes_clean = (
        crimes_df
        .dropDuplicates()                              # полные дубликаты строк
        .filter(f.col("DISTRICT").isNotNull())         # без района витрина не строится
    )

    # ---------- 4. JOIN с справочником ----------
    enriched = (
        crimes_clean
        .join(unique_codes, on="OFFENSE_CODE", how="left")
        .filter(f.col("crime_type").isNotNull())       # если кода нет в справочнике — отбросим
    )

    # ---------- 5. Обработка координат ----------
    # В датасете Boston Crimes "-1" — заглушка "неизвестное местоположение"
    enriched = (
        enriched
        .withColumn("Lat", f.when(f.col("Lat") == -1, None).otherwise(f.col("Lat")))
        .withColumn("Long", f.when(f.col("Long") == -1, None).otherwise(f.col("Long")))
    )

    # ---------- 6. Метрика crimes_total ----------
    crimes_total = enriched.groupBy("DISTRICT").agg(
        f.count("*").alias("crimes_total")
    )

    # ---------- 7. Метрика crimes_monthly (медиана) ----------
    # Считаем число преступлений по (район, год, месяц), затем медиану
    monthly_counts = enriched.groupBy("DISTRICT", "YEAR", "MONTH").count()
    median_monthly = monthly_counts.groupBy("DISTRICT").agg(
        f.expr("percentile_approx(count, 0.5)").alias("crimes_monthly")
    )

    # ---------- 8. Метрика frequent_crime_types (топ-3) ----------
    type_counts = enriched.groupBy("DISTRICT", "crime_type").count()

    window_spec = Window.partitionBy("DISTRICT").orderBy(f.col("count").desc())
    top3 = (
        type_counts
        .withColumn("rank", f.row_number().over(window_spec))
        .filter(f.col("rank") <= 3)
    )

    # Сортируем топ-3 по rank и собираем в строку
    top3_with_struct = top3.select(
        "DISTRICT",
        struct("rank", "crime_type").alias("type_struct"),
    )
    frequent_types = (
        top3_with_struct
        .groupBy("DISTRICT")
        .agg(sort_array(collect_list("type_struct")).alias("sorted_types"))
        .withColumn(
            "frequent_crime_types",
            concat_ws(", ", transform("sorted_types", lambda x: x.crime_type)),
        )
        .select("DISTRICT", "frequent_crime_types")
    )

    # ---------- 9. Метрики lat и lng ----------
    coords = enriched.groupBy("DISTRICT").agg(
        f.avg("Lat").alias("lat"),
        f.avg("Long").alias("lng"),
    )

    # ---------- 10. Сборка финальной витрины ----------
    final_df = (
        crimes_total
        .join(median_monthly, on="DISTRICT", how="inner")
        .join(frequent_types, on="DISTRICT", how="inner")
        .join(coords, on="DISTRICT", how="inner")
        .withColumnRenamed("DISTRICT", "district")
    )

    return final_df


def main(crimes_path, codes_path, output_path):
    spark = (
        SparkSession.builder
        .appName("BostonCrimesMart")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    final_df = build_mart(spark, crimes_path, codes_path)

    # Сохраняем в один parquet-файл
    final_df.coalesce(1).write.mode("overwrite").parquet(output_path)

    # Проверка результата
    print(f"Витрина сохранена в: {output_path}")
    result = spark.read.parquet(output_path)
    print(f"Количество районов: {result.count()}")
    result.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(
            "Использование: spark-submit boston_crimes_etl.py "
            "<путь_к_crime.csv> <путь_к_offense_codes.csv> <путь_к_папке_вывода>"
        )
        sys.exit(1)

    crimes_path_arg = sys.argv[1]
    codes_path_arg = sys.argv[2]
    output_path_arg = sys.argv[3]

    main(crimes_path_arg, codes_path_arg, output_path_arg)
from pyspark.sql import SparkSession

# 1. Создаём сессию Spark (работает внутри контейнера Airflow)
spark = SparkSession.builder \
    .appName("SparkETL") \
    .config("spark.jars", "/opt/airflow/postgresql-42.6.0.jar") \
    .config("spark.driver.extraClassPath", "/opt/airflow/postgresql-42.6.0.jar") \
    .master("local[*]") \
    .getOrCreate()

# 2. Читаем данные из test_table (используем имя сервиса "postgres")
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/airflow") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "test_table") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .load()

print("Исходные данные:")
df.show()

# 3. Трансформация: увеличиваем rows_count на 10%
df_transformed = df.withColumn("rows_count_transformed", df["rows_count"] * 1.1)

print("Данные после трансформации:")
df_transformed.select("id", "rows_count", "rows_count_transformed").show()

# 4. Запись в новую таблицу (перезаписываем при каждом запуске)
df_transformed.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/airflow") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "test_table_spark") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .mode("overwrite") \
    .save()

print("Данные успешно записаны в test_table_spark!")

# 5. Проверяем, что записалось
df_check = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/airflow") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "test_table_spark") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .load()

print("Проверка: данные в test_table_spark:")
df_check.show()

# 6. Останавливаем Spark
spark.stop()
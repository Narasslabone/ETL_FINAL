 	from pyspark.sql import SparkSession
 	from pyspark.sql.functions import col, to_date
 	from pyspark.sql.types import IntegerType, StringType, BooleanType
 	from pyspark.sql.utils import AnalysisException
 	
 	
 	# === Spark session ===
 	spark = SparkSession.builder.appName("Parquet ETL with Logging to S3").getOrCreate()
 	
 	
 	# === Пути ===
 	source_path = "s3a://databacket/weather_data.csv"
 	target_path = "s3a://databacket/weather_data.parquet"
 	
 	try:
 	    print(f"Чтение данных из: {source_path}")
 	    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
 	
 	    print("Схема исходных данных:")
 	    df.printSchema()
 	
 	    # Приведение типов + формат даты YYYYMMDD
 	    df = df.withColumn("location", col("location").cast(StringType())) \
 	           .withColumn("date_time", to_date(col("date_time").cast("string"), "yyyyMMdd")) \
 	           .withColumn("temperature_c", col("temperature_c").DoubleType())) \
 	           .withColumn("humidity_pct", col("humidity_pct").DoubleType())) \
 	           .withColumn("precipitation_mm", col("precipitation_mm").DoubleType())) \
 	           .withColumn("wind_speed_kmh", col("wind_speed_kmh").DoubleType())
 	
 	    print("Схема преобразованных данных:")
 	    df.printSchema()
 	
 	    # Удаление строк с пропущенными значениями
 	    df = df.na.drop()
 	
 	    print("Пример данных после преобразования:")
 	    df.show(5)
 	
 	    print(f"Запись в Parquet: {target_path}")
 	    df.write.mode("overwrite").parquet(target_path)
 	
 	    print("✅ Данные успешно сохранены в Parquet.")

 	except AnalysisException as ae:
 	    print("❌ Ошибка анализа:", ae)
 	except Exception as e:
 	    print("❌ Общая ошибка:", e)
 
 	spark.stop()
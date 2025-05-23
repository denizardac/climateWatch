from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, lower, lit
import os

# SparkSession başlat
spark = SparkSession.builder \
    .appName("ClimateWatch ETL") \
    .getOrCreate()

# Veri yolları
GDELT_PATH = "data_storage/gdelt/*.csv"
CLIMATE_PATH = "data_storage/climate/*.csv"
DISASTER_PATH = "data_storage/disasters/*.csv"
POLICY_PATH = "data_storage/policies/*.csv"
SUMMIT_PATH = "data_storage/summits/*.csv"
OUTPUT_PATH = "data_storage/processed/combined_data.parquet"

# 1. Veri Okuma
if os.path.exists("data_storage/gdelt"):
    gdelt_df = spark.read.option("header", True).csv(GDELT_PATH)
else:
    gdelt_df = None
if os.path.exists("data_storage/climate"):
    climate_df = spark.read.option("header", True).csv(CLIMATE_PATH)
else:
    climate_df = None
if os.path.exists("data_storage/disasters"):
    disaster_df = spark.read.option("header", True).csv(DISASTER_PATH)
else:
    disaster_df = None
if os.path.exists("data_storage/policies"):
    policy_df = spark.read.option("header", True).csv(POLICY_PATH)
else:
    policy_df = None
if os.path.exists("data_storage/summits"):
    summit_df = spark.read.option("header", True).csv(SUMMIT_PATH)
else:
    summit_df = None

# 2. Dönüşüm ve Temizlik (örnek: GDELT)
if gdelt_df is not None:
    gdelt_df = gdelt_df \
        .withColumnRenamed("date", "date") \
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
        .withColumn("source_type", lit("news")) \
        .withColumn("title", lower(col("title"))) \
        .withColumn("text", lower(col("text")))

# Climate verisi için örnek dönüşüm
def safe_col(df, old, new):
    if df is not None and old in df.columns:
        return df.withColumnRenamed(old, new)
    return df
if climate_df is not None:
    climate_df = safe_col(climate_df, "Year", "year")
    climate_df = safe_col(climate_df, "Mean", "temperature_anomaly")
    climate_df = climate_df.withColumn("source_type", lit("climate"))

# Diğer veri kaynakları için de benzer dönüşümler eklenebilir

# 3. Birleştirme (örnek: yıl bazında)
if gdelt_df is not None and climate_df is not None:
    climate_gdelt_joined = gdelt_df.join(
        climate_df,
        gdelt_df.date.substr(1, 4) == climate_df.year.cast("string"),
        how="left"
    )
else:
    climate_gdelt_joined = None

# 4. Sonuçları Kaydet
if climate_gdelt_joined is not None:
    climate_gdelt_joined.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"Birleşik veri {OUTPUT_PATH} olarak kaydedildi.")
else:
    print("Birleştirilecek yeterli veri bulunamadı.")

spark.stop() 
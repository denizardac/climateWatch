from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year
import os

def main():
    # 1. SparkSession başlat
    spark = (
        SparkSession.builder
        .appName("ClimateNewsPipeline")
        .getOrCreate()
    )

    # 2. GDELT verisini oku
    gdelt_path = os.path.join("gdelt_new", "gdelt_climate_news_20150101_to_20201231.csv")
    gdelt = (
        spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(gdelt_path)
    )
    # 3. 'DATE' kolonunu timestamp ve 'year' olarak dönüştür
    gdelt = (
        gdelt
          .withColumn("date", to_timestamp("DATE", "yyyyMMddHHmmss"))
          .withColumn("year", year("date"))
    )
    news_count = gdelt.groupBy("year").count().withColumnRenamed("count", "news_count")

    # 4. Çevresel veriyi oku (Enviro data)
    env_path = os.path.join("Enviro_data", "merged_climate_data_1970_2020.csv")
    env_df = (
        spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(env_path)
    )

    # 5. Ortak 'year' üzerinde birleştir
    final = news_count.join(env_df, on="year", how="inner").orderBy("year")

    # 6. Sonucu ekrana bas
    final.show(50, truncate=False)

    # 7. İstersen Parquet olarak kaydet
    # final.write.mode("overwrite").parquet("output/climate_news_env.parquet")

    spark.stop()

if __name__ == "__main__":
    main()

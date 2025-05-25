from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, lower, lit, date_format, from_json, schema_of_json, concat
import os
import json
import time

def ensure_directories():
    """Gerekli dizinlerin varlığını kontrol et ve yoksa oluştur"""
    directories = [
        "data_storage/gdelt",
        "data_storage/climate",
        "data_storage/kaggle",
        "data_storage/policies",
        "data_storage/open_data",
        "data_storage/summits",
        "data_storage/processed"
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            print(f"{directory} dizini oluşturuluyor...")
            os.makedirs(directory, exist_ok=True)

def process_data():
    # Dizinleri kontrol et ve oluştur
    ensure_directories()

    # MongoDB bağlantısını kontrol et
    try:
        from pymongo import MongoClient
        client = MongoClient('mongodb://mongodb:27017/', serverSelectionTimeoutMS=5000)
        # Bağlantıyı test et
        client.server_info()
        print("MongoDB bağlantısı başarılı")
    except Exception as e:
        print(f"MongoDB bağlantı hatası: {str(e)}")
        print("Lütfen MongoDB servisinin çalıştığından emin olun")
        return

    # SparkSession başlat
    try:
        spark = SparkSession.builder \
            .appName("ClimateWatch ETL") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "2g") \
            .getOrCreate()
        print("Spark bağlantısı başarılı")
    except Exception as e:
        print(f"Spark bağlantı hatası: {str(e)}")
        print("Lütfen Spark servisinin çalıştığından emin olun")
        return

    # Veri yolları
    GDELT_PATH = "data_storage/gdelt/*.csv"
    CLIMATE_PATH = "data_storage/climate/*.csv"
    KAGGLE_PATH = "data_storage/kaggle/*.csv"
    POLICY_PATH = "data_storage/policies/*.csv"
    OPEN_DATA_PATH = "data_storage/open_data/*.csv"
    SUMMIT_PATH = "data_storage/summits/*.csv"
    OUTPUT_PATH = "data_storage/processed/combined_data.parquet"

    # 1. GDELT Verilerini Oku
    print("GDELT verilerini okuyorum...")
    gdelt_df = spark.read.option("header", False).csv(GDELT_PATH)
    print(f"GDELT veri sayısı: {gdelt_df.count()}")

    # 2. İklim Verilerini Oku
    print("\nİklim verilerini okuyorum...")
    climate_df = spark.read.option("header", True).csv(CLIMATE_PATH)
    print(f"İklim veri sayısı: {climate_df.count()}")

    # 3. Kaggle Verilerini Oku
    print("\nKaggle verilerini okuyorum...")
    kaggle_files = [f for f in os.listdir("data_storage/kaggle") if f.endswith('.csv')]
    kaggle_dfs = []
    
    for file in kaggle_files:
        try:
            df = spark.read.option("header", True).csv(f"data_storage/kaggle/{file}")
            print(f"{file} veri sayısı: {df.count()}")
            kaggle_dfs.append(df)
        except Exception as e:
            print(f"{file} okunamadı: {e}")

    # 4. Politika Verilerini Oku
    print("\nPolitika verilerini okuyorum...")
    policy_files = [f for f in os.listdir("data_storage/policies") if f.endswith('.csv')]
    policy_dfs = []
    
    for file in policy_files:
        try:
            df = spark.read.option("header", True).csv(f"data_storage/policies/{file}")
            print(f"{file} veri sayısı: {df.count()}")
            policy_dfs.append(df)
        except Exception as e:
            print(f"{file} okunamadı: {e}")

    # 5. Açık Veri Setlerini Oku
    print("\nAçık veri setlerini okuyorum...")
    open_data_files = [f for f in os.listdir("data_storage/open_data") if f.endswith('.csv')]
    open_data_dfs = []
    
    for file in open_data_files:
        try:
            df = spark.read.option("header", True).csv(f"data_storage/open_data/{file}")
            print(f"{file} veri sayısı: {df.count()}")
            open_data_dfs.append(df)
        except Exception as e:
            print(f"{file} okunamadı: {e}")

    # 6. COP Zirveleri Verilerini Oku
    print("\nCOP zirveleri verilerini okuyorum...")
    summit_files = [f for f in os.listdir("data_storage/summits") if f.endswith('.csv')]
    summit_dfs = []
    
    for file in summit_files:
        try:
            df = spark.read.option("header", True).csv(f"data_storage/summits/{file}")
            print(f"{file} veri sayısı: {df.count()}")
            summit_dfs.append(df)
        except Exception as e:
            print(f"{file} okunamadı: {e}")

    # 7. Veri Dönüşümleri
    print("\nVerileri dönüştürüyorum...")
    
    # GDELT dönüşümü
    gdelt_df = gdelt_df \
        .withColumn("date", to_date(col("_c1"), "yyyyMMdd")) \
        .withColumn("gdelt_date_str", date_format(col("date"), "yyyy-MM-dd")) \
        .withColumn("title", col("_c27")) \
        .withColumn("text", col("_c28")) \
        .withColumn("news_source_type", lit("news")) \
        .select("gdelt_date_str", "title", "text", "news_source_type")

    # İklim verisi dönüşümü
    climate_df = climate_df \
        .withColumnRenamed("Year", "climate_year") \
        .withColumnRenamed("Mean", "temperature_anomaly") \
        .withColumn("climate_source_type", lit("climate"))

    # Politika verisi dönüşümü
    if policy_dfs:
        policy_df = policy_dfs[0]  # İlk politika veri setini al
        policy_df = policy_df \
            .withColumn("policy_source_type", lit("policy")) \
            .withColumn("policy_date_str", date_format(col("date"), "yyyy-MM-dd"))

    # Açık veri seti dönüşümü
    if open_data_dfs:
        open_data_df = open_data_dfs[0]  # İlk açık veri setini al
        open_data_df = open_data_df \
            .withColumn("open_data_source_type", lit("open_data")) \
            .withColumn("open_data_date_str", date_format(col("date"), "yyyy-MM-dd"))

    # COP zirveleri verisi dönüşümü
    if summit_dfs:
        summit_df = summit_dfs[0]  # İlk zirve veri setini al
        summit_df = summit_df \
            .withColumn("summit_source_type", lit("summit")) \
            .withColumn("summit_date_str", concat(col("year"), lit("-01-01")))  # Yılın ilk gününü kullan

    # 8. Verileri Birleştir
    print("\nVerileri birleştiriyorum...")
    
    # GDELT ve iklim verilerini birleştir
    climate_gdelt_joined = gdelt_df.join(
        climate_df,
        gdelt_df.gdelt_date_str.substr(1, 4) == climate_df.climate_year.cast("string"),
        how="left"
    )

    # Politika verilerini birleştir
    if policy_dfs:
        policy_df = policy_dfs[0]
        policy_df = policy_df \
            .withColumn("policy_source_type", lit("policy")) \
            .withColumn("policy_date_str", date_format(col("date"), "yyyy-MM-dd"))
        
        climate_gdelt_joined = climate_gdelt_joined.join(
            policy_df,
            climate_gdelt_joined.gdelt_date_str == policy_df.policy_date_str,
            how="left"
        )

    # Açık veri setlerini birleştir
    if open_data_dfs:
        open_data_df = open_data_dfs[0]
        open_data_df = open_data_df \
            .withColumn("open_data_source_type", lit("open_data")) \
            .withColumn("open_data_date_str", date_format(col("date"), "yyyy-MM-dd"))
        
        climate_gdelt_joined = climate_gdelt_joined.join(
            open_data_df,
            climate_gdelt_joined.gdelt_date_str == open_data_df.open_data_date_str,
            how="left"
        )

    # COP zirveleri verilerini birleştir
    if summit_dfs:
        summit_df = summit_dfs[0]
        summit_df = summit_df \
            .withColumn("summit_source_type", lit("summit")) \
            .withColumn("summit_date_str", concat(col("year"), lit("-01-01")))
        
        climate_gdelt_joined = climate_gdelt_joined.join(
            summit_df,
            climate_gdelt_joined.gdelt_date_str.substr(1, 4) == summit_df.year.cast("string"),
            how="left"
        )

    # Birleştirilmiş veriyi önbelleğe al
    climate_gdelt_joined = climate_gdelt_joined.cache()

    # 9. Sonuçları Kaydet
    print("\nSonuçları kaydediyorum...")
    climate_gdelt_joined.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"Birleşik veri {OUTPUT_PATH} olarak kaydedildi.")

    # Önbelleği temizle
    climate_gdelt_joined.unpersist()

    # 10. MongoDB'ye Kaydet
    print("\nMongoDB'ye kaydediyorum...")
    db = client['climatewatch']

    # MongoDB'ye kaydetme işlemlerini try-except bloğuna al
    try:
        # GDELT verilerini MongoDB'ye kaydet
        gdelt_collection = db['gdelt_events']
        gdelt_collection.delete_many({})
        gdelt_pandas = gdelt_df.toPandas()
        gdelt_records = gdelt_pandas.to_dict('records')
        gdelt_collection.insert_many(gdelt_records)
        print(f"GDELT verileri MongoDB'ye kaydedildi: {len(gdelt_records)} kayıt")

        # İklim verilerini MongoDB'ye kaydet
        climate_collection = db['climate_data']
        climate_collection.delete_many({})
        climate_pandas = climate_df.toPandas()
        climate_records = climate_pandas.to_dict('records')
        climate_collection.insert_many(climate_records)
        print(f"İklim verileri MongoDB'ye kaydedildi: {len(climate_records)} kayıt")

        # Kaggle verilerini MongoDB'ye kaydet
        kaggle_collection = db['kaggle_data']
        kaggle_collection.delete_many({})
        
        for i, df in enumerate(kaggle_dfs):
            try:
                # GlobalLandTemperaturesByCity.csv dosyasını atla
                if kaggle_files[i] == 'GlobalLandTemperaturesByCity.csv':
                    print(f"{kaggle_files[i]} dosyası MongoDB'ye aktarılmayacak (atlandı).")
                    continue
                rdd = df.rdd.map(lambda row: row.asDict())
                for partition in rdd.glom().collect():
                    if partition:
                        from pymongo import MongoClient
                        client = MongoClient('mongodb://mongodb:27017/', serverSelectionTimeoutMS=5000)
                        kaggle_collection = client['climatewatch']['kaggle_data']
                        for rec in partition:
                            rec['source_dataset'] = kaggle_files[i]
                        kaggle_collection.insert_many(partition)
                        client.close()
                        print(f"{kaggle_files[i]} - {len(partition)} kayıt kaydedildi")
                        time.sleep(0.1)
                print(f"{kaggle_files[i]} MongoDB'ye kaydedildi: {df.count()} kayıt")
            except Exception as e:
                print(f"{kaggle_files[i]} MongoDB'ye kaydedilemedi: {e}")
                print(f"Hata detayı: {str(e)}")

        # Politika verilerini MongoDB'ye kaydet
        if policy_dfs:
            policy_collection = db['policy_data']
            policy_collection.delete_many({})
            
            for i, df in enumerate(policy_dfs):
                try:
                    policy_pandas = df.toPandas()
                    policy_records = policy_pandas.to_dict('records')
                    for rec in policy_records:
                        rec['source_file'] = policy_files[i]
                    policy_collection.insert_many(policy_records)
                    print(f"{policy_files[i]} MongoDB'ye kaydedildi: {len(policy_records)} kayıt")
                except Exception as e:
                    print(f"{policy_files[i]} MongoDB'ye kaydedilemedi: {e}")

        # Açık veri setlerini MongoDB'ye kaydet
        if open_data_dfs:
            open_data_collection = db['open_data']
            open_data_collection.delete_many({})
            
            for i, df in enumerate(open_data_dfs):
                try:
                    open_data_pandas = df.toPandas()
                    open_data_records = open_data_pandas.to_dict('records')
                    for rec in open_data_records:
                        rec['source_file'] = open_data_files[i]
                    open_data_collection.insert_many(open_data_records)
                    print(f"{open_data_files[i]} MongoDB'ye kaydedildi: {len(open_data_records)} kayıt")
                except Exception as e:
                    print(f"{open_data_files[i]} MongoDB'ye kaydedilemedi: {e}")

        # COP zirveleri verilerini MongoDB'ye kaydet
        if summit_dfs:
            summit_collection = db['summit_data']
            summit_collection.delete_many({})
            
            for i, df in enumerate(summit_dfs):
                try:
                    summit_pandas = df.toPandas()
                    summit_records = summit_pandas.to_dict('records')
                    for rec in summit_records:
                        rec['source_file'] = summit_files[i]
                    summit_collection.insert_many(summit_records)
                    print(f"{summit_files[i]} MongoDB'ye kaydedildi: {len(summit_records)} kayıt")
                except Exception as e:
                    print(f"{summit_files[i]} MongoDB'ye kaydedilemedi: {e}")

    except Exception as e:
        print(f"MongoDB işlem hatası: {str(e)}")
        print("MongoDB işlemleri atlanıyor...")
    
    finally:
        # Spark'ı düzgün şekilde kapat
        try:
            spark.stop()
            print("Spark oturumu kapatıldı")
        except:
            pass

if __name__ == "__main__":
    process_data() 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from datetime import datetime, timedelta
import json
import os

def create_spark_session():
    """Spark oturumu oluştur"""
    return SparkSession.builder \
        .appName("ClimateWatch") \
        .master("local[*]") \
        .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/climatewatch") \
        .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/climatewatch") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .config("spark.driver.extraClassPath", "/usr/local/spark/jars/*") \
        .getOrCreate()

def load_data_from_mongodb(spark):
    """MongoDB'den verileri Spark DataFrame'e yükle"""
    # GDELT verilerini yükle
    gdelt_df = spark.read.format("mongo") \
        .option("database", "climatewatch") \
        .option("collection", "gdelt_events") \
        .load()
    
    # İklim verilerini yükle
    climate_df = spark.read.format("mongo") \
        .option("database", "climatewatch") \
        .option("collection", "climate_data") \
        .load()
    
    return gdelt_df, climate_df

def analyze_news_trends(gdelt_df):
    """Haber trendlerini analiz et"""
    # Yıllık haber sayıları
    yearly_trend = gdelt_df.groupBy("year") \
        .agg(count("*").alias("event_count")) \
        .orderBy("year")
    
    # Aylık haber sayıları
    monthly_trend = gdelt_df.groupBy("year", "month") \
        .agg(count("*").alias("event_count")) \
        .orderBy("year", "month")
    
    return yearly_trend, monthly_trend

def analyze_sentiment_trends(gdelt_df):
    """Duygu analizi trendlerini analiz et"""
    # Yıllık ortalama duygu skorları
    sentiment_trend = gdelt_df.groupBy("year") \
        .agg(avg("sentiment_score").alias("avg_sentiment")) \
        .orderBy("year")
    
    return sentiment_trend

def analyze_climate_correlation(gdelt_df, climate_df):
    """İklim verileri ile haber verilerinin korelasyonunu analiz et"""
    # Tarih bazlı join
    correlation_df = gdelt_df.join(
        climate_df,
        gdelt_df.year == climate_df.year,
        "inner"
    ).groupBy("year") \
     .agg(
         avg("sentiment_score").alias("avg_sentiment"),
         avg("temperature").alias("avg_temperature"),
         count("*").alias("event_count")
     ) \
     .orderBy("year")
    
    return correlation_df

def save_results_to_mongodb(results_df, collection_name):
    """Sonuçları MongoDB'ye kaydet"""
    results_df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("database", "climatewatch") \
        .option("collection", collection_name) \
        .save()

def main():
    # Spark oturumu başlat
    spark = create_spark_session()
    
    try:
        # Verileri yükle
        gdelt_df, climate_df = load_data_from_mongodb(spark)
        
        # Haber trendlerini analiz et
        yearly_trend, monthly_trend = analyze_news_trends(gdelt_df)
        save_results_to_mongodb(yearly_trend, "yearly_news_trend")
        save_results_to_mongodb(monthly_trend, "monthly_news_trend")
        
        # Duygu analizi trendlerini analiz et
        sentiment_trend = analyze_sentiment_trends(gdelt_df)
        save_results_to_mongodb(sentiment_trend, "sentiment_trend")
        
        # İklim korelasyonunu analiz et
        correlation_df = analyze_climate_correlation(gdelt_df, climate_df)
        save_results_to_mongodb(correlation_df, "climate_correlation")
        
        # Sonuçları göster
        print("Yıllık Haber Trendi:")
        yearly_trend.show()
        
        print("\nDuygu Analizi Trendi:")
        sentiment_trend.show()
        
        print("\nİklim Korelasyonu:")
        correlation_df.show()
        
    finally:
        # Spark oturumunu kapat
        spark.stop()

if __name__ == "__main__":
    main() 
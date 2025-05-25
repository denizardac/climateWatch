import os
from pyspark.sql import SparkSession

# SparkSession oluştur
spark = SparkSession.builder \
    .appName("KafkaStreamToConsole") \
    .getOrCreate()

# Kafka ayarları
topic = os.environ.get("KAFKA_TOPIC", "climate-data")
kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Kafka'dan veri oku
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Sadece value sütununu string olarak al
value_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Konsola yaz
query = value_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

print(f"Streaming started. Reading from topic: {topic} on {kafka_bootstrap_servers}")

query.awaitTermination() 
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, FloatType
import logging
from typing import Optional
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClimateDataStreamProcessor:
    def __init__(self, kafka_bootstrap_servers: str):
        """
        Initialize Spark Streaming processor for climate data
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        
        # Create checkpoint directory if it doesn't exist
        checkpoint_dir = os.path.join(os.getcwd(), "spark_checkpoint")
        os.makedirs(checkpoint_dir, exist_ok=True)
        
        # Initialize Spark Session with Kafka package
        self.spark = (SparkSession.builder
            .appName("ClimateDataProcessor")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
            .getOrCreate())
        
        logger.info("Spark Session initialized for stream processing")
        
        # Define schema for climate data
        self.schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("location", StringType(), True),
            StructField("temperature", FloatType(), True),
            StructField("humidity", FloatType(), True),
            StructField("wind_speed", FloatType(), True),
            StructField("precipitation", FloatType(), True),
            StructField("pressure", FloatType(), True),
            StructField("snow", FloatType(), True)
        ])

    def _get_climate_data_schema(self) -> StructType:
        """Define schema for climate data"""
        return self.schema

    def create_streaming_query(self, topic: str, checkpoint_location: Optional[str] = None):
        """
        Create and start a streaming query from Kafka
        
        Args:
            topic: Kafka topic to consume from
            checkpoint_location: Directory for checkpoint storage
        """
        # Read from Kafka
        stream_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()

        # Parse value from Kafka message
        parsed_df = stream_df.select(
            from_json(
                col("value").cast("string"),
                self._get_climate_data_schema()
            ).alias("data")
        ).select("data.*")

        # Add windowing and aggregations
        windowed_stats = parsed_df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window("timestamp", "5 minutes"),
                "location"
            ) \
            .agg({
                "temperature": "avg",
                "humidity": "avg",
                "precipitation": "sum",
                "wind_speed": "avg",
                "pressure": "avg",
                "snow": "sum"
            }) \
            .selectExpr(
                "window.start as window_start",
                "window.end as window_end",
                "location",
                "round(avg_temperature, 2) as avg_temp_celsius",
                "round(avg_humidity, 2) as avg_humidity_percent",
                "round(sum_precipitation, 2) as total_precipitation_mm",
                "round(avg_wind_speed, 2) as avg_wind_speed_kmh",
                "round(avg_pressure, 2) as avg_pressure_hpa",
                "round(sum_snow, 2) as total_snow_mm"
            )

        # Write stream to console for debugging
        query = windowed_stats.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false")

        if checkpoint_location:
            query = query.option("checkpointLocation", checkpoint_location)

        return query.start()

    def stop(self):
        """Stop the Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark Session stopped") 
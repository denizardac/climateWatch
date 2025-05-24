from kafka_utils import ClimateDataProducer, ClimateDataConsumer
from stream_processor import ClimateDataStreamProcessor
from utils.weather_api import WeatherDataFetcher
import time
from datetime import datetime
import logging
from typing import Dict, Any
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_producer_example():
    """Example of how to run the climate data producer with real weather data"""
    producer = ClimateDataProducer(
        bootstrap_servers=['localhost:9092'],
        topic='climate-data'
    )
    
    # Initialize weather data fetcher with demo key
    weather_fetcher = WeatherDataFetcher()
    locations = ['Istanbul', 'Ankara', 'Izmir', 'Antalya']
    
    try:
        logger.info("Starting weather data producer with MeteoStats API...")
        while True:  # Continuous streaming
            for location in locations:
                try:
                    # Get real weather data
                    data = weather_fetcher.get_weather_data(location)
                    producer.send_climate_data(
                        key=f"{location}-{data['timestamp']}", 
                        data=data
                    )
                    logger.info(f"Sent real weather data for {location}: {data['temperature']}°C, "
                              f"Humidity: {data['humidity']}%, Wind: {data['wind_speed']} km/h")
                except Exception as e:
                    logger.error(f"Error getting weather data for {location}: {str(e)}")
            
            # Wait 10 seconds before next update (MeteoStats data updates hourly)
            logger.info("Waiting 10 seconds before next update...")
            time.sleep(10)
    finally:
        producer.close()

def process_message(key: str, value: Dict[str, Any]):
    """Example message processing function"""
    logger.info(f"Processing message: {key}")
    logger.info(f"Temperature: {value['temperature']}°C, "
               f"Humidity: {value['humidity']}%, "
               f"Wind: {value['wind_speed']} km/h, "
               f"Pressure: {value['pressure']} hPa, "
               f"Location: {value['location']}")

def run_consumer_example():
    """Example of how to run the climate data consumer"""
    consumer = ClimateDataConsumer(
        bootstrap_servers=['localhost:9092'],
        topic='climate-data',
        group_id='climate-processor-group'
    )
    
    try:
        logger.info("Starting weather data consumer...")
        consumer.consume_messages(process_message)
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()

def run_spark_streaming_example():
    """Example of how to run the Spark streaming processor"""
    processor = ClimateDataStreamProcessor(
        kafka_bootstrap_servers='localhost:9092'
    )
    
    try:
        logger.info("Starting Spark Streaming processor...")
        query = processor.create_streaming_query(
            topic='climate-data',
            checkpoint_location='/tmp/checkpoint'
        )
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping processor...")
    finally:
        processor.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python climate_stream_example.py [producer|consumer|processor]")
        sys.exit(1)
    
    mode = sys.argv[1]
    if mode == "producer":
        run_producer_example()
    elif mode == "consumer":
        run_consumer_example()
    elif mode == "processor":
        run_spark_streaming_example()
    else:
        print("Invalid mode. Use 'producer', 'consumer', or 'processor'")
        sys.exit(1) 
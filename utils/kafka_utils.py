from kafka import KafkaProducer, KafkaConsumer
import json
from typing import Dict, Any, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClimateDataProducer:
    def __init__(self, bootstrap_servers: List[str], topic: str):
        """
        Initialize Kafka producer for climate data
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
            topic: Kafka topic to produce messages to
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=str.encode
        )
        logger.info(f"Producer initialized for topic: {topic}")

    def send_climate_data(self, key: str, data: Dict[str, Any]):
        """
        Send climate data to Kafka topic
        
        Args:
            key: Message key
            data: Climate data dictionary to be sent
        """
        try:
            future = self.producer.send(self.topic, key=key, value=data)
            self.producer.flush()
            future.get(timeout=60)
            logger.info(f"Successfully sent data with key: {key}")
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")

    def close(self):
        """Close the producer connection"""
        self.producer.close()

class ClimateDataConsumer:
    def __init__(self, bootstrap_servers: List[str], topic: str, group_id: str):
        """
        Initialize Kafka consumer for climate data
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
            topic: Kafka topic to consume messages from
            group_id: Consumer group ID
        """
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=bytes.decode
        )
        logger.info(f"Consumer initialized for topic: {topic}, group: {group_id}")

    def consume_messages(self, process_message_func=None):
        """
        Consume messages from Kafka topic
        
        Args:
            process_message_func: Optional function to process received messages
        """
        try:
            for message in self.consumer:
                logger.info(f"Received message: {message.key}")
                if process_message_func:
                    process_message_func(message.key, message.value)
                else:
                    logger.info(f"Message value: {message.value}")
        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
        finally:
            self.close()

    def close(self):
        """Close the consumer connection"""
        self.consumer.close() 
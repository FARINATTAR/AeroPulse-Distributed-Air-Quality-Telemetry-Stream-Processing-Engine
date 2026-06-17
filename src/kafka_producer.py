"""
AeroPulse - High-Throughput Kafka Producer
Publishes serialized air quality telemetry packets to Kafka topics
with partition keys ensuring strict city-level FIFO order.
"""

import json
from typing import Dict, Any
from kafka import KafkaProducer
from loguru import logger
from config import settings


class AeroPulseProducer:
    """Thread-safe Kafka producer managing connection pools and delivery callbacks."""

    def __init__(self):
        self.bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
        self.topic = settings.KAFKA_TOPIC_TELEMETRY
        self.producer = None
        self._connect()

    def _connect(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Kafka producer connected to {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect Kafka producer: {e}")
            self.producer = None

    def publish_telemetry(self, packet: Dict[str, Any]) -> bool:
        """Asynchronously pushes telemetry payload with city partition key."""
        if not self.producer:
            logger.warning("Producer offline. Attempting reconnect...")
            self._connect()
            if not self.producer:
                return False

        city_key = packet.get("city", "UNKNOWN")
        try:
            future = self.producer.send(
                topic=self.topic,
                key=city_key,
                value=packet
            )
            future.add_callback(self._on_success, city_key)
            future.add_errback(self._on_error, city_key)
            return True
        except Exception as ex:
            logger.error(f"Exception during Kafka produce for {city_key}: {ex}")
            return False

    def _on_success(self, metadata, city):
        logger.debug(f"Telemetry published for {city} -> partition={metadata.partition}, offset={metadata.offset}")

    def _on_error(self, exc, city):
        logger.error(f"Failed to deliver message for {city}: {exc}")

    def flush(self):
        if self.producer:
            self.producer.flush()

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed.")

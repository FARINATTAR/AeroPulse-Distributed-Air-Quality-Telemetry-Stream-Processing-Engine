"""
AeroPulse - Main Application & Multi-Threaded Coordinator
Coordinates periodic ingestion, Kafka streaming, and TimescaleDB sink processing.
"""

import signal
import sys
import time
from threading import Thread, Event
import schedule
from loguru import logger

from config import settings
from data_collector import AeroPulseDataCollector
from kafka_producer import AeroPulseProducer
from kafka_consumer import AeroPulseConsumer


class AeroPulseOrchestrator:
    """Manages concurrent background producer and consumer stream workers."""

    def __init__(self):
        self.collector = AeroPulseDataCollector()
        self.producer = AeroPulseProducer()
        self.consumer = AeroPulseConsumer()
        self.running = Event()
        self.running.set()
        self._setup_signals()

    def _setup_signals(self):
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.warning("Shutdown signal received. Stopping AeroPulse workers gracefully...")
        self.running.clear()

    def collect_and_publish_cycle(self):
        """Polls target stations and emits enriched events onto Kafka topic."""
        logger.info(f"Triggering scheduled ingestion cycle for {len(settings.TARGET_CITIES)} stations...")
        for city in settings.TARGET_CITIES:
            packet = self.collector.fetch_city_telemetry(city)
            if packet:
                self.producer.publish_telemetry(packet)
        self.producer.flush()

    def start(self):
        logger.info(f"Starting {settings.APP_NAME} Distributed Streaming Platform...")

        # Run initial collection immediately
        self.collect_and_publish_cycle()

        # Schedule recurring ingestion every 5 minutes
        schedule.every(settings.COLLECTION_INTERVAL_MINUTES).minutes.do(self.collect_and_publish_cycle)

        # Start consumer sink on dedicated daemon thread
        consumer_thread = Thread(target=self.consumer.process_stream, args=(self.running,), daemon=True)
        consumer_thread.start()

        # Main scheduler event loop
        try:
            while self.running.is_set():
                schedule.run_pending()
                time.sleep(1)
        finally:
            self.stop()
            consumer_thread.join(timeout=3)
            logger.info("AeroPulse pipeline halted successfully.")

    def stop(self):
        self.running.clear()
        self.producer.close()
        self.consumer.flush_batch()


if __name__ == "__main__":
    orchestrator = AeroPulseOrchestrator()
    orchestrator.start()

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


def configure_structured_logging():
    """Sets up customized log formatting with clean stream prefixes."""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level=settings.LOG_LEVEL,
        colorize=True
    )


class AeroPulseOrchestrator:
    """Manages concurrent background producer and consumer stream workers."""

    def __init__(self):
        configure_structured_logging()
        self.collector = AeroPulseDataCollector()
        self.producer = AeroPulseProducer()
        self.consumer = AeroPulseConsumer(batch_size=20, flush_interval_secs=8)
        self.running = Event()
        self.running.set()
        self.total_cycles = 0
        self._setup_signals()

    def _setup_signals(self):
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.warning("Graceful shutdown initiated. Terminating AeroPulse pipelines...")
        self.running.clear()

    def collect_and_publish_cycle(self):
        """Polls target stations and emits enriched events onto Kafka topic."""
        self.total_cycles += 1
        logger.info(f"[Cycle #{self.total_cycles}] Dispatching ingestion batch across {len(settings.TARGET_CITIES)} stations...")
        dispatched_count = 0
        for city in settings.TARGET_CITIES:
            packet = self.collector.fetch_city_telemetry(city)
            if packet and self.producer.publish_telemetry(packet):
                dispatched_count += 1
        self.producer.flush()
        logger.info(f"[Cycle #{self.total_cycles}] Emitted {dispatched_count} telemetry packets to Kafka.")

    def start(self):
        logger.info(f"==================================================")
        logger.info(f"  {settings.APP_NAME} Distributed Streaming Platform Online")
        logger.info(f"  Brokers: {settings.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"  Database: {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}")
        logger.info(f"==================================================")

        # Immediate first cycle
        self.collect_and_publish_cycle()

        # Background scheduling
        schedule.every(settings.COLLECTION_INTERVAL_MINUTES).minutes.do(self.collect_and_publish_cycle)

        # Consumer worker on worker thread
        consumer_thread = Thread(target=self.consumer.process_stream, args=(self.running,), daemon=True)
        consumer_thread.start()

        try:
            while self.running.is_set():
                schedule.run_pending()
                time.sleep(1)
        finally:
            self.stop()
            consumer_thread.join(timeout=3)
            logger.info("AeroPulse engine successfully disconnected.")

    def stop(self):
        self.running.clear()
        self.producer.close()
        self.consumer.flush_batch()


if __name__ == "__main__":
    orchestrator = AeroPulseOrchestrator()
    orchestrator.start()

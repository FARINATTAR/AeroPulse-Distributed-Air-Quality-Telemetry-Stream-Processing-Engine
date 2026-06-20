"""
AeroPulse - Stream Consumer & TimescaleDB Sink
Listens to Kafka raw telemetry topics and performs high-velocity batch inserts
into the partitioned TimescaleDB hypertable.
"""

import json
import time
from typing import List, Dict, Any
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger
from config import settings


class AeroPulseConsumer:
    """Consumer worker pulling telemetry messages and batch-persisting to TimescaleDB."""

    def __init__(self, batch_size: int = 15, flush_interval_secs: int = 10):
        self.batch_size = batch_size
        self.flush_interval = flush_interval_secs
        self.consumer = None
        self.db_conn = None
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self._init_db()
        self._init_kafka()

    def _init_db(self):
        try:
            self.db_conn = psycopg2.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                database=settings.POSTGRES_DB,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                connect_timeout=5
            )
            self.db_conn.autocommit = False
            logger.info(f"TimescaleDB sink connected to {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}")
        except Exception as e:
            logger.error(f"Error connecting to TimescaleDB: {e}")
            self.db_conn = None

    def _init_kafka(self):
        try:
            self.consumer = KafkaConsumer(
                settings.KAFKA_TOPIC_TELEMETRY,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=settings.KAFKA_CONSUMER_GROUP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=1000
            )
            logger.info(f"Kafka consumer subscribed to topic '{settings.KAFKA_TOPIC_TELEMETRY}'")
        except Exception as e:
            logger.error(f"Error connecting Kafka consumer: {e}")
            self.consumer = None

    def flush_batch(self):
        if not self.buffer:
            return
        if not self.db_conn:
            self._init_db()
            if not self.db_conn:
                return

        query = """
        INSERT INTO air_quality_telemetry (
            city, country, latitude, longitude, recorded_at,
            aqi_us, aqi_cpcb, cpcb_category,
            pm2_5, pm10, no2, so2, co, o3,
            temperature, humidity, wind_speed, data_source
        ) VALUES %s
        """

        records = [
            (
                p["city"], p["country"], p["latitude"], p["longitude"], p["recorded_at"],
                p.get("aqi_us"), p.get("aqi_cpcb"), p.get("cpcb_category"),
                p.get("pm2_5"), p.get("pm10"), p.get("no2"), p.get("so2"), p.get("co"), p.get("o3"),
                p.get("temperature"), p.get("humidity"), p.get("wind_speed"), p.get("data_source")
            )
            for p in self.buffer
        ]

        try:
            with self.db_conn.cursor() as cur:
                execute_values(cur, query, records)
            self.db_conn.commit()
            logger.info(f"Successfully flushed {len(records)} telemetry records to TimescaleDB")
            self.buffer.clear()
            self.last_flush_time = time.time()
        except Exception as err:
            logger.error(f"Failed to flush batch to database: {err}")
            if self.db_conn:
                self.db_conn.rollback()

    def process_stream(self, running_flag):
        """Main continuous poll-and-buffer loop."""
        while running_flag.is_set():
            if not self.consumer:
                time.sleep(2)
                self._init_kafka()
                continue

            try:
                msg_batch = self.consumer.poll(timeout_ms=1000)
                for partition, messages in msg_batch.items():
                    for record in messages:
                        self.buffer.append(record.value)

                # Flush condition
                if len(self.buffer) >= self.batch_size or (time.time() - self.last_flush_time > self.flush_interval):
                    self.flush_batch()

            except Exception as e:
                logger.error(f"Error in consumer polling loop: {e}")
                time.sleep(1)

        self.flush_batch()

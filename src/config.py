"""
AeroPulse - Configuration & Environment Settings
Handles dynamic configuration loading for ingestion endpoints, Kafka cluster,
TimescaleDB connection pools, and monitoring targets.
"""

import os
from typing import List
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class CityTarget(BaseModel):
    name: str
    country: str
    lat: float
    lon: float
    timezone: str


class AeroPulseSettings(BaseSettings):
    # Application Info
    APP_NAME: str = "AeroPulse"
    ENVIRONMENT: str = Field(default="production", env="ENVIRONMENT")
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    COLLECTION_INTERVAL_MINUTES: int = Field(default=5, env="COLLECTION_INTERVAL_MINUTES")

    # Ingestion API Keys
    OPENWEATHER_API_KEY: str = Field(default="demo_mock_key", env="OPENWEATHER_API_KEY")
    WAQI_API_KEY: str = Field(default="", env="WAQI_API_KEY")

    # Apache Kafka Broker Settings
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC_TELEMETRY: str = "aeropulse.telemetry.raw"
    KAFKA_TOPIC_ALERTS: str = "aeropulse.alerts.critical"
    KAFKA_CONSUMER_GROUP: str = "aeropulse-timescale-sink-group"

    # TimescaleDB / PostgreSQL Settings
    POSTGRES_HOST: str = Field(default="localhost", env="POSTGRES_HOST")
    POSTGRES_PORT: int = Field(default=5432, env="POSTGRES_PORT")
    POSTGRES_DB: str = Field(default="aeropulse", env="POSTGRES_DB")
    POSTGRES_USER: str = Field(default="aeropulse_admin", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field(default="aeropulse_secure_2026", env="POSTGRES_PASSWORD")

    # Monitored Metros (High Pollution Corridors & Global Standards)
    TARGET_CITIES: List[CityTarget] = [
        CityTarget(name="Delhi", country="India", lat=28.6139, lon=77.2090, timezone="Asia/Kolkata"),
        CityTarget(name="Mumbai", country="India", lat=19.0760, lon=72.8777, timezone="Asia/Kolkata"),
        CityTarget(name="Pune", country="India", lat=18.5204, lon=73.8567, timezone="Asia/Kolkata"),
        CityTarget(name="Bengaluru", country="India", lat=12.9716, lon=77.5946, timezone="Asia/Kolkata"),
        CityTarget(name="London", country="UK", lat=51.5074, lon=-0.1278, timezone="Europe/London"),
        CityTarget(name="Tokyo", country="Japan", lat=35.6762, lon=139.6503, timezone="Asia/Tokyo"),
        CityTarget(name="New York", country="USA", lat=40.7128, lon=-74.0060, timezone="America/New_York"),
    ]

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = AeroPulseSettings()

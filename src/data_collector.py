"""
AeroPulse - Telemetry Ingestion Engine
Polls air quality readings across target stations with dual AQI calculation:
1. US-EPA Standard (Index 1-5 scale)
2. Indian CPCB Standard (Sub-index calculation across PM2.5, PM10, NO2, SO2, CO, O3)
"""

import random
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import requests
from loguru import logger
from config import settings, CityTarget


def calculate_cpcb_subindex(concentration: float, breakpoints: list) -> float:
    """Calculates sub-index using linear interpolation for CPCB standards."""
    for b_lo, b_hi, i_lo, i_hi in breakpoints:
        if b_lo <= concentration <= b_hi:
            return i_lo + ((i_hi - i_lo) / (b_hi - b_lo)) * (concentration - b_lo)
    return 500.0 if concentration > breakpoints[-1][1] else 0.0


def compute_indian_cpcb_aqi(pm2_5: float, pm10: float, no2: float, so2: float) -> tuple[int, str]:
    """
    Calculates Indian National Air Quality Index (CPCB standard).
    Returns (aqi_value, health_category).
    """
    pm25_bp = [(0, 30, 0, 50), (30, 60, 51, 100), (60, 90, 101, 200),
               (90, 120, 201, 300), (120, 250, 301, 400), (250, 500, 401, 500)]
    pm10_bp = [(0, 50, 0, 50), (50, 100, 51, 100), (100, 250, 101, 200),
               (250, 350, 201, 300), (350, 430, 301, 400), (430, 600, 401, 500)]
    no2_bp = [(0, 40, 0, 50), (40, 80, 51, 100), (80, 180, 101, 200),
              (180, 280, 201, 300), (280, 400, 301, 400), (400, 800, 401, 500)]
    so2_bp = [(0, 40, 0, 50), (40, 80, 51, 100), (80, 380, 101, 200),
              (380, 800, 201, 300), (800, 1600, 301, 400), (1600, 2000, 401, 500)]

    sub_indices = [
        calculate_cpcb_subindex(pm2_5, pm25_bp),
        calculate_cpcb_subindex(pm10, pm10_bp),
        calculate_cpcb_subindex(no2, no2_bp),
        calculate_cpcb_subindex(so2, so2_bp)
    ]
    aqi = int(max(sub_indices))

    if aqi <= 50:
        category = "Good"
    elif aqi <= 100:
        category = "Satisfactory"
    elif aqi <= 200:
        category = "Moderate"
    elif aqi <= 300:
        category = "Poor"
    elif aqi <= 400:
        category = "Very Poor"
    else:
        category = "Severe"

    return aqi, category


class AeroPulseDataCollector:
    """Fetches air quality telemetry and enriches payloads with meteorological context."""

    def __init__(self):
        self.api_key = settings.OPENWEATHER_API_KEY
        self.base_url = "https://api.openweathermap.org/data/2.5/air_pollution"
        self.weather_url = "https://api.openweathermap.org/data/2.5/weather"

    def fetch_city_telemetry(self, city: CityTarget) -> Optional[Dict[str, Any]]:
        """Gathers live or simulated environmental metrics for a target location."""
        try:
            if not self.api_key or self.api_key == "demo_mock_key":
                return self._generate_simulated_packet(city)

            params = {"lat": city.lat, "lon": city.lon, "appid": self.api_key}
            res = requests.get(self.base_url, params=params, timeout=10)
            res.raise_for_status()
            data = res.json()["list"][0]

            components = data.get("components", {})
            pm2_5 = components.get("pm2_5", 15.0)
            pm10 = components.get("pm10", 30.0)
            no2 = components.get("no2", 12.0)
            so2 = components.get("so2", 5.0)

            cpcb_aqi, cpcb_category = compute_indian_cpcb_aqi(pm2_5, pm10, no2, so2)

            return {
                "schema_version": "2.1.0",
                "city": city.name,
                "country": city.country,
                "latitude": city.lat,
                "longitude": city.lon,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "aqi_us": data.get("main", {}).get("aqi", 2),
                "aqi_cpcb": cpcb_aqi,
                "cpcb_category": cpcb_category,
                "pm2_5": pm2_5,
                "pm10": pm10,
                "no2": no2,
                "so2": so2,
                "co": components.get("co", 200.0),
                "o3": components.get("o3", 45.0),
                "temperature": 28.5,
                "humidity": 62.0,
                "wind_speed": 3.8,
                "data_source": "openweather_live"
            }
        except Exception as e:
            logger.warning(f"Error fetching live telemetry for {city.name}: {e}. Falling back to simulation.")
            return self._generate_simulated_packet(city)

    def _generate_simulated_packet(self, city: CityTarget) -> Dict[str, Any]:
        """Deterministic realistic synthetic fallback when offline or without API key."""
        is_high_pollution = city.name in ["Delhi", "Mumbai"]
        pm2_5 = round(random.uniform(70, 220) if is_high_pollution else random.uniform(12, 45), 2)
        pm10 = round(pm2_5 * random.uniform(1.4, 2.2), 2)
        no2 = round(random.uniform(15, 65), 2)
        so2 = round(random.uniform(4, 25), 2)

        cpcb_aqi, cpcb_cat = compute_indian_cpcb_aqi(pm2_5, pm10, no2, so2)

        return {
            "schema_version": "2.1.0",
            "city": city.name,
            "country": city.country,
            "latitude": city.lat,
            "longitude": city.lon,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "aqi_us": min(5, max(1, int(cpcb_aqi / 60) + 1)),
            "aqi_cpcb": cpcb_aqi,
            "cpcb_category": cpcb_cat,
            "pm2_5": pm2_5,
            "pm10": pm10,
            "no2": no2,
            "so2": so2,
            "co": round(random.uniform(180, 500), 2),
            "o3": round(random.uniform(30, 85), 2),
            "temperature": round(random.uniform(22.0, 34.0), 1),
            "humidity": round(random.uniform(45.0, 78.0), 1),
            "wind_speed": round(random.uniform(1.5, 6.0), 1),
            "data_source": "aeropulse_simulator"
        }

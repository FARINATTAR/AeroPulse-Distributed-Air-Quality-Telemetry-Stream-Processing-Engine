# 🛠️ AeroPulse Operational Commands Reference

Quick reference cheatsheet for managing the AeroPulse data pipeline services and inspecting streams.

## 1. Container Lifecycle
```bash
# Start all services in background
docker compose up -d

# Check cluster health
docker compose ps

# View live application logs
docker compose logs -f aeropulse-engine
```

## 2. Kafka Topic Operations (Inside Container)
```bash
# List all active topics
docker exec -it aeropulse-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume live stream messages in real-time
docker exec -it aeropulse-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic aeropulse.telemetry.raw \
  --from-beginning
```

## 3. TimescaleDB Queries
```bash
# Connect to PostgreSQL / TimescaleDB shell
docker exec -it aeropulse-timescaledb psql -U aeropulse_admin -d aeropulse

# Sample analytical query: Check latest AQI per city
SELECT city, aqi_cpcb, cpcb_category, pm2_5, recorded_at
FROM air_quality_telemetry
ORDER BY recorded_at DESC
LIMIT 10;
```

# ⚡ Quickstart Guide

Get AeroPulse running locally in under 3 minutes.

### Prerequisites
- Docker & Docker Compose installed
- Python 3.10+ (for local development outside Docker)

### Option A: Run Full Stack via Docker
```bash
# 1. Clone repository
git clone https://github.com/FARINATTAR/AeroPulse-Distributed-Air-Quality-Telemetry-Stream-Processing-Engine.git
cd AeroPulse-Distributed-Air-Quality-Telemetry-Stream-Processing-Engine

# 2. Copy environment template
cp .env.example .env

# 3. Start stack
docker compose up -d
```

### Portals:
- **Grafana Dashboards:** [http://localhost:3000](http://localhost:3000) (`admin` / `admin`)
- **Kafka UI Console:** [http://localhost:8080](http://localhost:8080)
- **TimescaleDB:** `localhost:5432` (`aeropulse_admin` / `aeropulse_secure_2026`)

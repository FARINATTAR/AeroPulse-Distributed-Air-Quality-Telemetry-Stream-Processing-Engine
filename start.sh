#!/bin/bash
echo "🌫️ Starting AeroPulse Telemetry Streaming Engine..."
docker compose up -d

echo ""
echo "=================================================="
echo "AeroPulse Infrastructure Ready:"
echo "• Grafana Dashboard:   http://localhost:3000 (admin/admin)"
echo "• Kafka UI Console:    http://localhost:8080"
echo "• TimescaleDB Port:    localhost:5432"
echo "=================================================="

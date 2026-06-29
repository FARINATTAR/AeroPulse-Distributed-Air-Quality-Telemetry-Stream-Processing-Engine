@echo off
echo.
echo ========================================================
echo  AeroPulse: Distributed Air Quality Streaming Platform
echo ========================================================
echo.
docker compose up -d
if %ERRORLEVEL% EQU 0 (
    echo [OK] All services launched successfully!
    echo.
    echo Dashboard Portals:
    echo - Grafana Dashboards: http://localhost:3000 (admin/admin)
    echo - Kafka UI Manager:   http://localhost:8080
    echo - TimescaleDB:        localhost:5432
) else (
    echo [ERROR] Failed to launch containers. Ensure Docker Desktop is active.
)

.PHONY: help build up down restart logs status clean

help:
	@echo "AeroPulse Orchestration CLI"
	@echo "----------------------------------------"
	@echo "make build    - Build Docker images"
	@echo "make up       - Start all cluster services in background"
	@echo "make down     - Stop cluster services"
	@echo "make restart  - Restart entire stack"
	@echo "make logs     - Follow logs across all containers"
	@echo "make status   - Show running container health status"
	@echo "make clean    - Wipe all container volumes and caches"

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f aeropulse-engine

status:
	docker compose ps

clean:
	docker compose down -v
	find . -type d -name "__pycache__" -exec rm -rf {} +

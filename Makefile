.PHONY: help build up down logs clean test dbt-run dashboard

help: ## Show this help message
	@echo "Crypto Data Pipeline - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

build: ## Build all Docker images
	docker-compose build

up: ## Start all services
	docker-compose up -d
	@echo "Services starting..."
	@echo "Dashboard will be available at: http://localhost:8501"
	@echo "Grafana will be available at: http://localhost:3000 (admin/admin123)"

down: ## Stop all services
	docker-compose down

logs: ## View logs from all services
	docker-compose logs -f

logs-pipeline: ## View pipeline logs only
	docker-compose logs -f crypto-pipeline

logs-dashboard: ## View dashboard logs only
	docker-compose logs -f dashboard

clean: ## Clean up containers and volumes
	docker-compose down -v
	docker system prune -f

up-dbt: ## Run dbt transformations
	docker-compose up -d dbt-service

dbt-run: ## Run dbt transformations
	docker-compose run --rm dbt-service dbt run --project-dir /app/dbt

dbt-deps:
	docker-compose run --rm dbt-service dbt deps --project-dir /app/dbt

dbt-test: ## Run dbt tests
	docker-compose run --rm dbt-service dbt test --project-dir /app/dbt

dbt-docs: ## Generate and serve dbt documentation
	docker-compose run --rm dbt-service dbt docs generate --project-dir /app/dbt
	docker-compose run --rm dbt-service dbt docs serve --project-dir /app/dbt --port 8080

pipeline-health: ## Check pipeline health
	docker-compose exec crypto-pipeline python -c 'import sys; \
sys.path.append("/app/src"); \
from monitoring.health_monitor import PipelineHealthMonitor; \
from loaders.warehouse_loader import WarehouseLoader; \
from config.settings import DatabaseConfig; \
loader = WarehouseLoader(DatabaseConfig()); \
monitor = PipelineHealthMonitor(loader); \
health = monitor.check_pipeline_health(); \
print("Pipeline Health:", health["status"])'


run-manual-extraction: ## Run manual data extraction
	docker-compose exec crypto-pipeline python src/run_manual_extraction.py


setup-dev: ## Setup development environment
	python -m venv venv
	source venv/bin/activate && pip install -r requirements.txt -r requirements-dashboard.txt
	@echo "Development environment setup complete"
	@echo "Activate with: source venv/bin/activate"

setup-dev-windows: ## Setup development environment (Windows)
	python -m venv venv
	venv\Scripts\pip install -r requirements.txt -r requirements-dashboard.txt
	@echo "Development environment setup complete"
	@echo "Activate with: venv\Scripts\activate"

test: ## Run tests
	docker-compose exec crypto-pipeline python -m pytest tests/ -v

test-local: ## Run tests locally (requires local setup)
	pip install pytest pytest-asyncio
	python -m pytest tests/ -v

test-local-cov: ## Run tests with coverage locally
	pip install pytest pytest-asyncio pytest-cov
	python -m pytest tests/ -v --cov=src/ --cov-report=html --cov-report=term

.PHONY: db-migrate
db-migrate: ## Run database migrations
	docker-compose exec crypto-pipeline alembic -c /app/alembic.ini upgrade head

.PHONY: db-migrate-down
db-migrate-down: ## Rollback database migrations
	docker-compose exec crypto-pipeline alembic -c /app/alembic.ini downgrade -1

.PHONY: db-migrate-create
db-migrate-create: ## Create a new database migration
	docker-compose exec crypto-pipeline alembic -c /app/alembic.ini revision --autogenerate -m "$(MESSAGE)"

.PHONY: lint
lint: ## Run code linting
	docker-compose exec crypto-pipeline flake8 src/ tests/
	docker-compose exec crypto-pipeline black --check src/ tests/

.PHONY: format
format: ## Format code with black
	docker-compose exec crypto-pipeline black src/ tests/

.PHONY: dbt-lint
dbt-lint: ## Run dbt linting
	docker-compose run --rm dbt-service dbt deps --project-dir /app/dbt
	docker-compose run --rm dbt-service dbt parse --project-dir /app/dbt

.PHONY: dbt-clean
dbt-clean: ## Clean dbt target directory
	docker-compose run --rm dbt-service rm -rf /app/dbt/target

dashboard: ## Open dashboard in browser (macOS/Linux)
	@echo "Opening dashboard at http://localhost:8501"
	@which open >/dev/null && open http://localhost:8501 || which xdg-open >/dev/null && xdg-open http://localhost:8501 || echo "Please open http://localhost:8501 in your browser"

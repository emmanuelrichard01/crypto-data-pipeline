.PHONY: help build up down logs clean test dbt-run dashboard

help: ## Show this help message
	@echo "Crypto Data Pipeline - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

# Use docker-compose with .env file
DC = docker-compose --env-file .env

up: ## Start all services
	$(DC) up -d
	@echo "Services starting..."
	@echo "Dashboard will be available at: http://localhost:8501"
	@echo "Grafana will be available at: http://localhost:4001 (admin/admin123)"

down:
	$(DC) down

build:
	$(DC) build

logs:
	$(DC) logs -f

logs-pipeline:
	$(DC) logs -f crypto-pipeline

logs-dashboard:
	$(DC) logs -f dashboard

dbt-deps: ## Install dbt dependencies
	$(DC) run --rm dbt-service dbt deps --project-dir /app/dbt

dbt-seed: dbt-deps ## Load seed data into the database
	@echo "Loading seed data..."
	$(DC) run --rm dbt-service dbt seed --project-dir /app/dbt --full-refresh

dbt-run: ## Run dbt
	$(DC) run --rm dbt-service dbt run --project-dir /app/dbt

dbt-test: dbt-deps ## Run dbt tests
	$(DC) run --rm dbt-service dbt test --project-dir /app/dbt

pipeline-health:
	$(DC) run --rm crypto-pipeline python src/health_monitor.py

run-manual-extraction: ## Run a manual data extraction
	$(DC) run --rm crypto-pipeline python -u main.py manual

extract-test: ## Test the extraction pipeline
	$(DC) run --rm \
		-e PYTHONUNBUFFERED=1 \
		-e LOG_LEVEL=DEBUG \
		crypto-pipeline python -u main.py manual

clean:
	$(DC) down -v --remove-orphans
	docker system prune -f


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

secure-check: ## Check security configuration
	@echo "Checking security configuration..."
	@if [ -f .env ]; then \
		if grep -q "crypto_password_123\|your_password_here\|change_me" .env; then \
			echo "⚠️  Warning: Default or example passwords found in .env"; \
			exit 1; \
		fi; \
		if [ ! -s .env ]; then \
			echo "⚠️  Warning: .env file is empty"; \
			exit 1; \
		fi; \
		echo "✅ .env file check passed"; \
	else \
		echo "⚠️  Warning: .env file not found"; \
		exit 1; \
	fi
	@echo "✅ Security check complete"

dashboard: secure-check ## Open dashboard in browser (macOS/Linux)
	@echo "Opening dashboard at http://localhost:8501"
	@which open >/dev/null && open http://localhost:8501 || which xdg-open >/dev/null && xdg-open http://localhost:8501 || echo "Please open http://localhost:8501 in your browser"

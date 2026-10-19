# =============================================================================
# Enterprise Open Data Lakehouse Platform — Makefile
# Standard development, testing, and operational command automation
# =============================================================================

.DEFAULT_GOAL := help
PYTHON := python
DOCKER_COMPOSE := docker compose

.PHONY: help install up down restart status logs ps \
        generator stream transform maintenance dashboard \
        test benchmark lint format clean

help: ## Show this help message
	@echo "Enterprise Open Data Lakehouse Platform"
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ── Infrastructure & Containers ──────────────────────────────────────────────

up: ## Start all Lakehouse Docker services (Kafka, Spark, Nessie, MinIO, Trino)
	$(DOCKER_COMPOSE) up -d

down: ## Stop all Lakehouse Docker services
	$(DOCKER_COMPOSE) down

down-v: ## Tear down containers and wipe all local storage volumes
	$(DOCKER_COMPOSE) down -v

restart: down up ## Restart all Lakehouse Docker containers

status: ## Show running container health and status
	$(DOCKER_COMPOSE) ps

logs: ## Tail container logs (usage: make logs c=spark-master)
	$(DOCKER_COMPOSE) logs -f $(c)

# ── Pipeline Execution ───────────────────────────────────────────────────────

generator: ## Run the high-throughput Kafka telemetry generator
	$(PYTHON) scripts/kafka_data_generator.py --workers 2

generator-throttled: ## Run generator at steady-state 1000 events/sec
	$(PYTHON) scripts/kafka_data_generator.py --workers 1 --rate 1000

stream: ## Launch the PySpark Structured Streaming Kafka -> Iceberg job
	docker exec -it spark-master /opt/spark/bin/spark-submit \
		--master local[4] \
		--driver-memory 1g \
		--conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 \
		--conf spark.sql.shuffle.partitions=8 \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18 \
		/opt/spark/work-dir/streaming/04_iceberg_stream.py

transform: ## Run batch deduplication and Gold mart materialization
	docker exec -it spark-master /opt/spark/bin/spark-submit \
		--master local[4] \
		--driver-memory 1g \
		--conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 \
		--conf spark.sql.shuffle.partitions=8 \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18 \
		/opt/spark/work-dir/complex_transforms/05_iceberg_transforms.py

maintenance: ## Run table bin-packing compaction and snapshot expiration
	docker exec -it spark-master /opt/spark/bin/spark-submit \
		--master local[4] \
		--driver-memory 1g \
		--conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 \
		--conf spark.sql.shuffle.partitions=8 \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18 \
		/opt/spark/work-dir/streaming/06_iceberg_maintenance.py

dashboard: ## Start the FastAPI Control Tower web dashboard
	$(PYTHON) -m uvicorn dashboard.main:app --host 0.0.0.0 --port 8000 --reload

trino-cli: ## Open Trino interactive SQL CLI inside container
	docker exec -it trino trino --catalog iceberg --schema marts

# ── Verification & Quality ───────────────────────────────────────────────────

test: ## Run dataset and schema quality validation assertions
	$(PYTHON) tests/validate_dataset.py

benchmark: ## Run the full 5-stage engineering benchmark suite
	$(PYTHON) tests/benchmark_suite.py --test all

lint: ## Run code linter
	ruff check .

format: ## Format code with Ruff and Black
	ruff format .
	black .

# ── Cleanup ──────────────────────────────────────────────────────────────────

clean: ## Clean local build artifacts, pycache, and temp files
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
	rm -rf build/ dist/ *.egg-info/

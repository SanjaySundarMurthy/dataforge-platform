.PHONY: help build up down logs ps test seed clean deploy-all
.DEFAULT_GOAL := help

# ── Variables ───────────────────────────────────────────────
COMPOSE := docker compose -f docker/docker-compose.yml
COMPOSE_MON := docker compose -f docker/docker-compose.monitoring.yml
ENV_FILE := .env
TERRAFORM_DIR := infrastructure
HELM_RELEASE := dataforge

-include $(ENV_FILE)

# ── Colors ──────────────────────────────────────────────────
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m

# ════════════════════════════════════════════════════════════
# Help
# ════════════════════════════════════════════════════════════
help: ## Show this help message
	@echo ""
	@echo "$(BLUE)╔══════════════════════════════════════════════╗$(RESET)"
	@echo "$(BLUE)║     🔥 DataForge Platform - Commands         ║$(RESET)"
	@echo "$(BLUE)╚══════════════════════════════════════════════╝$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ════════════════════════════════════════════════════════════
# 🐳 Docker
# ════════════════════════════════════════════════════════════
build: ## Build all Docker images
	$(COMPOSE) build
	$(COMPOSE_MON) build

up: env-check ## Start all services (one-click setup)
	$(COMPOSE) up -d
	$(COMPOSE_MON) up -d
	@echo "$(GREEN)✅ DataForge Platform is running!$(RESET)"
	@echo "  📊 Grafana:      http://localhost:3000"
	@echo "  🚀 API:          http://localhost:8000/docs"
	@echo "  ⚡ Spark Master: http://localhost:8080"
	@echo "  📈 Prometheus:   http://localhost:9090"

down: ## Stop all services
	$(COMPOSE_MON) down
	$(COMPOSE) down

restart: down up ## Restart all services

logs: ## View logs from all services
	$(COMPOSE) logs -f

logs-spark: ## View Spark logs
	$(COMPOSE) logs -f spark-master spark-worker

logs-api: ## View API logs
	$(COMPOSE) logs -f api

ps: ## Show running containers
	$(COMPOSE) ps
	$(COMPOSE_MON) ps

# ════════════════════════════════════════════════════════════
# 🧪 Testing
# ════════════════════════════════════════════════════════════
test: test-spark test-api test-dbt ## Run all tests

test-spark: ## Run Spark job tests
	cd spark-jobs && python -m pytest tests/ -v --tb=short

test-api: ## Run API tests
	cd api && python -m pytest tests/ -v --tb=short

test-dbt: ## Run dbt tests
	cd data-warehouse/dbt && dbt test

test-quality: ## Run data quality checks
	cd spark-jobs && python -m pytest tests/test_data_quality.py -v

lint: ## Run linters
	cd spark-jobs && python -m flake8 src/ --max-line-length=120
	cd api && python -m flake8 app/ --max-line-length=120

format: ## Format code
	cd spark-jobs && python -m black src/ tests/
	cd api && python -m black app/ tests/

# ════════════════════════════════════════════════════════════
# 📊 Data Operations
# ════════════════════════════════════════════════════════════
seed: ## Generate and load sample e-commerce data
	$(COMPOSE) run --rm data-generator python -m src.generate
	@echo "$(GREEN)✅ Sample data loaded!$(RESET)"

run-pipeline: ## Execute full ETL pipeline (Bronze → Silver → Gold)
	$(COMPOSE) exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/app/src/pipeline.py
	@echo "$(GREEN)✅ ETL pipeline completed!$(RESET)"

run-spark-bronze: ## Run Bronze ingestion
	$(COMPOSE) exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/app/src/ingestion/file_ingestion.py

run-spark-silver: ## Run Silver transformation
	$(COMPOSE) exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/app/src/transformations/bronze_to_silver.py

run-spark-gold: ## Run Gold aggregation
	$(COMPOSE) exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/app/src/transformations/silver_to_gold.py

run-dbt: ## Run dbt transformations
	cd data-warehouse/dbt && dbt run

run-dbt-docs: ## Generate and serve dbt documentation
	cd data-warehouse/dbt && dbt docs generate && dbt docs serve

# ════════════════════════════════════════════════════════════
# 🏗️ Infrastructure (Terraform)
# ════════════════════════════════════════════════════════════
infra-init: ## Initialize Terraform
	cd $(TERRAFORM_DIR) && terraform init

infra-plan: ## Plan infrastructure changes
	cd $(TERRAFORM_DIR) && terraform plan -out=tfplan

infra-apply: ## Apply infrastructure changes
	cd $(TERRAFORM_DIR) && terraform apply tfplan

infra-destroy: ## Destroy all infrastructure (⚠️ DANGEROUS)
	@echo "$(RED)⚠️  This will destroy ALL infrastructure!$(RESET)"
	@read -p "Are you sure? (yes/no): " confirm && [ "$$confirm" = "yes" ] || exit 1
	cd $(TERRAFORM_DIR) && terraform destroy

infra-output: ## Show Terraform outputs
	cd $(TERRAFORM_DIR) && terraform output

# ════════════════════════════════════════════════════════════
# ☸️ Kubernetes
# ════════════════════════════════════════════════════════════
k8s-apply: ## Apply all Kubernetes manifests
	kubectl apply -f kubernetes/namespaces/
	kubectl apply -f kubernetes/spark/
	kubectl apply -f kubernetes/api/
	kubectl apply -f kubernetes/monitoring/

k8s-delete: ## Delete all Kubernetes resources
	kubectl delete -f kubernetes/monitoring/ --ignore-not-found
	kubectl delete -f kubernetes/api/ --ignore-not-found
	kubectl delete -f kubernetes/spark/ --ignore-not-found
	kubectl delete -f kubernetes/namespaces/ --ignore-not-found

# ════════════════════════════════════════════════════════════
# ⎈ Helm
# ════════════════════════════════════════════════════════════
helm-install: ## Install DataForge via Helm
	helm install $(HELM_RELEASE) helm-charts/dataforge-platform/ \
		--namespace dataforge --create-namespace \
		-f helm-charts/dataforge-platform/values.yaml

helm-upgrade: ## Upgrade DataForge Helm release
	helm upgrade $(HELM_RELEASE) helm-charts/dataforge-platform/ \
		--namespace dataforge \
		-f helm-charts/dataforge-platform/values.yaml

helm-uninstall: ## Uninstall DataForge Helm release
	helm uninstall $(HELM_RELEASE) --namespace dataforge

helm-template: ## Render Helm templates locally
	helm template $(HELM_RELEASE) helm-charts/dataforge-platform/

# ════════════════════════════════════════════════════════════
# 📈 Monitoring
# ════════════════════════════════════════════════════════════
dashboard: ## Open Grafana dashboard
	@echo "$(BLUE)Opening Grafana at http://localhost:3000$(RESET)"
	@echo "  User: admin / Password: dataforge123"

alerts: ## Show active Prometheus alerts
	@curl -s http://localhost:9093/api/v2/alerts | python -m json.tool 2>/dev/null || echo "AlertManager not running"

metrics: ## Show key Prometheus metrics
	@curl -s http://localhost:9090/api/v1/targets | python -m json.tool 2>/dev/null || echo "Prometheus not running"

# ════════════════════════════════════════════════════════════
# 🚀 Full Deployment
# ════════════════════════════════════════════════════════════
deploy-all: infra-apply deploy-apps ## Deploy everything (infra + apps)

deploy-apps: ## Deploy applications to K8s
	@echo "$(BLUE)Deploying applications...$(RESET)"
	$(MAKE) helm-install

# ════════════════════════════════════════════════════════════
# 🧹 Cleanup
# ════════════════════════════════════════════════════════════
clean: ## Clean up local artifacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf data/ logs/ 2>/dev/null || true
	@echo "$(GREEN)✅ Cleaned up!$(RESET)"

clean-all: clean down ## Clean everything including containers
	docker system prune -f

# ════════════════════════════════════════════════════════════
# 🔧 Utilities
# ════════════════════════════════════════════════════════════
env-check: ## Verify environment setup
	@test -f $(ENV_FILE) || (cp .env.example .env && echo "$(YELLOW)Created .env from .env.example$(RESET)")

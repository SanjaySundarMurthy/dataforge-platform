# 🔥 DataForge Platform

### Enterprise Data Engineering Platform — End-to-End

> A production-grade, fully automated data engineering platform built with Azure Data Factory, Spark, Databricks, Data Warehouse, Terraform, CI/CD, Docker, Kubernetes, Helm, and comprehensive monitoring. One-click setup. Zero compromise.

[![CI Pipeline](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=github-actions)](/.github/workflows/ci.yml)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?logo=terraform)](./infrastructure)
[![Docker](https://img.shields.io/badge/Container-Docker-2496ED?logo=docker)](./docker)
[![Kubernetes](https://img.shields.io/badge/Orchestration-Kubernetes-326CE5?logo=kubernetes)](./kubernetes)
[![Helm](https://img.shields.io/badge/Package-Helm-0F1689?logo=helm)](./helm-charts)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

---

## 📐 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           DataForge Platform Architecture                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
│  │  REST APIs   │  │  Databases   │  │  CSV/JSON    │  │  Streaming   │  DATA       │
│  │  (External)  │  │  (PostgreSQL)│  │  (Files)     │  │  (Events)    │  SOURCES    │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘             │
│         │                 │                  │                  │                     │
│  ═══════╪═════════════════╪══════════════════╪══════════════════╪═══════════════════  │
│         │          INGESTION LAYER (Azure Data Factory)        │                     │
│         └─────────────────┴──────────────────┴──────────────────┘                     │
│                                    │                                                  │
│  ┌─────────────────────────────────▼──────────────────────────────────┐               │
│  │                     DATA LAKE (ADLS Gen2)                          │               │
│  │  ┌──────────────┐  ┌──────────────────┐  ┌──────────────────┐     │               │
│  │  │ 🥉 BRONZE    │  │  🥈 SILVER       │  │  🥇 GOLD         │     │               │
│  │  │ Raw/As-Is    │→→│  Clean/Validated │→→│  Business-Ready  │     │               │
│  │  │ (Parquet)    │  │  (Delta Lake)    │  │  (Delta Lake)    │     │               │
│  │  └──────────────┘  └──────────────────┘  └──────────────────┘     │               │
│  └────────────────────────────────────────────────────────────────────┘               │
│                                    │                                                  │
│         ┌──────────────────────────┼──────────────────────────┐                       │
│         │                          │                          │                       │
│  ┌──────▼───────┐  ┌──────────────▼───────────┐  ┌──────────▼─────────┐              │
│  │   Spark      │  │    Databricks            │  │   dbt              │              │
│  │   (PySpark)  │  │    (Notebooks + Jobs)    │  │   (Transformations)│              │
│  │   ETL Jobs   │  │    ML Feature Eng.       │  │   Data Warehouse   │              │
│  └──────────────┘  └──────────────────────────┘  └────────────────────┘              │
│                                    │                                                  │
│  ┌─────────────────────────────────▼──────────────────────────────────┐               │
│  │              DATA WAREHOUSE (Azure Synapse / PostgreSQL)           │               │
│  │  ┌─────────┐  ┌───────────┐  ┌───────────┐  ┌──────────────────┐ │               │
│  │  │   dim_  │  │   fact_   │  │   agg_    │  │   analytics_     │ │               │
│  │  │customers│  │  orders   │  │daily_sales│  │  customer_360    │ │               │
│  │  │products │  │order_items│  │product_kpi│  │  revenue_report  │ │               │
│  │  └─────────┘  └───────────┘  └───────────┘  └──────────────────┘ │               │
│  └────────────────────────────────────────────────────────────────────┘               │
│                                    │                                                  │
│  ┌─────────────────────────────────▼──────────────────────────────────┐               │
│  │                    SERVING LAYER (FastAPI)                         │               │
│  │  /api/v1/analytics  /api/v1/health  /api/v1/metrics               │               │
│  └────────────────────────────────────────────────────────────────────┘               │
│                                                                                      │
│  ═══════════════════════ INFRASTRUCTURE LAYER ═══════════════════════                │
│  ┌────────────┐ ┌──────────┐ ┌────────┐ ┌──────┐ ┌────────────────┐                │
│  │ Terraform  │ │  Docker  │ │  K8s   │ │ Helm │ │ GitHub Actions │                │
│  └────────────┘ └──────────┘ └────────┘ └──────┘ └────────────────┘                │
│                                                                                      │
│  ═══════════════════════ MONITORING LAYER ═══════════════════════════                │
│  ┌────────────┐ ┌──────────┐ ┌──────────────┐ ┌───────────────────┐                │
│  │ Prometheus │ │ Grafana  │ │ AlertManager │ │ Loki (Logging)    │                │
│  └────────────┘ └──────────┘ └──────────────┘ └───────────────────┘                │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🗂️ Project Structure

```
dataforge-platform/
│
├── infrastructure/          # 🏗️  Terraform IaC (Azure)
│   ├── modules/             #     Reusable Terraform modules
│   │   ├── networking/      #     VNet, Subnets, NSGs
│   │   ├── data-lake/       #     ADLS Gen2 Storage
│   │   ├── data-factory/    #     Azure Data Factory
│   │   ├── databricks/      #     Azure Databricks Workspace
│   │   ├── synapse/         #     Azure Synapse Analytics
│   │   ├── aks/             #     Azure Kubernetes Service
│   │   ├── container-registry/ #  Azure Container Registry
│   │   ├── key-vault/       #     Azure Key Vault
│   │   └── monitoring/      #     Azure Monitor + Log Analytics
│   └── environments/        #     Per-environment configs
│
├── data-factory/            # 🏭  Azure Data Factory Pipelines
│   ├── pipeline/            #     Pipeline definitions
│   ├── dataset/             #     Dataset definitions
│   ├── linkedService/       #     Connection configurations
│   ├── trigger/             #     Scheduling triggers
│   └── dataflow/            #     Mapping data flows
│
├── spark-jobs/              # ⚡  PySpark ETL Jobs
│   ├── src/                 #     Source code
│   │   ├── ingestion/       #     Data ingestion modules
│   │   ├── transformations/ #     Bronze→Silver→Gold
│   │   └── quality/         #     Data quality checks
│   └── tests/               #     Unit & integration tests
│
├── databricks/              # 📓  Databricks Notebooks & Jobs
│   ├── notebooks/           #     Interactive notebooks
│   ├── jobs/                #     Job definitions
│   └── init-scripts/        #     Cluster init scripts
│
├── data-warehouse/          # 🏛️  Data Warehouse Layer
│   ├── migrations/          #     Schema migrations
│   └── dbt/                 #     dbt transformations
│       ├── models/          #     staging → marts
│       ├── tests/           #     Data tests
│       └── macros/          #     Reusable SQL macros
│
├── api/                     # 🚀  FastAPI Analytics Service
│   ├── app/                 #     Application code
│   └── tests/               #     API tests
│
├── data-generator/          # 🎲  Realistic Data Generator
│   └── src/                 #     E-commerce data simulation
│
├── docker/                  # 🐳  Docker Configurations
│   ├── docker-compose.yml   #     Full local stack
│   └── docker-compose.monitoring.yml
│
├── kubernetes/              # ☸️  Kubernetes Manifests
│   ├── namespaces/          #     Namespace definitions
│   ├── spark/               #     Spark on K8s
│   ├── api/                 #     API deployment
│   └── monitoring/          #     Monitoring stack
│
├── helm-charts/             # ⎈  Helm Charts
│   ├── dataforge-platform/  #     Umbrella chart
│   ├── dataforge-spark/     #     Spark chart
│   ├── dataforge-api/       #     API chart
│   └── dataforge-monitoring/#     Monitoring chart
│
├── monitoring/              # 📊  Observability Stack
│   ├── prometheus/          #     Metrics collection
│   ├── grafana/             #     Dashboards
│   ├── alertmanager/        #     Alert routing
│   └── loki/               #     Log aggregation
│
├── .github/workflows/       # 🔄  CI/CD Pipelines
│   ├── ci.yml               #     Lint, Test, Build
│   ├── cd-infra.yml         #     Infrastructure deployment
│   ├── cd-apps.yml          #     Application deployment
│   └── security-scan.yml    #     Security scanning
│
├── scripts/                 # 🛠️  Automation Scripts
│   ├── setup.sh             #     One-click Linux/Mac setup
│   ├── setup.ps1            #     One-click Windows setup
│   └── seed-data.sh         #     Load sample data
│
└── docs/                    # 📚  Documentation
    ├── architecture/        #     Architecture deep-dives
    ├── guides/              #     How-to guides
    └── concepts/            #     Concept explanations
```

---

## 🚀 Quick Start (One-Click Setup)

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Terraform 1.5+ (for cloud deployment)
- kubectl & helm (for K8s deployment)

### Local Development (Docker Compose)

```bash
# Clone the repository
git clone https://github.com/SanjaySundarMurthy/dataforge-platform.git
cd dataforge-platform

# One-click setup - starts everything
make up

# Or step by step:
make build          # Build all containers
make start          # Start all services
make seed           # Load sample e-commerce data
make test           # Run all tests
make dashboard      # Open Grafana dashboard
```

### Windows (PowerShell)

```powershell
# One-click setup
.\scripts\setup.ps1

# Or use Make
make up
```

### Cloud Deployment (Azure)

```bash
# Initialize infrastructure
make infra-init

# Plan and apply
make infra-plan
make infra-apply

# Deploy applications
make deploy-apps

# Full deployment (infra + apps)
make deploy-all
```

---

## 🎯 What You'll Learn

| Technology | Concepts Covered |
|-----------|-----------------|
| **Azure Data Factory** | Pipelines, Datasets, Linked Services, Triggers, Data Flows, Parameterization |
| **Apache Spark** | PySpark, RDDs, DataFrames, Spark SQL, Partitioning, Caching, UDFs |
| **Databricks** | Notebooks, Clusters, Jobs, Delta Lake, Unity Catalog, MLflow |
| **Data Warehouse** | Star Schema, Slowly Changing Dimensions, Fact Tables, Aggregations |
| **dbt** | Models, Tests, Macros, Seeds, Snapshots, Documentation |
| **Terraform** | Modules, State Management, Workspaces, Variables, Outputs |
| **Docker** | Multi-stage Builds, Compose, Networking, Volumes, Health Checks |
| **Kubernetes** | Deployments, Services, ConfigMaps, Secrets, HPA, PDB, RBAC |
| **Helm** | Charts, Values, Templates, Dependencies, Hooks |
| **CI/CD** | GitHub Actions, Multi-stage Pipelines, Environment Promotion |
| **Monitoring** | Prometheus, Grafana, Alerting, Loki, Distributed Tracing |
| **Data Quality** | Great Expectations, Schema Validation, Freshness Checks |

---

## 📊 Data Model (E-Commerce)

### Source Tables
- **customers** — Customer demographics and segments
- **products** — Product catalog with categories
- **orders** — Order transactions
- **order_items** — Line items per order
- **clickstream** — User behavior events
- **reviews** — Product reviews and ratings

### Medallion Architecture

| Layer | Purpose | Format | Example |
|-------|---------|--------|---------|
| 🥉 Bronze | Raw ingestion, as-is | Parquet | `bronze/orders/2024/01/01/` |
| 🥈 Silver | Cleaned, typed, deduplicated | Delta Lake | `silver/orders/` |
| 🥇 Gold | Business aggregations | Delta Lake | `gold/daily_sales/` |

### Data Warehouse (Star Schema)

```
                    ┌──────────────┐
                    │ dim_date     │
                    └──────┬───────┘
                           │
┌──────────────┐  ┌────────┴───────┐  ┌──────────────┐
│ dim_customer │──│  fact_orders   │──│ dim_product   │
└──────────────┘  └────────┬───────┘  └──────────────┘
                           │
                    ┌──────┴───────┐
                    │fact_order_   │
                    │   items      │
                    └──────────────┘
```

---

## 🔧 Make Targets

```bash
make help              # Show all available commands

# 🐳 Docker
make build             # Build all Docker images
make up                # Start all services
make down              # Stop all services
make logs              # View all logs
make ps                # Show running containers

# 🧪 Testing
make test              # Run all tests
make test-spark        # Run Spark job tests
make test-api          # Run API tests
make test-dbt          # Run dbt tests
make test-quality      # Run data quality checks

# 🏗️ Infrastructure
make infra-init        # Initialize Terraform
make infra-plan        # Plan infrastructure changes
make infra-apply       # Apply infrastructure
make infra-destroy     # Destroy infrastructure

# 📊 Data Operations
make seed              # Load sample data
make run-pipeline      # Execute full ETL pipeline
make run-spark         # Run Spark jobs
make run-dbt           # Run dbt transformations

# 📈 Monitoring
make dashboard         # Open Grafana
make alerts            # Show active alerts
make metrics           # Show Prometheus metrics

# 🚀 Deployment
make deploy-all        # Deploy everything
make deploy-infra      # Deploy infrastructure only
make deploy-apps       # Deploy applications only
```

---

## 🏗️ Environments

| Environment | Purpose | Infrastructure |
|-------------|---------|---------------|
| **local** | Development & learning | Docker Compose |
| **dev** | Integration testing | Azure (minimal) |
| **staging** | Pre-production | Azure (scaled down) |
| **prod** | Production | Azure (full scale) |

---

## 📈 Monitoring & Observability

| Tool | Purpose | URL (Local) |
|------|---------|-------------|
| **Grafana** | Dashboards & visualization | http://localhost:3000 |
| **Prometheus** | Metrics collection | http://localhost:9090 |
| **AlertManager** | Alert routing | http://localhost:9093 |
| **Loki** | Log aggregation | http://localhost:3100 |

### Pre-built Dashboards
- **Pipeline Overview** — ETL job status, durations, data volumes
- **Infrastructure Health** — CPU, memory, disk, network
- **Data Quality** — Validation pass/fail rates, schema drift
- **API Performance** — Request rates, latencies, error rates

---

## 🔒 Security

- All secrets managed via Azure Key Vault / K8s Secrets
- Network isolation with VNet and NSGs
- RBAC for all Azure resources
- Container image scanning in CI/CD
- No hardcoded credentials anywhere
- `.env` files in `.gitignore`

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<p align="center">
  <b>Built with ❤️ for the Data Engineering Community</b><br>
  <i>Star ⭐ this repo if you find it useful!</i>
</p>

# ⛵ Helm Charts — DataForge Platform

> Helm umbrella chart for deploying the entire DataForge platform to Kubernetes.

---

## 📁 Structure

```
helm-charts/
└── dataforge-platform/
    ├── Chart.yaml          # Chart metadata (v0.1.0)
    ├── values.yaml         # Default configuration values
    └── templates/
        ├── api.yaml        # API Deployment + Service + PDB + ServiceAccount
        ├── spark.yaml      # Spark Master + Worker + Services
        ├── monitoring.yaml # Prometheus + Grafana
        └── NOTES.txt       # Post-install instructions
```

---

## 🚀 Installation

```bash
# Install with defaults
helm install dataforge helm-charts/dataforge-platform

# Install with custom values
helm install dataforge helm-charts/dataforge-platform \
  --set api.replicas=5 \
  --set api.image.tag=v1.2.0 \
  --set grafana.adminPassword="$(openssl rand -base64 32)"

# Upgrade
helm upgrade dataforge helm-charts/dataforge-platform \
  --set api.image.tag=v1.3.0

# Uninstall
helm uninstall dataforge
```

---

## ⚙️ Key Values

```yaml
# API Configuration
api:
  replicas: 3
  image:
    repository: dataforgeacr.azurecr.io/dataforge-api
    tag: latest
  resources:
    requests: { cpu: 100m, memory: 256Mi }
    limits:   { cpu: 500m, memory: 512Mi }

# Spark Configuration
spark:
  master:
    image: { repository: ..., tag: latest }
  worker:
    replicas: 2

# Grafana — set password via --set or external secret
grafana:
  adminPassword: ""   # Never committed to Git
```

---

## 🔒 Security Features

| Feature | Implementation |
|:---|:---|
| **SecurityContext** | Non-root, read-only FS, drop ALL capabilities |
| **ServiceAccount** | Dedicated SA per workload |
| **PDB** | Minimum 1 pod available during disruptions |
| **No Hardcoded Secrets** | Grafana password set via `--set` or external secret manager |
| **Resource Limits** | CPU/memory limits on all containers |

---

## 📋 NOTES.txt

After installation, Helm displays access instructions:
```
DataForge Platform deployed successfully!

API:     kubectl port-forward svc/dataforge-api 8000:80
Spark:   kubectl port-forward svc/spark-master 8080:8080
Grafana: kubectl port-forward svc/grafana 3000:3000
```

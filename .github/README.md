# 🔄 CI/CD — GitHub Actions Workflows

> 4 workflows covering continuous integration, infrastructure deployment, application deployment, and security scanning.

---

## 📁 Workflows

```mermaid
graph TB
    subgraph "ci.yml — Every PR + Push"
        LINT[Ruff + Black<br/>Lint & Format] --> TEST_SPARK[Spark Tests<br/>pytest + pyspark]
        LINT --> TEST_API[API Tests<br/>pytest + fastapi]
        LINT --> TF_VAL[Terraform<br/>fmt + validate]
        LINT --> HELM_LINT[Helm Lint]
        TEST_SPARK & TEST_API --> BUILD[Docker Build<br/>Matrix: api, spark]
    end

    subgraph "cd-infra.yml — Push to main"
        TF_PLAN[Terraform Plan<br/>Save artifact] --> APPROVE[Manual Approval]
        APPROVE --> TF_APPLY[Terraform Apply<br/>From artifact]
    end

    subgraph "cd-apps.yml — Push to main"
        DOCKER[Docker Build<br/>Push to ACR] --> HELM_DEPLOY[Helm Upgrade<br/>to AKS]
    end

    subgraph "security-scan.yml — Weekly + PR"
        TRIVY[Trivy<br/>Container Scan]
        TFSEC[tfsec<br/>IaC Scan]
        PIP_AUDIT[pip-audit<br/>Dependencies]
    end
```

---

## 📋 Workflow Details

### `ci.yml` — Continuous Integration
| Job | Trigger | Steps |
|:---|:---|:---|
| `lint` | PR + push to main | Ruff lint, Black format check |
| `test-spark` | PR + push to main | Install PySpark 3.5.0, Delta Lake, run pytest |
| `test-api` | PR + push to main | Install FastAPI deps, run pytest |
| `validate-terraform` | PR + push to main | `terraform fmt -check`, `terraform validate` |
| `lint-helm` | PR + push to main | `helm lint` on all charts |
| `build-docker` | After tests pass | Matrix build (api, spark), no push |

### `cd-infra.yml` — Infrastructure Deployment
| Job | Trigger | Steps |
|:---|:---|:---|
| `plan` | Push to main (infrastructure/**) | OIDC login, `terraform plan`, save plan artifact |
| `apply` | After plan + environment approval | Download artifact, `terraform apply` from plan |

**Authentication:** Azure OIDC federation (no stored service principal secrets)

### `cd-apps.yml` — Application Deployment
| Job | Trigger | Steps |
|:---|:---|:---|
| `build-push` | Push to main (api/**, spark-jobs/**) | Build Docker images, push to ACR with SHA tag |
| `deploy` | After build-push | `helm upgrade --install` with image tags |

### `security-scan.yml` — Security Scanning
| Job | Trigger | Steps |
|:---|:---|:---|
| `trivy` | Weekly + PR | Scan Docker images for CVEs |
| `tfsec` | Weekly + PR | Scan Terraform for misconfigurations |
| `pip-audit` | Weekly + PR | Check Python dependencies for vulnerabilities |

---

## 🔒 Security

- **OIDC Authentication** — No stored Azure credentials; uses GitHub→Azure federation
- **Environment Protection** — Production deploys require manual approval
- **Artifact Passing** — Terraform plan saved as artifact, applied from identical plan
- **Minimal Permissions** — Each workflow uses least-privilege RBAC roles

# 🔌 API — FastAPI Analytics Service

> RESTful analytics service with connection pooling, Prometheus metrics, and OpenAPI documentation.

---

## 🏛️ Architecture

```mermaid
graph LR
    CLIENT[Client] -->|HTTP| FASTAPI[FastAPI<br/>Port 8000]
    FASTAPI -->|SQL| POOL[Connection Pool<br/>psycopg2 ThreadedPool]
    POOL -->|Query| PG[(PostgreSQL)]
    FASTAPI -->|/metrics| PROM[Prometheus]
    PROM --> GRAFANA[Grafana]
```

---

## 📁 Structure

```
api/
├── app/
│   ├── main.py        # Application entry point, middleware, endpoints
│   └── models.py      # Pydantic response models
├── tests/
│   └── test_api.py    # pytest test suite
├── Dockerfile         # Multi-stage production build
└── requirements.txt
```

---

## 🔑 Endpoints

| Method | Path | Description |
|:---|:---|:---|
| `GET` | `/` | Root — platform info |
| `GET` | `/health` | Health check — DB connectivity |
| `GET` | `/api/v1/analytics/summary` | Sales summary with trends |
| `GET` | `/api/v1/analytics/top-products` | Top products by revenue |
| `GET` | `/api/v1/analytics/customer-segments` | Customer segment breakdown |
| `GET` | `/metrics` | Prometheus metrics endpoint |
| `GET` | `/docs` | OpenAPI Swagger UI |

---

## 🔒 Security Features

| Feature | Implementation |
|:---|:---|
| **CORS** | Restricted origins via `CORS_ORIGINS` env var (no wildcards) |
| **Credentials** | `allow_credentials=False` (stateless API) |
| **Connection Pool** | `ThreadedConnectionPool` (min=2, max=10), prevents connection exhaustion |
| **No Hardcoded Secrets** | All DB credentials from environment variables |
| **Request Tracking** | Middleware tracks actual HTTP status codes for monitoring |
| **Graceful Shutdown** | Pool cleanup on application shutdown via lifespan |

---

## 🚀 Running

```bash
# Install dependencies
pip install fastapi uvicorn psycopg2-binary pydantic prometheus-client

# Set environment variables
export DATABASE_URL="postgresql://dataforge:dataforge@localhost:5432/dataforge"
export CORS_ORIGINS="http://localhost:3000"

# Run development server
cd api
uvicorn app.main:app --reload --port 8000

# Run tests
python -m pytest tests/ -v
```

---

## 📊 Metrics

The `/metrics` endpoint exposes Prometheus-compatible metrics:

| Metric | Type | Labels |
|:---|:---|:---|
| `http_requests_total` | Counter | `method`, `endpoint`, `status` |
| `http_request_duration_seconds` | Histogram | `method`, `endpoint` |

---

## 🐳 Docker

```bash
# Build
docker build -t dataforge-api -f docker/api/Dockerfile .

# Run
docker run -p 8000:8000 \
  -e DATABASE_URL="postgresql://dataforge:dataforge@host.docker.internal:5432/dataforge" \
  dataforge-api
```

# ============================================================
# DataForge Data Generator
# ============================================================
FROM python:3.11-slim

WORKDIR /app

COPY data-generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data-generator/src/ ./src/

RUN adduser --disabled-password --gecos '' appuser
USER appuser

CMD ["python", "-m", "src.generate"]

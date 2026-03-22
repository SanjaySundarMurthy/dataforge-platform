# ============================================================
# DataForge Spark Image
# ============================================================
# Multi-stage build for PySpark with Delta Lake support
# ============================================================

FROM python:3.11-slim AS base

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYSPARK_PYTHON=python3

# Install Spark
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt/ \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}"

# Install Python dependencies
COPY spark-jobs/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy application code
WORKDIR /app
COPY spark-jobs/ /app/

# Create data directories
RUN mkdir -p /data/landing /data/bronze /data/silver /data/gold

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:${SPARK_MASTER_WEBUI_PORT:-8080}/ || exit 1

# Entrypoint script
COPY docker/spark/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

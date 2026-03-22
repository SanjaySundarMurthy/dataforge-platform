#!/bin/bash
# ============================================================
# Spark Container Entrypoint
# ============================================================
set -e

SPARK_MODE="${SPARK_MODE:-master}"

case "$SPARK_MODE" in
    master)
        echo "🔥 Starting Spark Master..."
        exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
            --host "${SPARK_MASTER_HOST:-0.0.0.0}" \
            --port "${SPARK_MASTER_PORT:-7077}" \
            --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"
        ;;
    worker)
        echo "⚙️  Starting Spark Worker..."
        exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
            "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
            --cores "${SPARK_WORKER_CORES:-2}" \
            --memory "${SPARK_WORKER_MEMORY:-2G}" \
            --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}"
        ;;
    submit)
        echo "📤 Submitting Spark job..."
        exec "${SPARK_HOME}/bin/spark-submit" "$@"
        ;;
    *)
        echo "Unknown SPARK_MODE: $SPARK_MODE"
        echo "Valid modes: master, worker, submit"
        exit 1
        ;;
esac

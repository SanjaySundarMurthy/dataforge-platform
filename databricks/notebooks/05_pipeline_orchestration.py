# Databricks notebook source
# ============================================================
# Notebook 05: Pipeline Orchestration
# ============================================================
# Concepts covered:
# - Databricks Workflows / multi-task orchestration
# - Pipeline state management
# - Error handling & retry logic
# - Notification patterns
# - SLA monitoring
# ============================================================

# COMMAND ----------
# MAGIC %md
# MAGIC # Pipeline Orchestration
# MAGIC This notebook orchestrates the complete ETL pipeline:
# MAGIC 1. Bronze Ingestion (Landing → Bronze)
# MAGIC 2. Silver Transformation (Bronze → Silver)
# MAGIC 3. Gold Aggregation (Silver → Gold)
# MAGIC 4. Data Quality Checks
# MAGIC
# MAGIC It manages state, handles errors, and tracks pipeline metrics.

# COMMAND ----------
# Widgets for parameterization
dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("batch_date", "", "Batch Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("mode", "full", ["full", "incremental", "reprocess"], "Run Mode")
dbutils.widgets.dropdown("fail_on_error", "true", ["true", "false"], "Fail on Error")

environment = dbutils.widgets.get("environment")
batch_date = dbutils.widgets.get("batch_date")
mode = dbutils.widgets.get("mode")
fail_on_error = dbutils.widgets.get("fail_on_error") == "true"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline State Tracking

# COMMAND ----------
from datetime import datetime
from pyspark.sql import functions as F

pipeline_start = datetime.now()
pipeline_log = []

def log_step(step_name, status, records=0, error=None):
    """Log a pipeline step execution."""
    entry = {
        "step": step_name,
        "status": status,
        "records": records,
        "error": str(error) if error else None,
        "timestamp": datetime.now().isoformat(),
        "duration_seconds": (datetime.now() - pipeline_start).total_seconds(),
    }
    pipeline_log.append(entry)
    icon = "✅" if status == "success" else "❌"
    print(f"{icon} {step_name}: {status} ({records:,} records)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Bronze Ingestion

# COMMAND ----------
try:
    result = dbutils.notebook.run(
        "02_bronze_ingestion",
        timeout_seconds=3600,
        arguments={"environment": environment, "batch_date": batch_date}
    )
    log_step("bronze_ingestion", "success")
except Exception as e:
    log_step("bronze_ingestion", "failed", error=e)
    if fail_on_error:
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Silver Transformation

# COMMAND ----------
try:
    result = dbutils.notebook.run(
        "03_silver_transformation",
        timeout_seconds=3600,
        arguments={"environment": environment}
    )
    log_step("silver_transformation", "success")
except Exception as e:
    log_step("silver_transformation", "failed", error=e)
    if fail_on_error:
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Gold Aggregation

# COMMAND ----------
try:
    result = dbutils.notebook.run(
        "04_gold_aggregation",
        timeout_seconds=3600,
        arguments={"environment": environment}
    )
    log_step("gold_aggregation", "success")
except Exception as e:
    log_step("gold_aggregation", "failed", error=e)
    if fail_on_error:
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Data Quality Checks

# COMMAND ----------
try:
    result = dbutils.notebook.run(
        "06_data_quality_checks",
        timeout_seconds=1800,
        arguments={"environment": environment, "fail_on_error": str(fail_on_error).lower()}
    )
    log_step("data_quality", "success")
except Exception as e:
    log_step("data_quality", "failed", error=e)
    if fail_on_error:
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------
pipeline_end = datetime.now()
duration = (pipeline_end - pipeline_start).total_seconds()
failed_steps = [s for s in pipeline_log if s["status"] == "failed"]

print("=" * 60)
print(f"Pipeline Complete — {len(pipeline_log)} steps in {duration:.1f}s")
print(f"  Successful: {len(pipeline_log) - len(failed_steps)}")
print(f"  Failed:     {len(failed_steps)}")
if failed_steps:
    print(f"  Failures:   {[s['step'] for s in failed_steps]}")
print("=" * 60)

# Write pipeline log to Delta table
log_df = spark.createDataFrame(pipeline_log)
log_df = log_df.withColumn("pipeline_run_id", F.lit(f"{batch_date}_{pipeline_start.strftime('%H%M%S')}"))
log_df.write.mode("append").format("delta").saveAsTable("pipeline_audit.run_log")

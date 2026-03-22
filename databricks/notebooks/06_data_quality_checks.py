# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Data Quality Checks
# MAGIC
# MAGIC **DataForge Platform** — Automated DQ Validation
# MAGIC
# MAGIC Runs data quality checks on the Silver layer and reports results.
# MAGIC Can be configured to fail the pipeline if critical checks fail.
# MAGIC
# MAGIC ### Concepts Covered
# MAGIC - Automated data quality validation
# MAGIC - Null checks, uniqueness, range validation
# MAGIC - Referential integrity verification
# MAGIC - Quality reporting and metrics

# COMMAND ----------

dbutils.widgets.text("silver_path", "/mnt/datalake/silver", "Silver Path")
dbutils.widgets.text("fail_on_error", "true", "Fail on Error")

silver_path = dbutils.widgets.get("silver_path")
fail_on_error = dbutils.widgets.get("fail_on_error").lower() == "true"

# COMMAND ----------

from pyspark.sql import functions as F

results = []

def check(name, passed, details=""):
    status = "✅ PASS" if passed else "❌ FAIL"
    results.append({"check": name, "passed": passed, "details": details})
    print(f"  {status} | {name} | {details}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders Quality Checks

# COMMAND ----------

print("=" * 60)
print("ORDERS")
print("=" * 60)

orders = spark.read.format("delta").load(f"{silver_path}/orders")
total = orders.count()

# Not null
nulls = orders.filter(F.col("order_id").isNull()).count()
check("orders.order_id NOT NULL", nulls == 0, f"{nulls} nulls")

# Unique
dupes = total - orders.select("order_id").distinct().count()
check("orders.order_id UNIQUE", dupes == 0, f"{dupes} duplicates")

# Range
negatives = orders.filter(F.col("total_amount") < 0).count()
check("orders.total_amount >= 0", negatives == 0, f"{negatives} negative values")

# Valid status
valid = {"completed", "delivered", "shipped", "pending", "processing", "cancelled", "unknown"}
invalid = orders.filter(~F.col("status").isin(list(valid))).count()
check("orders.status VALID VALUES", invalid == 0, f"{invalid} invalid statuses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers Quality Checks

# COMMAND ----------

print("=" * 60)
print("CUSTOMERS")
print("=" * 60)

customers = spark.read.format("delta").load(f"{silver_path}/customers")

nulls = customers.filter(F.col("customer_id").isNull()).count()
check("customers.customer_id NOT NULL", nulls == 0, f"{nulls} nulls")

dupes = customers.count() - customers.select("customer_id").distinct().count()
check("customers.customer_id UNIQUE", dupes == 0, f"{dupes} duplicates")

null_regions = customers.filter(F.col("region").isNull()).count()
check("customers.region NOT NULL", null_regions == 0, f"{null_regions} nulls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential Integrity

# COMMAND ----------

print("=" * 60)
print("REFERENTIAL INTEGRITY")
print("=" * 60)

# Orders → Customers
orphans = (
    orders.select("customer_id").distinct()
    .join(customers.select("customer_id").distinct(), "customer_id", "left_anti")
    .count()
)
check("orders.customer_id → customers", orphans == 0, f"{orphans} orphans")

# Order Items → Orders
order_items = spark.read.format("delta").load(f"{silver_path}/order_items")
orphans = (
    order_items.select("order_id").distinct()
    .join(orders.select("order_id").distinct(), "order_id", "left_anti")
    .count()
)
check("order_items.order_id → orders", orphans == 0, f"{orphans} orphans")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_checks = len(results)
passed = sum(1 for r in results if r["passed"])
failed = total_checks - passed

print(f"\n{'='*60}")
print(f"DATA QUALITY SUMMARY")
print(f"{'='*60}")
print(f"Total checks: {total_checks}")
print(f"Passed:       {passed}")
print(f"Failed:       {failed}")
print(f"Pass rate:    {passed/total_checks*100:.1f}%")

if failed > 0 and fail_on_error:
    failures = [r for r in results if not r["passed"]]
    msg = "; ".join([f"{r['check']}: {r['details']}" for r in failures])
    raise Exception(f"Data quality gate failed! {failed} checks failed: {msg}")
elif failed > 0:
    print(f"\n⚠️ {failed} checks failed but fail_on_error is disabled")
else:
    print("\n✅ All quality checks passed!")

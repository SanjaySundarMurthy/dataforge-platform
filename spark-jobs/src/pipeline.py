"""
DataForge ETL Pipeline — Full Orchestrator
===========================================
Executes the complete ETL pipeline:
  Landing → Bronze → Silver → Gold → Quality Checks

This is the main entry point for the Spark-based pipeline.
"""

from src.common.spark_session import get_spark_session
from src.common.config import get_config
from src.common.logger import get_logger
from src.ingestion.file_ingestion import run_ingestion
from src.transformations.bronze_to_silver import run_bronze_to_silver
from src.transformations.silver_to_gold import run_silver_to_gold
from src.quality.data_quality import run_quality_checks

logger = get_logger(__name__)


def main():
    """Execute the full DataForge ETL pipeline."""
    config = get_config()

    logger.info("=" * 70)
    logger.info("🔥 DataForge ETL Pipeline — Starting")
    logger.info(f"   Environment: {config.environment}")
    logger.info(f"   Bronze path: {config.storage.bronze_path}")
    logger.info(f"   Silver path: {config.storage.silver_path}")
    logger.info(f"   Gold path:   {config.storage.gold_path}")
    logger.info("=" * 70)

    spark = get_spark_session("DataForge-Pipeline")

    try:
        # Step 1: Ingest from Landing → Bronze
        logger.info("\n📥 STEP 1/4: Ingestion (Landing → Bronze)")
        run_ingestion(spark)

        # Step 2: Transform Bronze → Silver
        logger.info("\n🔄 STEP 2/4: Transformation (Bronze → Silver)")
        run_bronze_to_silver(spark)

        # Step 3: Data Quality Checks on Silver
        logger.info("\n✅ STEP 3/4: Data Quality Checks (Silver)")
        quality_results = run_quality_checks(spark, config.storage.silver_path)

        if quality_results["failed"] > 0:
            logger.warning(
                f"⚠️  {quality_results['failed']} quality checks failed! "
                "Proceeding to Gold with caution."
            )

        # Step 4: Aggregate Silver → Gold
        logger.info("\n🏆 STEP 4/4: Aggregation (Silver → Gold)")
        run_silver_to_gold(spark)

        logger.info("\n" + "=" * 70)
        logger.info("🎉 DataForge ETL Pipeline — COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"💥 Pipeline failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

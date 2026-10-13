import argparse
import os
from pathlib import Path
import sys
import time
from typing import Optional, Sequence

# Safely resolve project root across both CLI and Databricks execution wrappers
if "__file__" in globals():
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
else:
    cwd = Path.cwd().resolve()
    PROJECT_ROOT = cwd.parents[1] if cwd.name == "telemetry" else cwd

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from src.telemetry.audit_views import AuditViewsManager
from src.telemetry.dlt_listener import capture_dlt_pipeline_events
from src.telemetry.logger import TelemetryLogger
from src.utils.config_loader import ConfigLoader


def main(
    argv: Optional[Sequence[str]] = None,
    spark: Optional[SparkSession] = None
) -> None:
    parser = argparse.ArgumentParser(description="Capture DLT events into audit logs.")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--deploy-views", action="store_true", help="Optionally re-deploy SQL views")
    args = parser.parse_args(argv)

    spark = spark or SparkSession.builder.getOrCreate()
    logger = TelemetryLogger(
        spark=spark,
        catalog=args.catalog,
        environment=args.environment
    )
    logger.ensure_audit_table()
    start_time = time.perf_counter()

    try:
        logger.log_event(
            pipeline_name="databricks_daily_workflow",
            step_name="capture_dlt_events",
            status="STARTED",
            additional_metadata={"pipeline_id": args.pipeline_id}
        )
        start_time = time.perf_counter()
        
        # 1. Fast batch DLT event ingestion (~1-2s)
        capture_dlt_pipeline_events(
            spark=spark,
            pipeline_id=args.pipeline_id,
            catalog=args.catalog,
            environment=args.environment
        )
        
        # 2. Re-deploy views ONLY if explicitly requested
        if args.deploy_views:
            config = ConfigLoader(env=args.environment, catalog=args.catalog)
            AuditViewsManager(spark=spark, config_loader=config).deploy_all_views()

        logger.log_event(
            pipeline_name="databricks_daily_workflow",
            step_name="capture_dlt_events",
            status="SUCCESS",
            execution_time_seconds=round(time.perf_counter() - start_time, 4),
            additional_metadata={"pipeline_id": args.pipeline_id}
        )
    except Exception as exc:
        logger.log_exception(
            pipeline_name="databricks_daily_workflow",
            step_name="capture_dlt_events",
            exception=exc,
            execution_time_seconds=round(time.perf_counter() - start_time, 4)
        )
        raise


if __name__ == "__main__":
    main()
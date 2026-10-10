import argparse
from pathlib import Path
import sys
from typing import Optional, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
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
    args = parser.parse_args(argv)

    spark = spark or SparkSession.builder.getOrCreate()
    logger = TelemetryLogger(
        spark=spark,
        catalog=args.catalog,
        environment=args.environment
    )
    logger.ensure_audit_table()

    try:
        logger.log_event(
            pipeline_name="databricks_daily_workflow",
            step_name="capture_dlt_events",
            status="STARTED",
            additional_metadata={"pipeline_id": args.pipeline_id}
        )
        capture_dlt_pipeline_events(
            spark=spark,
            pipeline_id=args.pipeline_id,
            catalog=args.catalog,
            environment=args.environment
        )
        config = ConfigLoader(env=args.environment, catalog=args.catalog)
        AuditViewsManager(spark=spark, config_loader=config).deploy_all_views()
        logger.log_event(
            pipeline_name="databricks_daily_workflow",
            step_name="capture_dlt_events",
            status="SUCCESS",
            additional_metadata={"pipeline_id": args.pipeline_id}
        )
    except Exception as exc:
        logger.log_exception(
            pipeline_name="databricks_daily_workflow",
            step_name="capture_dlt_events",
            exception=exc
        )
        raise


if __name__ == "__main__":
    main()

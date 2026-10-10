from pyspark.sql import SparkSession
from src.utils.config_loader import ConfigLoader


class AuditViewsManager:
    """
    Manages creation and deployment of diagnostic SQL views on top of audit_logs
    for real-time pipeline monitoring, exception tracing, and data quality metrics.
    """

    def __init__(self, spark: SparkSession, config_loader: ConfigLoader):
        self.spark = spark
        self.config = config_loader
        self.catalog = self.config.get_catalog()
        self.audit_schema = self.config.get_schema("audit")
        self.audit_table = f"{self.catalog}.{self.audit_schema}.audit_logs"

    def deploy_health_summary_view(self) -> None:
        """Deploys a view aggregating job executions, row mutations, and success rates."""
        view_name = f"{self.catalog}.{self.audit_schema}.vw_pipeline_health_summary"
        
        sql_query = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT 
            environment,
            pipeline_name,
            step_name,
            COUNT(*) AS total_executions,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
            ROUND(
                (SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2
            ) AS success_rate_pct,
            SUM(records_processed) AS total_records_processed,
            MAX(timestamp) AS last_execution_utc
        FROM {self.audit_table}
        GROUP BY environment, pipeline_name, step_name;
        """
        self.spark.sql(sql_query)

    def deploy_error_traceback_view(self) -> None:
        """Deploys a view dedicated to active pipeline errors and stack traces."""
        view_name = f"{self.catalog}.{self.audit_schema}.vw_active_error_tracebacks"
        
        sql_query = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT 
            timestamp AS failure_time_utc,
            environment,
            pipeline_name,
            step_name,
            error_message,
            metadata
        FROM {self.audit_table}
        WHERE status = 'FAILED'
        ORDER BY timestamp DESC;
        """
        self.spark.sql(sql_query)

    def deploy_all_views(self) -> None:
        """Deploys all telemetry views to Unity Catalog."""
        self.deploy_health_summary_view()
        self.deploy_error_traceback_view()
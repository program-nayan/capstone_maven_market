import time
from typing import Dict, Any, Callable
from pyspark.sql import SparkSession
from src.telemetry.logger import TelemetryLogger


class PerformanceBenchmark:
    """
    Engine for benchmarking query execution speed, throughput (records/sec),
    and cluster warm-up latency across Silver and Gold Delta layers.
    """

    def __init__(self, spark: SparkSession, environment: str = "dev"):
        self.spark = spark
        self.logger = TelemetryLogger(spark=spark, environment=environment)

    def measure_query_performance(
        self,
        query_name: str,
        sql_query: str
    ) -> Dict[str, Any]:
        """Executes a SQL query, measures latency, and logs execution benchmarks to audit table."""
        start_time = time.time()
        
        # Execute query
        df = self.spark.sql(sql_query)
        record_count = df.count()  # Force execution evaluation
        
        elapsed_time = round(time.time() - start_time, 4)
        records_per_second = round(record_count / elapsed_time, 2) if elapsed_time > 0 else record_count

        benchmark_metrics = {
            "execution_time_seconds": elapsed_time,
            "records_per_second": records_per_second,
            "query_type": "Databricks SQL / PySpark"
        }

        # Log metric result to audit logs
        self.logger.log_event(
            pipeline_name="performance_benchmark_suite",
            step_name=query_name,
            status="BENCHMARK",
            records_processed=record_count,
            additional_metadata=benchmark_metrics
        )

        return benchmark_metrics
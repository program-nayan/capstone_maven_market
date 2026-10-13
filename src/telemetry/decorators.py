import time
import functools
from typing import Callable, Any
from pyspark.sql import DataFrame
from src.telemetry.logger import TelemetryLogger

def log_execution(pipeline_name: str, step_name: str, capture_row_count: bool = False):
    """
    Decorator for PySpark transformation functions.
    Dynamically identifies environment and protects against streaming crashes.
    """
    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            spark = kwargs.get("spark", None)
            if not spark and args:
                for arg in args:
                    if hasattr(arg, "sql"):
                        spark = arg
                        break
                    elif hasattr(arg, "sparkSession"):
                        spark = arg.sparkSession
                        break

            if not spark:
                return func(*args, **kwargs)

            # Auto-detect environment from Spark runtime conf
            env = spark.conf.get("bundle.environment", "dev")
            catalog = spark.conf.get("catalog_name", "maven_market_uc")
            logger = TelemetryLogger(spark=spark, catalog=catalog, environment=env)

            start_time = time.perf_counter()
            logger.log_event(
                pipeline_name=pipeline_name,
                step_name=step_name,
                status="STARTED",
                records_processed=0
            )

            try:
                result = func(*args, **kwargs)
                
                records_count = 0
                if capture_row_count and isinstance(result, DataFrame):
                    if not result.isStreaming:
                        records_count = result.count()
                    else:
                        records_count = -1

                execution_time_sec = round(time.perf_counter() - start_time, 4)
                
                logger.log_event(
                    pipeline_name=pipeline_name,
                    step_name=step_name,
                    status="SUCCESS",
                    records_processed=records_count,
                    execution_time_seconds=execution_time_sec
                )
                return result

            except Exception as e:
                execution_time_sec = round(time.perf_counter() - start_time, 4)
                logger.log_exception(
                    pipeline_name=pipeline_name,
                    step_name=step_name,
                    exception=e,
                    execution_time_seconds=execution_time_sec
                )
                raise e

        return wrapper
    return decorator
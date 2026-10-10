import time
import functools
from typing import Callable, Any
from pyspark.sql import DataFrame
from src.telemetry.logger import TelemetryLogger

def log_execution(pipeline_name: str, step_name: str):
    """
    Decorator for PySpark transformation functions.
    Handles streaming DataFrames safely without triggering invalid .count() actions.
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
                # If no SparkSession is supplied, run the target function without logging
                return func(*args, **kwargs)

            logger = TelemetryLogger(spark=spark)
            start_time = time.time()
            
            logger.log_event(
                pipeline_name=pipeline_name,
                step_name=step_name,
                status="STARTED",
                records_processed=0
            )

            try:
                result = func(*args, **kwargs)
                
                records_count = 0
                if isinstance(result, DataFrame):
                    # Safely skip .count() on streaming DataFrames to prevent AnalysisException
                    if not result.isStreaming:
                        records_count = result.count()
                    else:
                        records_count = -1  # Indicates streaming query

                execution_time_sec = round(time.time() - start_time, 2)
                
                logger.log_event(
                    pipeline_name=pipeline_name,
                    step_name=step_name,
                    status="SUCCESS",
                    records_processed=records_count,
                    execution_time_seconds=execution_time_sec
                )
                return result

            except Exception as e:
                execution_time_sec = round(time.time() - start_time, 2)
                logger.log_exception(
                    pipeline_name=pipeline_name,
                    step_name=step_name,
                    exception=e,
                    execution_time_seconds=execution_time_sec
                )
                raise e

        return wrapper
    return decorator
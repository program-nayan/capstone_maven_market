import time
import functools
from typing import Callable, Any
from pyspark.sql import DataFrame
from src.telemetry.logger import TelemetryLogger


def log_execution(pipeline_name: str, step_name: str):
    """
    Decorator that wraps PySpark transformation functions.
    Automatically logs execution start, completion time, row counts, and uncaught exceptions.
    """
    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract spark session from arguments or kwargs
            spark = kwargs.get("spark", None)
            if not spark and args:
                # Fallback: check if first arg has a spark attribute or is a SparkSession
                for arg in args:
                    if hasattr(arg, "sql"):
                        spark = arg
                        break
                    elif hasattr(arg, "sparkSession"):
                        spark = arg.sparkSession
                        break

            if not spark:
                raise ValueError("Telemetry decorator requires a active SparkSession passed via args/kwargs.")

            logger = TelemetryLogger(spark=spark)
            start_time = time.time()
            
            # 1. Log Start
            logger.log_event(
                pipeline_name=pipeline_name,
                step_name=step_name,
                status="STARTED",
                records_processed=0
            )

            try:
                # Execute underlying PySpark step
                result = func(*args, **kwargs)
                
                # Count records if function returns a PySpark DataFrame
                records_count = 0
                if isinstance(result, DataFrame):
                    records_count = result.count()

                execution_time_sec = round(time.time() - start_time, 2)
                
                # 2. Log Success
                logger.log_event(
                    pipeline_name=pipeline_name,
                    step_name=step_name,
                    status="SUCCESS",
                    records_processed=records_count,
                    additional_metadata={"execution_time_seconds": execution_time_sec}
                )
                return result

            except Exception as e:
                execution_time_sec = round(time.time() - start_time, 2)
                # 3. Log Failure & Re-raise exception
                logger.log_exception(
                    pipeline_name=pipeline_name,
                    step_name=step_name,
                    exception=e,
                    additional_metadata={"execution_time_seconds": execution_time_sec}
                )
                raise e

        return wrapper
    return decorator
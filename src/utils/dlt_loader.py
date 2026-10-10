import os
import yaml
from pyspark.sql import SparkSession


def load_config(config_path: str = None) -> dict:
    """
    Loads and parses the YAML configuration file for Maven Market DLT pipelines.
    
    Order of precedence for locating config.yml:
    1. Explicit config_path argument passed to load_config()
    2. spark.conf.get("pipeline.config_path") set in DLT Pipeline Settings Configuration
    3. Workspace / local relative path fallbacks (config/config.yml)
    """
    spark = SparkSession.builder.getOrCreate()
    
    # 1. Check if path passed directly or set in Spark Conf
    if not config_path:
        try:
            config_path = spark.conf.get("pipeline.config_path")
        except Exception:
            config_path = None

    # 2. Candidate paths to search if not found in spark.conf
    candidate_paths = []
    if config_path:
        candidate_paths.append(config_path)

    # Common workspace and relative fallback paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(current_dir, "../../"))
    
    candidate_paths.extend([
        os.path.join(repo_root, "config", "config.yml"),
        "config/config.yml",
        "../config/config.yml",
        "/Workspace/Users/omrajeshkale@gmail.com/capstone_maven_market/config/config.yml"
    ])

    # 3. Locate first existing config file
    resolved_path = None
    for path in candidate_paths:
        if path and os.path.exists(path):
            resolved_path = path
            break

    if not resolved_path:
        raise FileNotFoundError(
            f"Could not locate 'config.yml'. Searched in candidate paths: {candidate_paths}. "
            "Ensure 'pipeline.config_path' is set in DLT Pipeline Settings Configuration JSON."
        )

    # 4. Load and parse YAML configuration
    with open(resolved_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


# Alias function name for backward compatibility if imported as load_yaml / load_config_file
load_config_file = load_config
import os
import sys
import warnings
from pathlib import Path
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Suppress PySpark UDF eval type inference warnings
warnings.filterwarnings("ignore", message="Cannot infer the eval type from type hints.*")

import shutil
from typing import Optional

# Ensure Java 17+ runtime is discovered across environments
def _find_java_home() -> Optional[Path]:
    if "JAVA_HOME" in os.environ and Path(os.environ["JAVA_HOME"]).exists():
        return Path(os.environ["JAVA_HOME"])

    # 1. Infer from `java` executable on PATH
    java_bin = shutil.which("java")
    if java_bin:
        resolved = Path(java_bin).resolve()
        candidate = resolved.parent.parent
        if (candidate / "bin" / "java").exists():
            return candidate

    # 2. Check user-local and system-wide JVM installations
    user_jvm_dir = Path.home() / ".local" / "share" / "jvm"
    candidates = [
        user_jvm_dir / "temurin-17-jre",
        Path("/usr/lib/jvm/default-java"),
        Path("/usr/lib/jvm/java-17-openjdk-amd64"),
    ]
    if user_jvm_dir.exists():
        candidates.extend(sorted(user_jvm_dir.glob("*17*"), reverse=True))

    for candidate in candidates:
        if candidate.exists() and (candidate / "bin" / "java").exists():
            return candidate

    return None


_discovered_java_home = _find_java_home()
if _discovered_java_home:
    if "JAVA_HOME" not in os.environ:
        os.environ["JAVA_HOME"] = str(_discovered_java_home)
    _java_bin = str(_discovered_java_home / "bin")
    _current_path = os.environ.get("PATH", "")
    if _java_bin not in _current_path.split(os.pathsep):
        os.environ["PATH"] = f"{_java_bin}{os.pathsep}{_current_path}"


def get_spark_session(app_name: str = "NCAA-Lacrosse-ETL") -> SparkSession:
    """Build and return an optimized local SparkSession with Delta Lake support."""
    builder = (
        SparkSession.builder
        .master(os.environ.get("LAXPXP_SPARK_MASTER", "local[*]"))
        .appName(app_name)
        # --- Memory: Utilize half of your 32 GiB RAM ---
        .config("spark.driver.memory", os.environ.get("LAXPXP_SPARK_DRIVER_MEMORY", "16g"))
        .config("spark.driver.maxResultSize", "4g")
        
        # --- CPU & Partitions: Match your 16 CPU threads ---
        .config("spark.default.parallelism", os.environ.get("LAXPXP_SPARK_DEFAULT_PARALLELISM", "16"))
        .config("spark.sql.shuffle.partitions", os.environ.get("LAXPXP_SPARK_SHUFFLE_PARTITIONS", "32"))
        
        # --- Adaptive Query Execution (AQE) ---
        # Automatically coalesces tiny partitions on small runs,
        # but dynamically splits or handles skew on large datasets
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")  # 128 MB target
        
        # --- PyArrow & Clean Logs ---
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.executorEnv.PYTHONWARNINGS", "ignore::UserWarning:pyspark.sql.udf")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.showConsoleProgress", "true")
        .config("spark.sql.debug.maxToStringFields", "200")
        
        # --- Delta Lake ---
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Incremental Silver replacements target contest_id data, not disk partitions.
        .config("spark.databricks.delta.replaceWhere.dataColumns.enabled", "true")
        .config("spark.databricks.delta.replaceWhere.constraintCheck.enabled", "true")
    )
    event_log_dir = os.environ.get("LAXPXP_SPARK_EVENT_LOG_DIR")
    if event_log_dir:
        Path(event_log_dir).mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir", Path(event_log_dir).resolve().as_uri()
        )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

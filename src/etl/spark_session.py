import os
import shutil
import warnings
from pathlib import Path
from typing import Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Suppress PySpark UDF eval type inference warnings
warnings.filterwarnings("ignore", message="Cannot infer the eval type from type hints.*")


def _java_major(name: str) -> int:
    """Parse the major version from a JVM directory name such as `java-17-openjdk-amd64`."""
    token = name.split("-")
    return int(token[1]) if len(token) > 1 and token[1].isdigit() else 0


# Ensure a Java 17+ runtime is discovered across environments
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

    # 2. Fall back to standard system JVM installation roots. Spark 4.x requires
    #    Java 17+, so a versioned install wins over the distribution's default.
    jvm_root = Path("/usr/lib/jvm")
    if jvm_root.is_dir():
        installs = [p for p in jvm_root.iterdir() if (p / "bin" / "java").exists()]
        supported = sorted(p for p in installs if _java_major(p.name) >= 17)
        fallback = supported or [p for p in installs if p.name == "default-java"]
        if fallback:
            return fallback[0]

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
        # Modest defaults so the bundled demo runs on a typical laptop; the
        # benchmark and README raise them through the LAXPXP_SPARK_* variables.
        .config("spark.driver.memory", os.environ.get("LAXPXP_SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.driver.maxResultSize", "4g")

        .config("spark.default.parallelism", os.environ.get("LAXPXP_SPARK_DEFAULT_PARALLELISM", "4"))
        .config("spark.sql.shuffle.partitions", os.environ.get("LAXPXP_SPARK_SHUFFLE_PARTITIONS", "32"))

        # Adaptive Query Execution (AQE): coalesces small partitions, splits
        # skewed ones, and targets a 128 MiB partition size.
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

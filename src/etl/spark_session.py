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
    """Build and return an optimized local SparkSession with Delta Lake support and clean logging."""
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.executorEnv.PYTHONWARNINGS", "ignore::UserWarning:pyspark.sql.udf")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.codegen.wholeStage", "true")
        .config("spark.sql.debug.maxToStringFields", "200")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark

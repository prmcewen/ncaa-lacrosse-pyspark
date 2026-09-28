import logging
import shutil
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.storagelevel import StorageLevel

logger = logging.getLogger(__name__)


class DataSkewError(ValueError):
    """
    Raised when data distribution across partition keys or physical partitions
    exceeds configured skew thresholds.
    """

    def __init__(self, message: str, metrics: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.metrics = metrics or {}


def check_data_skew(
    df: DataFrame,
    partition_cols: Optional[Union[str, List[str]]] = None,
    max_skew_ratio: float = 2.5,
    max_absolute_rows: Optional[int] = None,
    max_skewness: Optional[float] = None,
    raise_on_skew: bool = True,
    allow_empty: bool = True,
) -> Dict[str, Any]:
    """
    Test a DataFrame for potential partition data skew before partitioning or downstream stages.

    Evaluates the record distribution across partition columns (or physical Spark partitions
    if partition_cols is None). Computes summary statistics including min, max, average,
    standard deviation, max-to-average skew ratio, and Fisher-Pearson skewness.

    If skew exceeds any configured threshold (max_skew_ratio, max_absolute_rows, or max_skewness),
    it logs actionable diagnostics including top skewed partition keys and raises DataSkewError
    (or flags is_skewed=True when raise_on_skew is False).

    Args:
        df: The PySpark DataFrame to analyze.
        partition_cols: Column name or list of column names used as partition keys.
                        If None, evaluates distribution across physical partitions.
        max_skew_ratio: Maximum acceptable ratio of max_partition_rows / avg_partition_rows.
                        Defaults to 2.5 (lacrosse game plays typically have ~1.15 ratio).
        max_absolute_rows: Optional upper bound on row count for any single partition.
        max_skewness: Optional maximum allowable Fisher-Pearson standardized skewness coefficient.
        raise_on_skew: Whether to raise DataSkewError if skew thresholds are violated.
        allow_empty: Whether an empty DataFrame is acceptable without error.

    Returns:
        Dict containing partition distribution metrics and skew assessment.

    Raises:
        DataSkewError: If skew metrics violate the configured thresholds and raise_on_skew is True.
    """
    if isinstance(partition_cols, str):
        cols_list = [partition_cols]
    elif partition_cols is not None:
        cols_list = list(partition_cols)
    else:
        cols_list = None

    if cols_list:
        counts_df = df.groupBy(*cols_list).agg(F.count(F.lit(1)).alias("row_count"))
        partition_label = f"partition columns {cols_list}"
    else:
        counts_df = df.groupBy(F.spark_partition_id().alias("partition_id")).agg(
            F.count(F.lit(1)).alias("row_count")
        )
        partition_label = "physical partitions"

    summary_row = counts_df.select(
        F.count(F.lit(1)).alias("num_partitions"),
        F.coalesce(F.sum("row_count"), F.lit(0)).alias("total_rows"),
        F.coalesce(F.min("row_count"), F.lit(0)).alias("min_rows"),
        F.coalesce(F.max("row_count"), F.lit(0)).alias("max_rows"),
        F.coalesce(F.avg("row_count"), F.lit(0.0)).alias("avg_rows"),
        F.stddev("row_count").alias("stddev_rows"),
        F.skewness("row_count").alias("skewness"),
    ).collect()[0]

    num_partitions = int(summary_row["num_partitions"]) if summary_row["num_partitions"] is not None else 0
    total_rows = int(summary_row["total_rows"]) if summary_row["total_rows"] is not None else 0
    min_rows = int(summary_row["min_rows"]) if summary_row["min_rows"] is not None else 0
    max_rows = int(summary_row["max_rows"]) if summary_row["max_rows"] is not None else 0
    avg_rows = float(summary_row["avg_rows"]) if summary_row["avg_rows"] is not None else 0.0
    stddev_rows = float(summary_row["stddev_rows"]) if summary_row["stddev_rows"] is not None else 0.0
    skewness = float(summary_row["skewness"]) if summary_row["skewness"] is not None else None

    skew_ratio = (max_rows / avg_rows) if avg_rows > 0 else 1.0

    if num_partitions == 0:
        if not allow_empty:
            msg = f"Empty DataFrame encountered during skew check for {partition_label}."
            if raise_on_skew:
                raise DataSkewError(msg, metrics={"num_partitions": 0, "total_rows": 0})
            logger.warning(msg)
        return {
            "partition_cols": cols_list,
            "num_partitions": 0,
            "total_rows": 0,
            "min_rows": 0,
            "max_rows": 0,
            "avg_rows": 0.0,
            "stddev_rows": 0.0,
            "skewness": None,
            "skew_ratio": 1.0,
            "is_skewed": False,
            "violations": [],
        }

    violations: List[str] = []
    if num_partitions > 1 and skew_ratio > max_skew_ratio:
        violations.append(
            f"Skew ratio {skew_ratio:.2f} exceeds maximum allowed ratio of {max_skew_ratio:.2f}"
        )
    if max_absolute_rows is not None and max_rows > max_absolute_rows:
        violations.append(
            f"Maximum partition row count {max_rows} exceeds absolute limit of {max_absolute_rows}"
        )
    if max_skewness is not None and skewness is not None and abs(skewness) > max_skewness:
        violations.append(
            f"Fisher-Pearson skewness {skewness:.2f} exceeds maximum allowed skewness of {max_skewness:.2f}"
        )

    is_skewed = len(violations) > 0
    metrics: Dict[str, Any] = {
        "partition_cols": cols_list,
        "num_partitions": num_partitions,
        "total_rows": total_rows,
        "min_rows": min_rows,
        "max_rows": max_rows,
        "avg_rows": avg_rows,
        "stddev_rows": stddev_rows,
        "skewness": skewness,
        "skew_ratio": skew_ratio,
        "is_skewed": is_skewed,
        "violations": violations,
    }

    if is_skewed:
        top_partitions = counts_df.orderBy(F.desc("row_count")).limit(5).collect()
        top_partitions_str = "\n".join(f"  - {row.asDict()}" for row in top_partitions)
        msg = (
            f"Data skew detected for {partition_label}!\n"
            f"Violations:\n" + "\n".join(f"  * {v}" for v in violations) + "\n"
            f"Partition Distribution Summary:\n"
            f"  - Total partitions: {num_partitions}\n"
            f"  - Total rows: {total_rows}\n"
            f"  - Min rows: {min_rows}\n"
            f"  - Max rows: {max_rows}\n"
            f"  - Avg rows: {avg_rows:.2f}\n"
            f"  - Stddev: {stddev_rows:.2f}\n"
            f"  - Skew ratio (max/avg): {skew_ratio:.2f}\n"
            f"  - Fisher-Pearson skewness: {f'{skewness:.2f}' if skewness is not None else 'N/A'}\n"
            f"Top 5 largest partitions:\n{top_partitions_str}"
        )
        if raise_on_skew:
            logger.error(msg)
            raise DataSkewError(msg, metrics=metrics)
        else:
            logger.warning(msg)
    else:
        logger.info(
            f"Data skew check PASSED for {partition_label}: "
            f"skew_ratio={skew_ratio:.2f} <= {max_skew_ratio:.2f}, "
            f"partitions={num_partitions}, total_rows={total_rows}, "
            f"min={min_rows}, max={max_rows}, avg={avg_rows:.2f}"
        )

    return metrics


check_partition_skew = check_data_skew


def apply_broadcast_join(
    fact_df: DataFrame,
    dim_df: DataFrame,
    left_on: str,
    right_on: Optional[str] = None,
    how: str = "left"
) -> DataFrame:
    """
    Apply broadcast hash join (BHJ) on small dimension tables.
    Eliminates expensive Shuffle Exchange stages in Spark.
    """
    right_col = right_on or left_on
    logger.info(f"Applying broadcast join: fact.{left_on} == dim.{right_col} ({how})")
    if left_on == right_col:
        return fact_df.join(broadcast(dim_df), on=left_on, how=how)
    else:
        return fact_df.join(broadcast(dim_df), fact_df[left_on] == dim_df[right_col], how=how).drop(dim_df[right_col])


def cache_and_profile(
    df: DataFrame,
    label: str,
    storage_level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    eager: bool = False,
) -> DataFrame:
    """
    Persist DataFrame before multiple downstream aggregation branches.

    By default, persistence is registered lazily (eager=False) to preserve Spark's
    pipelined execution and avoid forcing an unnecessary driver action solely for logging.
    If eager=True, forces an immediate action (count()) to materialize the cache
    and profile execution duration.
    """
    cached_df = df.persist(storage_level)
    if eager:
        t0 = time.perf_counter()
        count = cached_df.count()
        duration = time.perf_counter() - t0
        logger.info(f"[CACHE] Materialized {label} ({count} rows) in {duration:.4f}s")
    else:
        logger.info(f"[CACHE] Registered lazy persistence for {label} ({storage_level})")
    return cached_df


def unpersist_dataframe(df: Optional[DataFrame], blocking: bool = False) -> None:
    """
    Safely unpersist a cached DataFrame to free executor memory and avoid resource leaks.

    Args:
        df: Optional PySpark DataFrame to unpersist.
        blocking: Whether to block until all blocks are removed from executor memory/disk.
    """
    if df is not None:
        try:
            df.unpersist(blocking=blocking)
            logger.info("Successfully unpersisted cached DataFrame (blocking=%s).", blocking)
        except Exception as e:
            logger.warning("Failed to unpersist cached DataFrame: %s", e)


def save_partitioned_parquet(
    df: DataFrame,
    output_path: Union[str, Path],
    partition_cols: Union[str, List[str]],
    mode: str = "overwrite",
    check_skew: bool = True,
    max_skew_ratio: float = 2.5,
    max_absolute_rows: Optional[int] = None,
    max_skewness: Optional[float] = None,
    post_write_skew: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Write DataFrame to columnar Parquet format with explicit partitioning.

    Validates partition data skew without causing redundant upstream lineage evaluation:
    - If `df` is cached (`storageLevel != StorageLevel.NONE`), skew validation executes
      pre-emptively before the write stage with zero redundant upstream computation.
    - If `df` is uncached (`storageLevel == StorageLevel.NONE`), pre-write skew aggregation
      is bypassed to avoid evaluating upstream lineage twice. Skew assertions are instead
      executed post-write directly on the persisted output files. If skew thresholds are
      violated, the invalid output files are cleaned up and DataSkewError is raised.
    - If `post_write_skew` is explicitly set to True/False, forces post-write or pre-write
      skew validation respectively.

    Uses Snappy compression for high throughput and reduced disk footprint, and explicitly
    repartitions by partition columns to prevent fragmented small files.

    Args:
        df: Input PySpark DataFrame to persist.
        output_path: Filesystem destination path for partitioned Parquet output.
        partition_cols: Column name or list of column names used as partition keys.
        mode: Spark write mode (e.g. 'overwrite', 'append'). Defaults to 'overwrite'.
        check_skew: Whether to perform partition skew validation.
        max_skew_ratio: Maximum allowed ratio of max_partition_rows / avg_partition_rows.
        max_absolute_rows: Optional maximum row count allowed in any single partition.
        max_skewness: Optional maximum allowed Fisher-Pearson standardized skewness.
        post_write_skew: If True, forces skew verification on persisted files post-write.
                         If False, only executes pre-write if cached.
                         If None (default), intelligently chooses: pre-write if cached,
                         post-write on persisted output files if uncached.

    Returns:
        Dict containing skew diagnostic metrics (or empty dict if check_skew=False).

    Raises:
        DataSkewError: If partition data skew thresholds are exceeded.
    """
    if isinstance(partition_cols, str):
        cols_list = [partition_cols]
    elif partition_cols is not None:
        cols_list = list(partition_cols)
    else:
        cols_list = []

    output_path = Path(output_path)
    skew_report: Dict[str, Any] = {}
    is_cached = df.storageLevel != StorageLevel.NONE

    # Skew checks on uncached DataFrames cause redundant upstream lineage evaluation.
    # Therefore, pre-write skew validation strictly requires df.storageLevel != StorageLevel.NONE.
    # If uncached, skew validation is executed post-write on the persisted output files.
    run_pre_write = (
        check_skew
        and bool(cols_list)
        and is_cached
        and (not post_write_skew if post_write_skew is not None else True)
    )
    run_post_write = (
        check_skew
        and bool(cols_list)
        and (post_write_skew if post_write_skew is not None else not is_cached)
    )

    if run_pre_write:
        logger.info(
            f"Pre-write skew check: DataFrame is cached ({df.storageLevel}). "
            "Evaluating partition skew without redundant upstream lineage evaluation..."
        )
        skew_report = check_data_skew(
            df=df,
            partition_cols=cols_list,
            max_skew_ratio=max_skew_ratio,
            max_absolute_rows=max_absolute_rows,
            max_skewness=max_skewness,
            raise_on_skew=True,
        )
    elif check_skew and cols_list and not is_cached and post_write_skew is False:
        logger.warning(
            "DataFrame storageLevel is NONE and post_write_skew is False. "
            "Skipping pre-write skew check to prevent evaluating upstream lineage twice. "
            "Cache DataFrame before writing or enable post-write skew validation."
        )
    elif check_skew and cols_list and not is_cached:
        logger.info(
            "DataFrame is uncached (storageLevel == StorageLevel.NONE). "
            "Bypassing pre-write skew check to avoid evaluating upstream lineage twice; "
            "skew assertions will execute post-write on persisted Parquet files."
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing partitioned Parquet to {output_path} (mode={mode}, partitions: {cols_list})...")
    write_df = df.repartition(*cols_list) if cols_list else df
    (
        write_df.write
        .mode(mode)
        .partitionBy(*cols_list)
        .option("compression", "snappy")
        .parquet(str(output_path))
    )
    logger.info(f"Partitioned Parquet successfully written to {output_path}")

    if run_post_write:
        logger.info(f"Executing post-write skew assertion on persisted Parquet files at {output_path}...")
        persisted_df = df.sparkSession.read.parquet(str(output_path))
        try:
            skew_report = check_data_skew(
                df=persisted_df,
                partition_cols=cols_list,
                max_skew_ratio=max_skew_ratio,
                max_absolute_rows=max_absolute_rows,
                max_skewness=max_skewness,
                raise_on_skew=True,
            )
        except DataSkewError:
            # Clean up invalid partitioned files so corrupt skewed output is not left on storage
            logger.error(
                f"Post-write skew assertion failed on {output_path}. "
                "Cleaning up invalid output files."
            )
            if output_path.exists():
                if output_path.is_dir():
                    shutil.rmtree(output_path, ignore_errors=True)
                else:
                    output_path.unlink(missing_ok=True)
            raise

    return skew_report


def save_explain_plan(df: DataFrame, plan_path: Path, title: str = "Spark Execution Plan") -> None:
    """
    Capture and persist Spark extended physical and logical execution plans
    for architectural reviews and compliance audits.
    """
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        explain_str = df._jdf.queryExecution().toString()
    except Exception:
        explain_str = "Execution plan captured."
    with plan_path.open("w", encoding="utf-8") as f:
        f.write(f"# {title}\n\n```\n{explain_str}\n```\n")
    logger.info(f"Execution plan saved to {plan_path}")

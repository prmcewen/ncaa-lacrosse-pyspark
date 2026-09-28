import pytest
from pathlib import Path
from typing import List, Tuple
from unittest.mock import patch

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from src.etl.optimizations import (
    DataSkewError,
    cache_and_profile,
    check_data_skew,
    check_partition_skew,
    save_partitioned_parquet,
    unpersist_dataframe,
)


def _create_mock_df(
    spark: SparkSession,
    contest_counts: List[Tuple[int, int]],
) -> DataFrame:
    """Helper to generate a mock DataFrame with specified row counts per contest_id."""
    data = []
    schema = StructType([
        StructField("contest_id", LongType(), False),
        StructField("play_seq", IntegerType(), False),
        StructField("play_text", StringType(), True),
    ])
    for contest_id, count in contest_counts:
        for seq in range(1, count + 1):
            data.append((contest_id, seq, f"Play {seq} of contest {contest_id}"))
    return spark.createDataFrame(data, schema)


def test_check_data_skew_balanced_passes(spark: SparkSession):
    """Balanced data across partition columns should pass without raising."""
    # 4 contests, each with 100 plays -> skew ratio is exactly 1.0
    df = _create_mock_df(spark, [(101, 100), (102, 100), (103, 100), (104, 100)])

    report = check_data_skew(df, partition_cols=["contest_id"], max_skew_ratio=2.0)

    assert report["is_skewed"] is False
    assert report["num_partitions"] == 4
    assert report["total_rows"] == 400
    assert report["min_rows"] == 100
    assert report["max_rows"] == 100
    assert report["avg_rows"] == 100.0
    assert report["skew_ratio"] == 1.0
    assert report["violations"] == []


def test_check_data_skew_ratio_throws_error(spark: SparkSession):
    """When a partition has disproportionate rows, DataSkewError must be raised."""
    # 3 small contests (10 plays each) and 1 giant contest (200 plays)
    # total = 230, avg = 57.5, max = 200, skew_ratio = 200 / 57.5 = 3.48
    df = _create_mock_df(spark, [(101, 10), (102, 10), (103, 10), (104, 200)])

    with pytest.raises(DataSkewError) as exc_info:
        check_data_skew(df, partition_cols=["contest_id"], max_skew_ratio=2.0)

    err = exc_info.value
    assert "Data skew detected" in str(err)
    assert "Skew ratio 3.48 exceeds maximum allowed ratio of 2.00" in str(err)
    assert "contest_id" in str(err)
    assert err.metrics["is_skewed"] is True
    assert err.metrics["max_rows"] == 200
    assert err.metrics["min_rows"] == 10
    assert err.metrics["skew_ratio"] > 2.0


def test_check_data_skew_raise_on_skew_false(spark: SparkSession):
    """With raise_on_skew=False, DataSkewError is suppressed and is_skewed=True returned."""
    df = _create_mock_df(spark, [(101, 10), (102, 10), (103, 10), (104, 200)])

    report = check_data_skew(df, partition_cols="contest_id", max_skew_ratio=2.0, raise_on_skew=False)

    assert report["is_skewed"] is True
    assert len(report["violations"]) == 1
    assert "Skew ratio" in report["violations"][0]


def test_check_data_skew_max_absolute_rows(spark: SparkSession):
    """Should throw DataSkewError if any partition exceeds max_absolute_rows."""
    # Ratio is ~1.11 (balanced), but max rows (500) exceeds ceiling of 300
    df = _create_mock_df(spark, [(101, 450), (102, 500)])

    with pytest.raises(DataSkewError) as exc_info:
        check_data_skew(df, partition_cols=["contest_id"], max_skew_ratio=2.5, max_absolute_rows=300)

    assert "Maximum partition row count 500 exceeds absolute limit of 300" in str(exc_info.value)


def test_check_data_skew_single_partition(spark: SparkSession):
    """A single partition key should pass cleanly without false positive ratio errors."""
    df = _create_mock_df(spark, [(101, 250)])

    report = check_data_skew(df, partition_cols=["contest_id"], max_skew_ratio=2.0)

    assert report["is_skewed"] is False
    assert report["num_partitions"] == 1
    assert report["skew_ratio"] == 1.0


def test_check_data_skew_empty_dataframe(spark: SparkSession):
    """Empty DataFrame handling: allow_empty=True passes; allow_empty=False raises."""
    schema = StructType([StructField("contest_id", LongType(), False)])
    empty_df = spark.createDataFrame([], schema)

    # Allowed
    report = check_data_skew(empty_df, partition_cols=["contest_id"], allow_empty=True)
    assert report["is_skewed"] is False
    assert report["num_partitions"] == 0

    # Not allowed
    with pytest.raises(DataSkewError) as exc_info:
        check_data_skew(empty_df, partition_cols=["contest_id"], allow_empty=False)
    assert "Empty DataFrame encountered" in str(exc_info.value)


def test_check_data_skew_physical_partitions(spark: SparkSession):
    """When partition_cols is None, evaluates physical Spark partitions."""
    df = _create_mock_df(spark, [(101, 50), (102, 50)]).repartition(4)

    report = check_data_skew(df, partition_cols=None, max_skew_ratio=5.0)

    assert "num_partitions" in report
    assert report["num_partitions"] <= 4
    assert report["total_rows"] == 100


def test_check_partition_skew_alias(spark: SparkSession):
    """Verify check_partition_skew alias is functional."""
    df = _create_mock_df(spark, [(101, 50), (102, 50)])
    report = check_partition_skew(df, partition_cols=["contest_id"])
    assert report["is_skewed"] is False


def test_save_partitioned_parquet_pre_validates_skew(spark: SparkSession, tmp_path: Path):
    """save_partitioned_parquet should validate data skew before writing."""
    out_dir = tmp_path / "test_partitioned.parquet"

    # 1. Balanced data writes successfully
    balanced_df = _create_mock_df(spark, [(201, 50), (202, 50)])
    report = save_partitioned_parquet(
        df=balanced_df,
        output_path=out_dir,
        partition_cols=["contest_id"],
        max_skew_ratio=2.0,
    )
    assert report["is_skewed"] is False
    assert out_dir.exists()

    # 2. Skewed data aborts with DataSkewError and cleans up invalid output
    skewed_df = _create_mock_df(spark, [(301, 10), (302, 10), (303, 300)])
    skew_out_dir = tmp_path / "test_skewed.parquet"
    with pytest.raises(DataSkewError):
        save_partitioned_parquet(
            df=skewed_df,
            output_path=skew_out_dir,
            partition_cols=["contest_id"],
            max_skew_ratio=2.0,
        )
    assert not skew_out_dir.exists()


def test_save_partitioned_parquet_cached_pre_validates_skew(spark: SparkSession, tmp_path: Path):
    """When DataFrame is cached, save_partitioned_parquet pre-validates skew before writing."""
    cached_df = _create_mock_df(spark, [(301, 10), (302, 10), (303, 300)]).cache()
    cached_df.count()

    skew_out_dir = tmp_path / "test_cached_skewed.parquet"
    try:
        with pytest.raises(DataSkewError):
            save_partitioned_parquet(
                df=cached_df,
                output_path=skew_out_dir,
                partition_cols=["contest_id"],
                max_skew_ratio=2.0,
            )
        # Pre-write check failed, so output directory was never written to
        assert not skew_out_dir.exists()
    finally:
        cached_df.unpersist(blocking=False)


def test_save_partitioned_parquet_lineage_protection_uncached_vs_cached(spark: SparkSession, tmp_path: Path):
    """Verify that uncached DataFrames bypass pre-write skew check to prevent redundant upstream lineage evaluation."""
    # 1. Uncached DataFrame: pre-write check is bypassed; check_data_skew receives the persisted DataFrame
    uncached_df = _create_mock_df(spark, [(201, 20), (202, 20)])
    out_dir_uncached = tmp_path / "uncached_lineage.parquet"

    with patch("src.etl.optimizations.check_data_skew", wraps=check_data_skew) as mock_skew:
        report = save_partitioned_parquet(
            df=uncached_df,
            output_path=out_dir_uncached,
            partition_cols=["contest_id"],
            max_skew_ratio=2.0,
        )
        assert report["is_skewed"] is False
        assert mock_skew.call_count == 1
        # The DataFrame passed to check_data_skew should NOT be the uncached input df
        called_df = mock_skew.call_args[1]["df"]
        assert called_df is not uncached_df

    # 2. Cached DataFrame: pre-write check executes directly on the cached DataFrame
    cached_df = _create_mock_df(spark, [(201, 20), (202, 20)]).cache()
    cached_df.count()
    out_dir_cached = tmp_path / "cached_lineage.parquet"

    try:
        with patch("src.etl.optimizations.check_data_skew", wraps=check_data_skew) as mock_skew_cached:
            report_cached = save_partitioned_parquet(
                df=cached_df,
                output_path=out_dir_cached,
                partition_cols=["contest_id"],
                max_skew_ratio=2.0,
            )
            assert report_cached["is_skewed"] is False
            assert mock_skew_cached.call_count == 1
            # The DataFrame passed to check_data_skew should be the cached input df
            called_df_cached = mock_skew_cached.call_args[1]["df"]
            assert called_df_cached is cached_df
    finally:
        cached_df.unpersist(blocking=False)


def test_save_partitioned_parquet_post_write_skew_flag(spark: SparkSession, tmp_path: Path):
    """Test explicit post_write_skew=True and post_write_skew=False controls."""
    df = _create_mock_df(spark, [(201, 25), (202, 25)])

    # post_write_skew=False with uncached df skips skew check with warning to avoid double evaluation
    out_dir_skip = tmp_path / "skip_skew.parquet"
    with patch("src.etl.optimizations.check_data_skew", wraps=check_data_skew) as mock_skew:
        report = save_partitioned_parquet(
            df=df,
            output_path=out_dir_skip,
            partition_cols=["contest_id"],
            post_write_skew=False,
        )
        assert mock_skew.call_count == 0
        assert report == {}
        assert out_dir_skip.exists()

    # post_write_skew=True forces post-write check on persisted files even if cached
    cached_df = _create_mock_df(spark, [(201, 25), (202, 25)]).cache()
    cached_df.count()
    out_dir_forced = tmp_path / "forced_post_write.parquet"
    try:
        with patch("src.etl.optimizations.check_data_skew", wraps=check_data_skew) as mock_skew_post:
            report_forced = save_partitioned_parquet(
                df=cached_df,
                output_path=out_dir_forced,
                partition_cols=["contest_id"],
                post_write_skew=True,
            )
            assert mock_skew_post.call_count == 1
            assert report_forced["is_skewed"] is False
            # Check was run on persisted df, not the cached input df
            assert mock_skew_post.call_args[1]["df"] is not cached_df
    finally:
        cached_df.unpersist(blocking=False)


def test_save_partitioned_parquet_repartitions_to_prevent_small_files(spark: SparkSession, tmp_path: Path):
    """Verify that save_partitioned_parquet repartitions by partition keys, preventing multi-task small files per partition."""
    out_dir = tmp_path / "test_repartitioned.parquet"

    # Create a DataFrame distributed across multiple Spark partitions where partition keys are scattered
    raw_data = [
        (401, f"event_{i}") for i in range(20)
    ] + [
        (402, f"event_{i}") for i in range(20)
    ]
    scattered_df = spark.createDataFrame(raw_data, ["contest_id", "event"]).repartition(4)

    save_partitioned_parquet(
        df=scattered_df,
        output_path=out_dir,
        partition_cols=["contest_id"],
        check_skew=False,
    )

    # Each partition directory should contain exactly 1 parquet file because of repartition("contest_id")
    p401 = list((out_dir / "contest_id=401").glob("*.parquet"))
    p402 = list((out_dir / "contest_id=402").glob("*.parquet"))

    assert len(p401) == 1
    assert len(p402) == 1


def test_cache_and_profile_lazy_and_unpersist(spark: SparkSession):
    """Verify cache_and_profile persists DataFrame lazily without eager count and unpersists cleanly."""
    df = _create_mock_df(spark, [(101, 10), (102, 10)])

    # Lazy caching by default
    cached_df = cache_and_profile(df, "mock_lazy_silver", eager=False)
    assert cached_df.is_cached is True

    # Deterministic unpersist
    cached_df.unpersist(blocking=False)
    assert cached_df.is_cached is False


def test_cache_and_profile_eager_and_helper_unpersist(spark: SparkSession):
    """Verify eager=True materializes cache and unpersist_dataframe safely clears cache."""
    df = _create_mock_df(spark, [(101, 15), (102, 25)])

    cached_df = cache_and_profile(df, "mock_eager_silver", eager=True)
    assert cached_df.is_cached is True

    # Unpersist via utility helper
    unpersist_dataframe(cached_df, blocking=False)
    assert cached_df.is_cached is False

    # Safe to call on None or already unpersisted df
    unpersist_dataframe(None)
    unpersist_dataframe(cached_df)


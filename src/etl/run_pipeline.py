import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.db.gold_storage import publish_gold_tables
from src.etl.gold_incremental import build_gold_tables
from src.etl.optimizations import (
    cache_and_profile,
    check_data_skew,
    save_explain_plan,
    unpersist_dataframe,
)
from src.etl.spark_session import get_spark_session
from src.etl.transform import (
    GOLD_DIR,
    SILVER_DIR,
    generate_gold_tables,
    extract_dimensions_from_silver_contests,
    get_latest_valid_snapshots,
    get_pending_snapshots,
    process_bronze_to_silver,
    process_bronze_to_silver_contests,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PipelineRunner")


def _write_silver_partitions(df: DataFrame, output: Path, pending_snapshots, full_refresh: bool) -> None:
    """Replace only changed contest partitions, preferring Delta Lake."""
    output.mkdir(parents=True, exist_ok=True)
    try:
        writer = df.repartition("contest_id").write.format("delta").mode("overwrite").partitionBy("contest_id")
        if full_refresh or not (output / "_delta_log").exists():
            writer.option("overwriteSchema", "true").save(str(output))
        else:
            pending_cids = ",".join(str(s[0]) for s in pending_snapshots)
            writer.option("replaceWhere", f"contest_id in ({pending_cids})").save(str(output))
    except Exception as e:
        logger.warning("Delta write failed for %s (%s); falling back to partitioned Parquet.", output, e)
        overwrite_mode = "static" if full_refresh or not any(output.glob("contest_id=*/*.parquet")) else "dynamic"
        df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", overwrite_mode)
        df.repartition("contest_id").write.mode("overwrite").partitionBy("contest_id").parquet(str(output))


def _read_silver(spark, output: Path) -> DataFrame:
    if (output / "_delta_log").exists():
        return spark.read.format("delta").load(str(output))
    return spark.read.parquet(str(output))


def _selected_silver_contests(spark, contests: DataFrame, snapshots) -> DataFrame:
    """Use only the manifest-selected versions and reject incomplete metadata."""
    expected = spark.createDataFrame(
        [(int(cid), ts) for cid, ts, _ in snapshots],
        "contest_id long, ingest_timestamp string",
    )
    selected = contests.join(F.broadcast(expected), ["contest_id", "ingest_timestamp"], "inner")
    if selected.count() != len(snapshots):
        raise ValueError("Silver contests do not contain exactly one row per selected snapshot")
    invalid = selected.filter(
        F.col("teams").isNull()
        | (F.size("teams") == 0)
        | F.exists("teams", lambda team: team["team_id"].isNull())
    )
    if invalid.limit(1).count():
        raise ValueError("Silver contests contain missing team metadata or team IDs")
    return selected


def run_pipeline(full_refresh: bool = False) -> None:
    logger.info("Initializing PySpark session...")
    spark = get_spark_session("NCAA-Lacrosse-ETL-Pipeline")
    cached_silver: Optional[DataFrame] = None
    dim_teams: Optional[DataFrame] = None
    dim_contests: Optional[DataFrame] = None

    try:
        snapshots = get_latest_valid_snapshots()
        if not snapshots:
            logger.warning("No valid snapshots found in manifest! Run ingestion first.")
            return

        logger.info(f"Discovered {len(snapshots)} contest snapshot(s) in manifest.")

        silver_output = SILVER_DIR / "silver_plays.parquet"
        contests_output = SILVER_DIR / "silver_contests.parquet"

        pending_plays = get_pending_snapshots(
            spark, snapshots, silver_output_path=silver_output, full_refresh=full_refresh,
        )
        pending_contests = get_pending_snapshots(
            spark, snapshots, silver_output_path=contests_output, full_refresh=full_refresh,
        )

        if pending_plays:
            logger.info("Processing %s changed play snapshot(s)...", len(pending_plays))
            plays = process_bronze_to_silver(spark, pending_plays)
            _write_silver_partitions(plays, silver_output, pending_plays, full_refresh)

        if pending_contests:
            logger.info("Processing %s changed contest metadata snapshot(s)...", len(pending_contests))
            contests = process_bronze_to_silver_contests(spark, pending_contests)
            _write_silver_partitions(contests, contests_output, pending_contests, full_refresh)

        # Each Silver table has its own freshness check. A retry after a failed
        # Gold publication reads committed Silver without reparsing Bronze.
        silver_contests = _selected_silver_contests(
            spark, _read_silver(spark, contests_output), snapshots,
        )
        dim_teams, dim_contests = extract_dimensions_from_silver_contests(silver_contests)

        if dim_teams is not None:
            dim_teams = dim_teams.cache()
            dim_teams.count()
        if dim_contests is not None:
            dim_contests = dim_contests.cache()
            dim_contests.count()

        # Optimization: Read partitioned Parquet / Delta & cache before downstream gold branching
        silver_df = _read_silver(spark, silver_output)
        selected = spark.createDataFrame(
            [(int(cid), ts) for cid, ts, _ in snapshots],
            "contest_id long, ingest_timestamp string",
        )
        silver_df = silver_df.join(F.broadcast(selected), ["contest_id", "ingest_timestamp"], "inner")
        cached_silver = cache_and_profile(silver_df, "silver_plays")

        # Model the contest-keyed shuffle used by downstream windows and writes.
        logger.info("Checking Silver contest-keyed Spark task skew...")
        check_data_skew(
            cached_silver,
            partition_cols=["contest_id"],
            max_skew_ratio=2.5,
            max_absolute_rows=2000,
            raise_on_skew=True,
        )

        logger.info("Selecting changed Silver rows for Gold...")
        gold_tables = build_gold_tables(
            spark, cached_silver, dim_teams, dim_contests,
            GOLD_DIR, full_refresh, generate_gold_tables,
        )
        if gold_tables is None:
            logger.info("Gold is current; no publication needed.")
            return

        # Validate the complete aggregate before publishing a new generation.
        logger.info("Checking team-keyed aggregate Spark task skew...")
        check_data_skew(
            gold_tables["agg_team_game_stats"],
            partition_cols=["team_id"],
            max_skew_ratio=3.5,
            raise_on_skew=True,
        )
        enriched_facts = gold_tables["fact_plays"]

        published = publish_gold_tables(gold_tables.writes, GOLD_DIR)
        logger.info("Published complete Gold generation: %s", published)

        # Save Explain Plan for Portfolio & Audit Documentation
        docs_dir = Path(__file__).resolve().parents[2] / "docs"
        save_explain_plan(
            enriched_facts,
            docs_dir / "spark_execution_plan.md",
            title="PySpark Physical & Logical Plan (Broadcast Join & Forward-Fill Windowing)"
        )

        logger.info("ETL Pipeline completed successfully! All Silver and Gold tables materialized.")

    finally:
        for cached in (cached_silver, dim_teams, dim_contests):
            unpersist_dataframe(cached, blocking=False)
        spark.stop()
        logger.info("SparkSession stopped cleanly.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="NCAA Lacrosse ETL Pipeline Runner")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Force a complete rebuild of all Silver partitions from Bronze JSON snapshots",
    )
    args = parser.parse_args()
    run_pipeline(full_refresh=args.full_refresh)

import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame

from src.db.gold_storage import publish_gold_tables
from src.etl.optimizations import (
    apply_broadcast_join,
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
    extract_dimensions_from_snapshots,
    get_latest_valid_snapshots,
    get_pending_snapshots,
    process_bronze_to_silver,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PipelineRunner")


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
        silver_output.mkdir(parents=True, exist_ok=True)

        # Incremental Partition Detection (CDC)
        pending_snapshots = get_pending_snapshots(
            spark,
            snapshots,
            silver_output_path=silver_output,
            full_refresh=full_refresh,
        )

        if pending_snapshots:
            logger.info("Processing %s changed contest snapshot(s)...", len(pending_snapshots))
            s_df = process_bronze_to_silver(spark, pending_snapshots)
            try:
                if full_refresh or not (silver_output / "_delta_log").exists():
                    s_df.repartition("contest_id").write.format("delta").mode("overwrite").partitionBy("contest_id").option("overwriteSchema", "true").save(str(silver_output))
                else:
                    pending_cids = ",".join(str(s[0]) for s in pending_snapshots)
                    s_df.repartition("contest_id").write.format("delta").mode("overwrite").partitionBy("contest_id").option("replaceWhere", f"contest_id in ({pending_cids})").save(str(silver_output))
            except Exception as e:
                logger.warning("Delta write failed (%s); falling back to partitioned Parquet.", e)
                overwrite_mode = "static" if full_refresh or not any(silver_output.glob("contest_id=*/*.parquet")) else "dynamic"
                spark.conf.set("spark.sql.sources.partitionOverwriteMode", overwrite_mode)
                s_df.repartition("contest_id").write.mode("overwrite").partitionBy("contest_id").option("compression", "snappy").parquet(str(silver_output))
        else:
            logger.info("Silver is current; rebuilding Gold from current snapshots.")

        # Current Bronze is authoritative, including on a retry after Silver was
        # committed but the previous Gold publication failed.
        dim_teams, dim_contests = extract_dimensions_from_snapshots(spark, snapshots)

        if dim_teams is not None:
            dim_teams = dim_teams.cache()
            dim_teams.count()
        if dim_contests is not None:
            dim_contests = dim_contests.cache()
            dim_contests.count()

        # Optimization: Read partitioned Parquet / Delta & cache before downstream gold branching
        if (silver_output / "_delta_log").exists():
            silver_df = spark.read.format("delta").load(str(silver_output))
        else:
            silver_df = spark.read.parquet(str(silver_output))
        cached_silver = cache_and_profile(silver_df, "silver_plays")

        # Partition Skew Validation: Verify uniform play distribution across all contests
        logger.info("Testing Silver dataset for potential contest partition skew...")
        check_data_skew(
            cached_silver,
            partition_cols=["contest_id"],
            max_skew_ratio=2.5,
            max_absolute_rows=2000,
            raise_on_skew=True,
        )

        # Generate Gold Datasets
        logger.info("Generating Gold Star Schema and analytical aggregate tables...")
        gold_tables = generate_gold_tables(
            spark,
            cached_silver,
            dim_teams=dim_teams,
            dim_contests=dim_contests,
        )

        # Validate team distribution skew before downstream analytics
        logger.info("Testing aggregate team stats for potential team partition skew...")
        check_data_skew(
            gold_tables["agg_team_game_stats"],
            partition_cols=["team_id"],
            max_skew_ratio=3.5,
            raise_on_skew=True,
        )

        # Optimization: Broadcast Join on dim_teams lookup
        enriched_facts = apply_broadcast_join(
            cached_silver,
            gold_tables["dim_teams"].select("team_id", "color", "name_full"),
            left_on="team_id",
            right_on="team_id",
            how="left"
        )
        gold_tables["fact_plays"] = enriched_facts

        published = publish_gold_tables(gold_tables, GOLD_DIR)
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

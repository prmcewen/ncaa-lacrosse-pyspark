import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.db.gold_storage import publish_gold_tables
from src.etl.optimizations import (
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
    """Replace only changed contest partitions using Delta Lake."""
    output.mkdir(parents=True, exist_ok=True)
    writer = df.repartition("contest_id").write.format("delta").mode("overwrite").partitionBy("contest_id")
    if full_refresh or not (output / "_delta_log").exists():
        writer.option("overwriteSchema", "true").save(str(output))
    else:
        pending_cids = ",".join(str(s[0]) for s in pending_snapshots)
        writer.option("replaceWhere", f"contest_id in ({pending_cids})").save(str(output))


def _read_silver(spark, output: Path) -> DataFrame:
    return spark.read.format("delta").load(str(output))


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
    metrics = {}
    pipeline_start = time.perf_counter()

    try:
        stage_start = time.perf_counter()
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

        metrics["snapshot_selection_seconds"] = time.perf_counter() - stage_start

        if pending_plays:
            stage_start = time.perf_counter()
            logger.info("Processing %s changed play snapshot(s)...", len(pending_plays))
            plays = process_bronze_to_silver(spark, pending_plays)
            _write_silver_partitions(plays, silver_output, pending_plays, full_refresh)
            metrics["silver_plays_seconds"] = time.perf_counter() - stage_start

        if pending_contests:
            stage_start = time.perf_counter()
            logger.info("Processing %s changed contest metadata snapshot(s)...", len(pending_contests))
            contests = process_bronze_to_silver_contests(spark, pending_contests)
            _write_silver_partitions(contests, contests_output, pending_contests, full_refresh)
            metrics["silver_contests_seconds"] = time.perf_counter() - stage_start

        # Each Silver table has its own freshness check. A retry after a failed
        # Gold publication reads committed Silver without reparsing Bronze.
        stage_start = time.perf_counter()
        silver_contests = _selected_silver_contests(
            spark, _read_silver(spark, contests_output), snapshots,
        )
        dim_teams, dim_contests = extract_dimensions_from_silver_contests(silver_contests)

        dim_teams = dim_teams.cache()
        dim_contests = dim_contests.cache()

        metrics["dimensions_seconds"] = time.perf_counter() - stage_start

        # Persist Silver because both Gold outputs reuse it. Registration stays lazy.
        stage_start = time.perf_counter()
        silver_df = _read_silver(spark, silver_output)
        selected = spark.createDataFrame(
            [(int(cid), ts) for cid, ts, _ in snapshots],
            "contest_id long, ingest_timestamp string",
        )
        silver_df = silver_df.join(F.broadcast(selected), ["contest_id", "ingest_timestamp"], "inner")
        cached_silver = silver_df.persist()
        metrics["silver_read_seconds"] = time.perf_counter() - stage_start

        logger.info("Generating complete Gold tables from Silver...")
        stage_start = time.perf_counter()
        gold_tables = generate_gold_tables(spark, cached_silver, dim_teams, dim_contests)
        metrics["gold_build_seconds"] = time.perf_counter() - stage_start
        enriched_facts = gold_tables["fact_plays"]

        stage_start = time.perf_counter()
        published = publish_gold_tables(gold_tables, GOLD_DIR)
        metrics["gold_publication_seconds"] = time.perf_counter() - stage_start
        metrics["gold_published"] = True
        logger.info("Published complete Gold generation: %s", published)

        # Save Explain Plan for Portfolio & Audit Documentation
        plan_output = Path(os.environ.get(
            "LAXPXP_PLAN_OUTPUT",
            Path(__file__).resolve().parents[2] / "docs" / "spark_execution_plan.md",
        ))
        save_explain_plan(
            enriched_facts,
            plan_output,
            title="PySpark Physical & Logical Plan (Broadcast Join & Forward-Fill Windowing)"
        )

        logger.info("ETL Pipeline completed successfully! All Silver and Gold tables materialized.")

    finally:
        metrics["total_seconds"] = time.perf_counter() - pipeline_start
        metrics_path = os.environ.get("LAXPXP_BENCH_METRICS")
        if metrics_path:
            try:
                Path(metrics_path).write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
            except OSError as exc:
                logger.warning("Could not write benchmark metrics: %s", exc)
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

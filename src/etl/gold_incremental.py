"""Select Gold work from published rows and current Silver snapshots."""
from pathlib import Path
from typing import Callable, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.db.gold_storage import GOLD_TABLES, GoldTableWrite, resolve_gold_dir


class GoldGeneration(dict):
    """Complete queryable tables with a write plan for changed partitions."""

    def __init__(self, tables, writes):
        super().__init__(tables)
        self.writes = writes


def _read_table(spark: SparkSession, path: Path) -> DataFrame:
    if (path / "_delta_log").exists():
        return spark.read.format("delta").load(str(path))
    return spark.read.parquet(str(path))


def _changed_keys(current: DataFrame, previous: DataFrame, key: str) -> set[int]:
    if set(current.columns) != set(previous.columns):
        raise ValueError("Gold schema changed")
    columns = current.columns
    old = previous.select(*columns)
    changed = current.exceptAll(old).select(key).union(old.exceptAll(current).select(key))
    return {int(row[key]) for row in changed.distinct().collect() if row[key] is not None}


def _only_contests(df: DataFrame, contests: set[int]) -> DataFrame:
    return df.filter(F.col("contest_id").isin(sorted(contests))) if contests else df.limit(0)


def _merge_contests(previous: DataFrame, changed: DataFrame, contests: set[int]) -> DataFrame:
    if not contests:
        return previous
    return previous.filter(~F.col("contest_id").isin(sorted(contests))).unionByName(changed)


def _shot_profile(df: DataFrame, contests: set[int]) -> dict:
    rows = (_only_contests(df, contests)
            .filter(F.col("event_type") == "SHOT")
            .groupBy("shot_result")
            .agg(F.count("*").alias("total"),
                 F.sum(F.when(F.col("shot_possession_retained") == True, 1).otherwise(0)).alias("retained"))
            .collect())
    return {row["shot_result"]: (row["total"], row["retained"]) for row in rows}


def build_gold_tables(
    spark: SparkSession,
    silver: DataFrame,
    dim_teams: DataFrame,
    dim_contests: DataFrame,
    root: Path,
    full_refresh: bool,
    generator: Callable,
) -> Optional[Dict[str, DataFrame]]:
    """Return a complete Gold generation or None when the publication is current.

    A changed shot profile can change global expected-loss rates, so that case
    refreshes every aggregate. Facts remain scoped to changed contests and teams.
    """
    previous_dir = resolve_gold_dir(root)
    paths = {name: previous_dir / f"{name}.parquet" for name in GOLD_TABLES}
    if full_refresh or not all(path.exists() for path in paths.values()):
        tables = generator(spark, silver, dim_teams=dim_teams, dim_contests=dim_contests)
        writes = dict(tables)
        for name in ("fact_plays", "agg_team_game_stats"):
            writes[name] = GoldTableWrite(tables[name], partition_by="contest_id")
        return GoldGeneration(tables, writes)

    previous = {name: _read_table(spark, path) for name, path in paths.items()}
    old_facts = previous["fact_plays"]
    required = {"contest_id", "ingest_timestamp", "team_id", "event_type",
                "shot_result", "shot_possession_retained"}
    if not required.issubset(old_facts.columns):
        tables = generator(spark, silver, dim_teams=dim_teams, dim_contests=dim_contests)
        writes = dict(tables)
        for name in ("fact_plays", "agg_team_game_stats"):
            writes[name] = GoldTableWrite(tables[name], partition_by="contest_id")
        return GoldGeneration(tables, writes)

    try:
        team_ids = _changed_keys(dim_teams, previous["dim_teams"], "team_id")
        contest_ids = _changed_keys(dim_contests, previous["dim_contests"], "contest_id")
    except ValueError:
        tables = generator(spark, silver, dim_teams=dim_teams, dim_contests=dim_contests)
        writes = dict(tables)
        for name in ("fact_plays", "agg_team_game_stats"):
            writes[name] = GoldTableWrite(tables[name], partition_by="contest_id")
        return GoldGeneration(tables, writes)

    old_versions = {(row["contest_id"], row["ingest_timestamp"])
                    for row in old_facts.select("contest_id", "ingest_timestamp").distinct().collect()}
    new_versions = {(row["contest_id"], row["ingest_timestamp"])
                    for row in silver.select("contest_id", "ingest_timestamp").distinct().collect()}
    changed_plays = {cid for cid, _ in old_versions.symmetric_difference(new_versions)}
    if not (team_ids or contest_ids or changed_plays):
        return None

    fact_contests = set(changed_plays)
    if team_ids:
        for df in (old_facts, silver):
            fact_contests.update(int(row["contest_id"]) for row in
                                 df.filter(F.col("team_id").isin(sorted(team_ids)))
                                   .select("contest_id").distinct().collect())

    aggregate_contests = set(changed_plays)
    if changed_plays and _shot_profile(old_facts, changed_plays) != _shot_profile(silver, changed_plays):
        aggregate_contests = {int(row["contest_id"]) for row in
                              silver.select("contest_id").distinct().collect()}
        aggregate_contests.update(int(row["contest_id"]) for row in
                                  previous["agg_team_game_stats"].select("contest_id").distinct().collect())

    generated = generator(
        spark, _only_contests(silver, fact_contests),
        dim_teams=dim_teams, dim_contests=dim_contests,
        baseline_silver_df=silver,
        aggregate_silver_df=_only_contests(silver, aggregate_contests),
    )
    fact_updates = generated["fact_plays"]
    aggregate_updates = generated["agg_team_game_stats"]
    generated["fact_plays"] = _merge_contests(old_facts, fact_updates, fact_contests)
    generated["agg_team_game_stats"] = _merge_contests(
        previous["agg_team_game_stats"], aggregate_updates, aggregate_contests,
    )
    writes = dict(generated)
    for name, changed in (("dim_teams", team_ids), ("dim_contests", contest_ids)):
        if not changed:
            writes[name] = GoldTableWrite(generated[name], previous=paths[name])
    writes["fact_plays"] = GoldTableWrite(
        generated["fact_plays"], previous=paths["fact_plays"],
        updates=fact_updates if fact_contests else None,
        contests=fact_contests, partition_by="contest_id",
    )
    writes["agg_team_game_stats"] = GoldTableWrite(
        generated["agg_team_game_stats"], previous=paths["agg_team_game_stats"],
        updates=aggregate_updates if aggregate_contests else None,
        contests=aggregate_contests, partition_by="contest_id",
    )
    return GoldGeneration(generated, writes)

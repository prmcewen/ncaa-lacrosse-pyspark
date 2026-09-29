"""Incremental Gold selection and merge behavior."""
from pyspark.sql import functions as F

from src.etl.gold_incremental import build_gold_tables


def _frames(spark, *, corrected=False, retained=False):
    teams = spark.createDataFrame(
        [(10, "corrected" if corrected else "original"), (20, "other")],
        "team_id long, name_full string",
    )
    contests = spark.createDataFrame([(1, "one"), (2, "two")], "contest_id long, title string")
    silver = spark.createDataFrame([
        (1, "new", 10, "SHOT", "WIDE", retained),
        (2, "old", 20, "SHOT", "SAVE", True),
    ], "contest_id long, ingest_timestamp string, team_id long, "
       "event_type string, shot_result string, shot_possession_retained boolean")
    return silver, teams, contests


def _published(spark, root):
    silver, teams, contests = _frames(spark)
    old = silver.withColumn("ingest_timestamp", F.lit("old"))
    tables = {
        "dim_teams": teams,
        "dim_contests": contests,
        "fact_plays": old,
        "agg_team_game_stats": spark.createDataFrame(
            [(1, 10, "A"), (2, 20, "B")],
            "contest_id long, team_id long, team_short string",
        ),
    }
    for name, df in tables.items():
        df.write.parquet(str(root / f"{name}.parquet"))


def _generator(calls):
    def generate(spark, facts, dim_teams, dim_contests, *, baseline_silver_df=None,
                 aggregate_silver_df=None):
        calls.append((
            {r.contest_id for r in facts.select("contest_id").distinct().collect()},
            {r.contest_id for r in aggregate_silver_df.select("contest_id").distinct().collect()},
            {r.contest_id for r in baseline_silver_df.select("contest_id").distinct().collect()},
        ))
        agg = aggregate_silver_df.select("contest_id", "team_id").distinct().withColumn(
            "team_short", F.lit("changed"),
        )
        return {"dim_teams": dim_teams, "dim_contests": dim_contests,
                "fact_plays": facts, "agg_team_game_stats": agg}
    return generate


def test_incremental_gold_reuses_unaffected_contests(spark, tmp_path):
    _published(spark, tmp_path)
    silver, teams, contests = _frames(spark)
    calls = []
    result = build_gold_tables(spark, silver, teams, contests, tmp_path, False, _generator(calls))
    assert calls == [({1}, {1}, {1, 2})]
    assert {r.contest_id: r.ingest_timestamp for r in result["fact_plays"].collect()} == {1: "new", 2: "old"}
    assert {r.contest_id: r.team_short for r in result["agg_team_game_stats"].collect()} == {1: "changed", 2: "B"}


def test_shot_profile_change_refreshes_all_aggregates(spark, tmp_path):
    _published(spark, tmp_path)
    silver, teams, contests = _frames(spark, retained=True)
    calls = []
    build_gold_tables(spark, silver, teams, contests, tmp_path, False, _generator(calls))
    assert calls == [({1}, {1, 2}, {1, 2})]


def test_current_gold_skips_publication_and_team_change_refreshes_facts(spark, tmp_path):
    _published(spark, tmp_path)
    silver, teams, contests = _frames(spark)
    silver = silver.withColumn("ingest_timestamp", F.lit("old"))
    calls = []
    generator = _generator(calls)
    assert build_gold_tables(spark, silver, teams, contests, tmp_path, False, generator) is None
    corrected = teams.withColumn("name_full", F.when(F.col("team_id") == 10, "corrected").otherwise("other"))
    result = build_gold_tables(spark, silver, corrected, contests, tmp_path, False, generator)
    assert calls == [({1}, set(), {1, 2})]
    assert result["fact_plays"].count() == 2


def test_global_shot_rate_change_matches_full_gold_rebuild(spark, tmp_path):
    from src.db.gold_storage import publish_gold_tables
    from src.etl.transform import generate_gold_tables

    schema = ("play_id string, contest_id long, ingest_timestamp string, team_id long, "
              "event_team_short string, event_type string, shot_result string, "
              "shot_possession_retained boolean, clear_result string, penalty_duration_seconds long")
    old = spark.createDataFrame([
        ("1_1", 1, "old", 10, "A", "SHOT", "WIDE", False, None, None),
        ("2_1", 2, "old", 20, "B", "SHOT", "WIDE", False, None, None),
    ], schema)
    teams = spark.createDataFrame(
        [(10, "red", "A"), (20, "blue", "B")], "team_id long, color string, name_full string",
    )
    contests = spark.createDataFrame(
        [(1, "one", "final"), (2, "two", "final")],
        "contest_id long, title string, status string",
    )
    initial = build_gold_tables(spark, old, teams, contests, tmp_path, False, generate_gold_tables)
    publish_gold_tables(initial.writes, tmp_path)

    current = spark.createDataFrame([
        ("1_1", 1, "new", 10, "A", "SHOT", "WIDE", True, None, None),
        ("2_1", 2, "old", 20, "B", "SHOT", "WIDE", False, None, None),
    ], schema)
    incremental = build_gold_tables(spark, current, teams, contests, tmp_path, False, generate_gold_tables)
    full = generate_gold_tables(spark, current, teams, contests)
    actual = incremental["agg_team_game_stats"]
    expected = full["agg_team_game_stats"]
    assert actual.exceptAll(expected).count() == 0
    assert expected.exceptAll(actual).count() == 0
    assert actual.filter("contest_id = 2").first().normalized_shots_lost == 0.5

import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.etl.transform import (
    SILVER_DIR,
    extract_dimensions_from_silver_contests,
    generate_gold_tables,
    get_latest_valid_snapshots,
    get_pending_snapshots,
    process_bronze_to_silver,
    process_bronze_to_silver_contests,
)


def test_get_pending_snapshots_full_refresh(spark: SparkSession, tmp_path: Path):
    snapshots = [
        (101, "20260901T000000Z", tmp_path / "101.json"),
        (102, "20260901T000000Z", tmp_path / "102.json"),
    ]
    # When full_refresh is True, all snapshots should be returned regardless of silver state
    pending = get_pending_snapshots(spark, snapshots, silver_output_path=tmp_path / "silver", full_refresh=True)
    assert pending == snapshots


def test_get_pending_snapshots_missing_silver_dir(spark: SparkSession, tmp_path: Path):
    snapshots = [
        (101, "20260901T000000Z", tmp_path / "101.json"),
    ]
    non_existent = tmp_path / "non_existent_silver"
    pending = get_pending_snapshots(spark, snapshots, silver_output_path=non_existent, full_refresh=False)
    assert pending == snapshots


def test_get_pending_snapshots_incremental_detection(spark: SparkSession, tmp_path: Path):
    silver_path = tmp_path / "silver_test.parquet"
    silver_path.mkdir(parents=True, exist_ok=True)

    # Create a Delta-backed mock Silver with contests 101 and 102, matching
    # the format _write_silver_partitions produces in production.
    schema = StructType([
        StructField("contest_id", LongType(), False),
        StructField("ingest_timestamp", StringType(), False),
        StructField("val", StringType(), True),
    ])
    df = spark.createDataFrame([
        (101, "20260901T000000Z", "play1"),
        (102, "20260901T000000Z", "play2"),
    ], schema)
    df.repartition("contest_id").write.format("delta").mode("overwrite").partitionBy(
        "contest_id"
    ).save(str(silver_path))

    # Case A: Same snapshots -> 0 pending
    snapshots_up_to_date = [
        (101, "20260901T000000Z", tmp_path / "101.json"),
        (102, "20260901T000000Z", tmp_path / "102.json"),
    ]
    pending = get_pending_snapshots(spark, snapshots_up_to_date, silver_output_path=silver_path)
    assert len(pending) == 0

    # Case B: A new contest 103 is added
    snapshots_with_new = [
        (101, "20260901T000000Z", tmp_path / "101.json"),
        (102, "20260901T000000Z", tmp_path / "102.json"),
        (103, "20260901T000000Z", tmp_path / "103.json"),
    ]
    pending = get_pending_snapshots(spark, snapshots_with_new, silver_output_path=silver_path)
    assert len(pending) == 1
    assert pending[0][0] == 103

    # Case C: Contest 101 was re-ingested with a newer timestamp
    snapshots_with_updated = [
        (101, "20260902T120000Z", tmp_path / "101.json"),
        (102, "20260901T000000Z", tmp_path / "102.json"),
    ]
    pending = get_pending_snapshots(spark, snapshots_with_updated, silver_output_path=silver_path)
    assert len(pending) == 1
    assert pending[0][0] == 101
    assert pending[0][1] == "20260902T120000Z"


def test_get_pending_snapshots_rejects_parquet_only_silver(spark: SparkSession, tmp_path: Path):
    """Silver written without a Delta log must fail loudly, not be read as Parquet."""
    silver_path = tmp_path / "silver_parquet_only.parquet"
    df = spark.createDataFrame(
        [(101, "20260901T000000Z", "play1")],
        "contest_id long, ingest_timestamp string, val string",
    )
    df.repartition("contest_id").write.mode("overwrite").partitionBy("contest_id").parquet(str(silver_path))

    snapshots = [(101, "20260901T000000Z", tmp_path / "101.json")]
    with pytest.raises(ValueError, match="Delta"):
        get_pending_snapshots(spark, snapshots, silver_output_path=silver_path)

    # --full-refresh is the documented recovery path: it rebuilds every snapshot,
    # and _write_silver_partitions then replaces the Parquet with a Delta table.
    assert get_pending_snapshots(
        spark, snapshots, silver_output_path=silver_path, full_refresh=True
    ) == snapshots


def test_get_pending_snapshots_treats_empty_dir_as_unmaterialized(spark: SparkSession, tmp_path: Path):
    silver_path = tmp_path / "silver_empty.parquet"
    silver_path.mkdir(parents=True, exist_ok=True)

    snapshots = [(101, "20260901T000000Z", tmp_path / "101.json")]
    assert get_pending_snapshots(spark, snapshots, silver_output_path=silver_path) == snapshots


def test_process_bronze_to_silver_batch_and_single(spark: SparkSession):
    snapshots = get_latest_valid_snapshots()
    assert len(snapshots) >= 2

    # Pick two snapshots for testing
    s1, s2 = snapshots[0], snapshots[1]

    # Process individually
    df1 = process_bronze_to_silver(spark, s1)
    df2 = process_bronze_to_silver(spark, s2)
    count1 = df1.count()
    count2 = df2.count()

    # Process both together in a single batch
    batch_df = process_bronze_to_silver(spark, [s1, s2])
    batch_count = batch_df.count()

    assert batch_count == count1 + count2
    assert set(batch_df.columns) == set(df1.columns)
    assert set(batch_df.columns) == set(df2.columns)
    assert "contest_id" in batch_df.columns
    assert "play_id" in batch_df.columns
    assert "event_type" in batch_df.columns

    # Verify contest IDs in batch
    cids = {r["contest_id"] for r in batch_df.select("contest_id").distinct().collect()}
    assert cids == {s1[0], s2[0]}


def test_generate_gold_tables_broadcast_join(spark: SparkSession):
    snapshots = get_latest_valid_snapshots()
    assert len(snapshots) >= 1
    silver_df = process_bronze_to_silver(spark, snapshots[:2])
    silver_contests = process_bronze_to_silver_contests(spark, snapshots[:2])
    dim_teams, dim_contests = extract_dimensions_from_silver_contests(silver_contests)

    gold_tables = generate_gold_tables(spark, silver_df, dim_teams, dim_contests)
    assert "fact_plays" in gold_tables
    fact_df = gold_tables["fact_plays"]

    # Verify dimensional columns from dim_teams are joined into fact_plays via broadcast join
    assert "color" in fact_df.columns
    assert "name_full" in fact_df.columns
    assert fact_df.count() == silver_df.count()

    # Verify baseline retention broadcast join produces expected analytical columns in agg_team_game_stats
    assert "agg_team_game_stats" in gold_tables
    agg_df = gold_tables["agg_team_game_stats"]
    assert "normalized_shots_lost" in agg_df.columns
    assert "normalized_shooting_efficiency" in agg_df.columns
    assert agg_df.count() > 0


def test_generate_gold_tables_with_pre_extracted_dimensions(spark: SparkSession):
    snapshots = get_latest_valid_snapshots()
    assert len(snapshots) >= 2
    silver_df = process_bronze_to_silver(spark, snapshots[:2])
    silver_contests = process_bronze_to_silver_contests(spark, snapshots[:2])
    dim_teams, dim_contests = extract_dimensions_from_silver_contests(silver_contests)

    # Supply Silver-derived dimensions explicitly to the Gold builder.
    gold_tables = generate_gold_tables(
        spark,
        silver_df,
        dim_teams=dim_teams,
        dim_contests=dim_contests,
    )

    assert "fact_plays" in gold_tables
    assert "dim_teams" in gold_tables
    assert "dim_contests" in gold_tables
    assert "agg_team_game_stats" in gold_tables

    fact_df = gold_tables["fact_plays"]
    assert "color" in fact_df.columns
    assert "name_full" in fact_df.columns
    assert fact_df.count() == silver_df.count()
    assert gold_tables["dim_teams"].count() == dim_teams.count()
    assert gold_tables["dim_contests"].count() == dim_contests.count()


def test_gold_aggregate_uses_current_silver_for_global_shot_rates(spark: SparkSession):
    silver = spark.createDataFrame([
        ("1_1", 1, "new", 10, "A", "SHOT", "WIDE", True, None, None),
        ("2_1", 2, "old", 20, "B", "SHOT", "WIDE", False, None, None),
    ], "play_id string, contest_id long, ingest_timestamp string, team_id long, "
       "event_team_short string, event_type string, shot_result string, "
       "shot_possession_retained boolean, clear_result string, penalty_duration_seconds long")
    teams = spark.createDataFrame(
        [(10, "red", "A"), (20, "blue", "B")],
        "team_id long, color string, name_full string",
    )
    contests = spark.createDataFrame(
        [(1, "one", "final"), (2, "two", "final")],
        "contest_id long, title string, status string",
    )

    gold = generate_gold_tables(spark, silver, teams, contests)
    contest_two = gold["agg_team_game_stats"].filter("contest_id = 2").first()
    assert contest_two.normalized_shots_lost == 0.5


def test_run_pipeline_unpersists_cached_silver_in_finally(spark: SparkSession, tmp_path: Path, monkeypatch):
    """Verify the current Silver cache is released after a downstream Gold failure."""
    from unittest.mock import MagicMock
    from src.etl import run_pipeline as runner

    class SparkWrapper:
        def __init__(self, spark_session):
            self._spark = spark_session
            self.stop = MagicMock()

        def __getattr__(self, name):
            return getattr(self._spark, name)

    mock_spark = SparkWrapper(spark)
    monkeypatch.setattr(runner, "get_spark_session", lambda *args, **kwargs: mock_spark)
    monkeypatch.setattr(runner, "SILVER_DIR", tmp_path / "silver")
    monkeypatch.setattr(runner, "GOLD_DIR", tmp_path / "gold")
    snapshots = [(1, "20260901T000000Z", tmp_path / "1.json")]
    monkeypatch.setattr(runner, "get_latest_valid_snapshots", lambda: snapshots)
    monkeypatch.setattr(runner, "get_pending_snapshots", lambda *args, **kwargs: [])

    silver_plays = spark.createDataFrame(
        [("1_1", 1, "20260901T000000Z", 10)],
        "play_id string, contest_id long, ingest_timestamp string, team_id long",
    )
    silver_contests = spark.createDataFrame(
        [(1, "20260901T000000Z", "one", "final", [
            (10, "HOME", "Home", "HOME", "home", "red", True),
            (20, "AWAY", "Away", "AWAY", "away", "blue", False),
        ])],
        "contest_id long, ingest_timestamp string, title string, status string, "
        "teams array<struct<team_id:long,name_short:string,name_full:string,name_6char:string,"
        "seoname:string,color:string,is_home:boolean>>",
    )
    monkeypatch.setattr(
        runner, "_read_silver",
        lambda session, path: silver_contests if "contests" in path.name else silver_plays,
    )

    unpersisted_play_cache = []
    original_unpersist = runner.unpersist_dataframe

    def track_unpersist(df, *args, **kwargs):
        if df is not None and "play_id" in df.columns:
            assert df.is_cached
            unpersisted_play_cache.append((df, args, kwargs))
        return original_unpersist(df, *args, **kwargs)

    monkeypatch.setattr(runner, "unpersist_dataframe", track_unpersist)
    monkeypatch.setattr(
        runner, "generate_gold_tables",
        MagicMock(side_effect=RuntimeError("Forced downstream failure")),
    )

    with pytest.raises(RuntimeError, match="Forced downstream failure"):
        runner.run_pipeline()

    assert len(unpersisted_play_cache) == 1
    cached, args, kwargs = unpersisted_play_cache[0]
    assert not cached.is_cached
    assert kwargs.get("blocking") is False or args == (False,)
    assert mock_spark.stop.called

def test_turnover_missing_clock_backward_fill(spark: SparkSession, tmp_path: Path):
    """Verify that plays missing clock (such as turnovers) backward-fill to the next available

    clock in the period, and default to 0:00 when there is no subsequent clock in the quarter/game.
    """
    import json

    mock_data = {
        "data": {
            "playbyplay": {
                "contestId": 999999,
                "title": "Mock Contest",
                "status": "final",
                "teams": [
                    {"teamId": "10", "nameShort": "HOME", "name6Char": "HOME", "nameFull": "Home",
                     "color": "red", "isHome": True, "seoname": "home"},
                    {"teamId": "20", "nameShort": "AWAY", "name6Char": "AWAY", "nameFull": "Away",
                     "color": "blue", "isHome": False, "seoname": "away"},
                ],
                "periods": [
                    {
                        "periodNumber": 1,
                        "periodDisplay": "1st",
                        "playbyplayStats": [
                            # Play 1: Turnover at beginning of period, clock is missing ("")
                            {"clock": "", "teamId": 10, "plays": [{"playText": "Turnover by HOME Alice."}]},
                            # Play 2: Shot with explicit clock 14:45
                            {"clock": "14:45", "teamId": 20, "plays": [{"playText": "Shot by AWAY Bob WIDE."}]},
                            # Play 3: Turnover in middle of period, clock is None
                            {"clock": None, "teamId": 10, "plays": [{"playText": "Turnover by HOME Charlie."}]},
                            # Play 4: Ground ball with explicit clock 14:10
                            {"clock": "14:10", "teamId": 20, "plays": [{"playText": "Ground ball pickup by AWAY Dan."}]},
                            # Play 5: Turnover at end of period, clock is missing ("")
                            {"clock": "", "teamId": 10, "plays": [{"playText": "Turnover by HOME Eve."}]},
                        ],
                    },
                    {
                        # Period 2: No clocks present in the entire period
                        "periodNumber": 2,
                        "periodDisplay": "2nd",
                        "playbyplayStats": [
                            {"clock": None, "teamId": 10, "plays": [{"playText": "Turnover by HOME Frank."}]},
                        ],
                    },
                ],
            }
        }
    }
    json_path = tmp_path / "mock_turnover_bfill.json"
    with open(json_path, "w") as f:
        json.dump(mock_data, f)

    snapshot = (999999, "20260927T200000Z", json_path)
    df = process_bronze_to_silver(spark, snapshot)

    plays = df.orderBy("play_seq").select(
        "period_number", "play_seq", "play_text", "clock_display", "period_seconds_remaining"
    ).collect()

    assert len(plays) == 6

    # Play 1: Turnover at start backward-fills to next available clock (14:45)
    assert plays[0]["play_text"] == "Turnover by HOME Alice."
    assert plays[0]["clock_display"] == "14:45"
    assert plays[0]["period_seconds_remaining"] == 14 * 60 + 45

    # Play 2: Explicit clock preserved
    assert plays[1]["play_text"] == "Shot by AWAY Bob WIDE."
    assert plays[1]["clock_display"] == "14:45"
    assert plays[1]["period_seconds_remaining"] == 14 * 60 + 45

    # Play 3: Turnover in middle backward-fills to next clock (14:10)
    assert plays[2]["play_text"] == "Turnover by HOME Charlie."
    assert plays[2]["clock_display"] == "14:10"
    assert plays[2]["period_seconds_remaining"] == 14 * 60 + 10

    # Play 4: Explicit clock preserved
    assert plays[3]["play_text"] == "Ground ball pickup by AWAY Dan."
    assert plays[3]["clock_display"] == "14:10"

    # Play 5: Turnover at end of quarter/game has no subsequent clock -> defaults to 0:00
    assert plays[4]["play_text"] == "Turnover by HOME Eve."
    assert plays[4]["clock_display"] == "0:00"
    assert plays[4]["period_seconds_remaining"] == 0

    # Play 6: Turnover in period with no clocks at all -> defaults to 0:00
    assert plays[5]["play_text"] == "Turnover by HOME Frank."
    assert plays[5]["clock_display"] == "0:00"
    assert plays[5]["period_seconds_remaining"] == 0


def test_write_silver_partitions_fails_without_parquet_fallback(spark: SparkSession, tmp_path: Path):
    from unittest.mock import MagicMock
    from src.etl.run_pipeline import _write_silver_partitions

    silver_out = tmp_path / "silver_fail.parquet"
    mock_df = MagicMock()
    mock_writer = MagicMock()
    mock_df.repartition.return_value.write.format.return_value.mode.return_value.partitionBy.return_value = mock_writer
    mock_writer.option.return_value.save.side_effect = RuntimeError("Delta Lake write simulation failure")

    with pytest.raises(RuntimeError, match="Delta Lake write simulation failure"):
        _write_silver_partitions(mock_df, silver_out, [(1, "20260901T000000Z", tmp_path / "1.json")], full_refresh=True)

    assert not list(silver_out.glob("contest_id=*/*.parquet"))


def test_read_silver_fails_if_not_delta(spark: SparkSession, tmp_path: Path):
    from src.etl.run_pipeline import _read_silver

    parquet_dir = tmp_path / "plain_parquet.parquet"
    df = spark.createDataFrame([(1, "20260901T000000Z")], "contest_id long, ingest_timestamp string")
    df.write.parquet(str(parquet_dir))

    with pytest.raises(Exception) as exc_info:
        _read_silver(spark, parquet_dir)
    assert "DELTA" in type(exc_info.value).__name__ or "delta" in str(exc_info.value).lower()


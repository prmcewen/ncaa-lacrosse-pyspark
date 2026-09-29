import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.etl.transform import (
    SILVER_DIR,
    extract_dimensions_from_pbp,
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

    # Create mock silver parquet with contest 101 and 102
    schema = StructType([
        StructField("contest_id", LongType(), False),
        StructField("ingest_timestamp", StringType(), False),
        StructField("val", StringType(), True),
    ])
    df = spark.createDataFrame([
        (101, "20260901T000000Z", "play1"),
        (102, "20260901T000000Z", "play2"),
    ], schema)
    df.repartition("contest_id").write.mode("overwrite").partitionBy("contest_id").parquet(str(silver_path))

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


def test_process_bronze_to_silver_with_dimensions(spark: SparkSession):
    snapshots = get_latest_valid_snapshots()
    assert len(snapshots) >= 2
    silver_df, dim_teams, dim_contests = process_bronze_to_silver(spark, snapshots[:2], return_dimensions=True)

    assert silver_df.count() > 0
    assert dim_teams.count() > 0
    assert dim_contests.count() == 2

    # Check dim_teams columns
    expected_team_cols = {"team_id", "name_short", "name_full", "name_6char", "seoname", "color"}
    assert expected_team_cols.issubset(set(dim_teams.columns))

    # Check dim_contests columns
    expected_contest_cols = {"contest_id", "title", "status"}
    assert expected_contest_cols.issubset(set(dim_contests.columns))


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


def test_run_pipeline_unpersists_cached_silver_in_finally(spark: SparkSession, monkeypatch):
    """Verify run_pipeline safely unpersists cached_silver in finally: block even on errors."""
    from unittest.mock import MagicMock
    from src.etl import run_pipeline as rp_module

    class SparkWrapper:
        def __init__(self, spark_session):
            self._spark = spark_session
            self.stop = MagicMock()

        def __getattr__(self, name):
            return getattr(self._spark, name)

    mock_spark = SparkWrapper(spark)
    monkeypatch.setattr(rp_module, "get_spark_session", lambda *args, **kwargs: mock_spark)

    unpersist_called = []
    real_cache_and_profile = rp_module.cache_and_profile

    def mock_cache_and_profile(df, label, **kwargs):
        cached = real_cache_and_profile(df, label, **kwargs)
        orig_unpersist = cached.unpersist

        def tracking_unpersist(*u_args, **u_kwargs):
            unpersist_called.append((u_args, u_kwargs))
            return orig_unpersist(*u_args, **u_kwargs)

        cached.unpersist = tracking_unpersist
        return cached

    monkeypatch.setattr(rp_module, "cache_and_profile", mock_cache_and_profile)
    monkeypatch.setattr(rp_module, "generate_gold_tables", MagicMock(side_effect=RuntimeError("Forced downstream failure")))
    monkeypatch.setattr(
        rp_module, "build_gold_tables",
        lambda session, silver, teams, contests, *args: rp_module.generate_gold_tables(
            session, silver, dim_teams=teams, dim_contests=contests,
        ),
    )

    with pytest.raises(RuntimeError, match="Forced downstream failure"):
        rp_module.run_pipeline()

    assert len(unpersist_called) == 1
    # Check that blocking=False was passed
    args, kwargs = unpersist_called[0]
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



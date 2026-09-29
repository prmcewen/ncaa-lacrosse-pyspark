import json

import pytest
from pyspark.sql import functions as F

from src.etl.schemas import NCAA_PBP_SCHEMA
from src.etl.transform import (
    extract_dimensions_from_pbp,
    extract_dimensions_from_silver_contests,
    process_bronze_to_silver_contests,
    validate_dimension_keys,
)


def _payload(contest_id, title, color, status="final"):
    return {"data": {"playbyplay": {
        "contestId": contest_id, "title": title, "status": status,
        "teams": [
            {"teamId": "10", "nameShort": "HOME", "name6Char": "HOME", "nameFull": title,
             "color": color, "isHome": True, "seoname": "home"},
            {"teamId": "20", "nameShort": "AWAY", "name6Char": "AWAY", "nameFull": "Away",
             "color": "blue", "isHome": False, "seoname": "away"},
        ],
        "periods": [{"periodNumber": 1, "periodDisplay": "1st", "playbyplayStats": [
            {"clock": "15:00", "teamId": 10, "plays": [{"playText": "Shot by HOME Alex Adams WIDE."}]},
            {"clock": "14:00", "teamId": 20, "plays": [{"playText": "Shot by AWAY Ben Baker WIDE."}]},
        ]}],
    }}}


def test_latest_dimension_rows_are_unique_and_deterministic(spark):
    payloads = [_payload(1, "old", "red", "live"), _payload(2, "middle", "blue"), _payload(1, "new", "green")]
    pbp = spark.createDataFrame(payloads, NCAA_PBP_SCHEMA).select("data.playbyplay.*")
    pbp = pbp.withColumn("_snapshot_timestamp", F.when(F.col("title") == "new", "20260903")
                         .when(F.col("title") == "middle", "20260902").otherwise("20260901"))
    for partitions in (1, 3):
        teams, contests = extract_dimensions_from_pbp(pbp.repartition(partitions))
        assert teams.count() == 2
        assert contests.count() == 2
        team = teams.filter("team_id = 10").first()
        assert (team.name_full, team.color) == ("new", "green")
        contest = contests.filter("contest_id = 1").first()
        assert (contest.title, contest.status) == ("new", "final")
        facts = spark.createDataFrame([(1, 10), (2, 20)], ["play_id", "team_id"])
        assert facts.join(teams, "team_id", "left").count() == facts.count()


def test_silver_contests_preserves_teams_without_plays(spark, tmp_path):
    older = _payload(1, "old", "red")
    newer = _payload(2, "new", "green")
    older["data"]["playbyplay"]["periods"] = []
    newer["data"]["playbyplay"]["periods"] = []
    paths = [tmp_path / "old.json", tmp_path / "new.json"]
    for path, payload in zip(paths, (older, newer)):
        path.write_text(json.dumps(payload))
    snapshots = [(1, "20260901T000000Z", paths[0]), (2, "20260902T000000Z", paths[1])]

    silver_contests = process_bronze_to_silver_contests(spark, snapshots)
    assert silver_contests.count() == 2
    assert silver_contests.filter("contest_id = 1").first().teams[0].color == "red"
    teams, contests = extract_dimensions_from_silver_contests(silver_contests)
    assert teams.count() == 2
    assert contests.count() == 2
    assert (teams.filter("team_id = 10").first().name_full,
            teams.filter("team_id = 10").first().color) == ("new", "green")
    assert contests.filter("contest_id = 1").first().title == "old"


@pytest.mark.parametrize("rows,message", [([(10,), (10,)], "duplicate"), ([(None,)], "null")])
def test_invalid_dimension_keys_are_rejected(spark, rows, message):
    with pytest.raises(ValueError, match=message):
        validate_dimension_keys(spark.createDataFrame(rows, "team_id long"), "team_id")


def test_missing_silver_contests_backfills_without_rebuilding_plays(spark, tmp_path, monkeypatch):
    from shutil import rmtree
    from src.etl import run_pipeline as runner

    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    source = tmp_path / "contest.json"
    source.write_text(json.dumps(_payload(1, "original", "red")))
    snapshots = [(1, "20260901T000000Z", source)]
    monkeypatch.setattr(runner, "SILVER_DIR", silver)
    monkeypatch.setattr(runner, "GOLD_DIR", gold)
    monkeypatch.setattr(runner, "get_spark_session", lambda *args: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(runner, "get_latest_valid_snapshots", lambda: snapshots)
    monkeypatch.setattr(runner, "save_explain_plan", lambda *args, **kwargs: None)
    runner.run_pipeline()
    first_publication = (gold / "current.json").read_bytes()
    runner.run_pipeline()
    assert (gold / "current.json").read_bytes() == first_publication

    rmtree(silver / "silver_contests.parquet")
    def unexpected_play_rebuild(*args, **kwargs):
        raise AssertionError("Current Silver plays should not be rebuilt")
    monkeypatch.setattr(runner, "process_bronze_to_silver", unexpected_play_rebuild)
    runner.run_pipeline()
    assert (silver / "silver_contests.parquet").exists()
    assert (gold / "current.json").read_bytes() == first_publication


def test_retry_after_silver_write_publishes_corrected_dimensions(spark, tmp_path, monkeypatch):
    from src.etl import run_pipeline as runner
    from src.db.gold_storage import resolve_gold_dir
    from src.db import duckdb_client as database

    gold = tmp_path / "gold"
    silver = tmp_path / "silver"
    monkeypatch.setattr(runner, "GOLD_DIR", gold)
    monkeypatch.setattr(runner, "SILVER_DIR", silver)
    monkeypatch.setattr(runner, "get_spark_session", lambda *args: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)
    monkeypatch.setattr(runner, "save_explain_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(database, "GOLD_DIR", gold)
    monkeypatch.setattr(database, "SILVER_PARQUET", silver / "silver_plays.parquet")
    source = tmp_path / "old.json"
    source.write_text(json.dumps(_payload(1, "old", "red", "live")))
    snapshots = [(1, "20260901T000000Z", source)]
    monkeypatch.setattr(runner, "get_latest_valid_snapshots", lambda: snapshots)
    runner.run_pipeline()
    previous = resolve_gold_dir(gold)
    original_publish = runner.publish_gold_tables

    corrected = tmp_path / "corrected.json"
    corrected.write_text(json.dumps(_payload(1, "corrected", "green")))
    snapshots[:] = [(1, "20260902T000000Z", corrected)]

    def fail_publish(*args):
        raise RuntimeError("injected publication failure")

    monkeypatch.setattr(runner, "publish_gold_tables", fail_publish)
    with pytest.raises(RuntimeError, match="injected publication failure"):
        runner.run_pipeline()
    assert resolve_gold_dir(gold) == previous
    assert runner.get_pending_snapshots(spark, snapshots, silver / "silver_plays.parquet") == []
    monkeypatch.setattr(runner, "publish_gold_tables", original_publish)

    def unexpected_bronze_read(*args, **kwargs):
        raise AssertionError("Silver is current; Bronze should not be reparsed")

    monkeypatch.setattr(runner, "process_bronze_to_silver", unexpected_bronze_read)
    monkeypatch.setattr(runner, "process_bronze_to_silver_contests", unexpected_bronze_read)
    runner.run_pipeline()
    assert resolve_gold_dir(gold) != previous
    client = database.DuckDBClient()
    try:
        assert client.get_contests() == [{"contest_id": 1, "title": "corrected", "status": "final"}]
        assert client.execute_query("SELECT name_full, color FROM dim_teams WHERE team_id=10") == [{"name_full": "corrected", "color": "green"}]
        assert client.execute_query("SELECT count(*) n, count(DISTINCT play_id) unique_n FROM fact_plays") == [{"n": 2, "unique_n": 2}]
    finally:
        client.close()

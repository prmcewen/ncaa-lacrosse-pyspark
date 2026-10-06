"""Regression coverage for contest replacement inside shared Delta files."""
import pytest
from delta.tables import DeltaTable

from src.etl.run_pipeline import _write_silver_partitions
from src.etl.transform import get_pending_snapshots
from src.db.gold_storage import publish_gold_tables


def test_incremental_replacement_preserves_other_games_in_shared_file(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("LAXPXP_DELTA_WRITE_PARTITIONS", "1")
    output = tmp_path / "silver"
    schema = "contest_id long, ingest_timestamp string, play_id string"
    original = spark.createDataFrame([
        (1, "old", "1_1"), (1, "old", "1_2"), (2, "old", "2_1"),
    ], schema)
    snapshots = [(1, "old", tmp_path / "1.json"), (2, "old", tmp_path / "2.json")]
    _write_silver_partitions(original, output, snapshots, full_refresh=True)
    assert not original.is_cached
    assert DeltaTable.forPath(spark, str(output)).detail().first().numFiles == 1
    assert not list(output.glob("contest_id=*"))

    # Shrink game 1 and add game 3, while game 2 shares the overwritten file.
    changed = [(1, "new", tmp_path / "1.json"), (3, "new", tmp_path / "3.json")]
    incoming = spark.createDataFrame([(1, "new", "1_1"), (3, "new", "3_1")], schema)
    _write_silver_partitions(incoming, output, changed, full_refresh=False)
    assert not incoming.is_cached
    actual = spark.read.format("delta").load(str(output))
    assert {(r.contest_id, r.ingest_timestamp, r.play_id) for r in actual.collect()} == {
        (1, "new", "1_1"), (2, "old", "2_1"), (3, "new", "3_1"),
    }
    assert get_pending_snapshots(spark, [changed[0], snapshots[1], changed[1]], output) == []

    # An out-of-predicate row must fail atomically without altering game 2.
    invalid = spark.createDataFrame([(2, "bad", "bad")], schema)
    with pytest.raises(Exception, match="(?i)constraint|replaceWhere"):
        _write_silver_partitions(
            invalid, output, changed[:1], False,
        )
    assert not invalid.is_cached
    assert spark.read.format("delta").load(str(output)).filter("contest_id=2").first().play_id == "2_1"


def test_full_refresh_migrates_legacy_partitioning(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("LAXPXP_DELTA_WRITE_PARTITIONS", "2")
    output = tmp_path / "legacy"
    original = spark.createDataFrame([(1, "old"), (2, "old")], "contest_id long, ingest_timestamp string")
    original.write.format("delta").partitionBy("contest_id").save(str(output))
    snapshots = [(1, "new", tmp_path / "1.json")]
    incoming = spark.createDataFrame([(1, "new")], original.schema)
    with pytest.raises(ValueError, match="full-refresh"):
        _write_silver_partitions(incoming, output, snapshots, False)
    _write_silver_partitions(incoming, output, snapshots, True)
    detail = DeltaTable.forPath(spark, str(output)).detail().first()
    assert detail.partitionColumns == []
    assert detail.numFiles <= 2
    assert spark.read.format("delta").load(str(output)).collect() == incoming.collect()


def test_gold_writes_bound_files_without_contest_directories(spark, tmp_path, monkeypatch):
    monkeypatch.setenv("LAXPXP_DELTA_WRITE_PARTITIONS", "2")
    games = spark.createDataFrame([(i,) for i in range(40)], "contest_id long")
    teams = spark.createDataFrame([(i,) for i in range(4)], "team_id long")
    published = publish_gold_tables({
        "dim_teams": teams, "dim_contests": games,
        "fact_plays": games, "agg_team_game_stats": games,
    }, tmp_path)
    for name, expected in [("dim_teams", 4), ("dim_contests", 40), ("fact_plays", 40), ("agg_team_game_stats", 40)]:
        output = published / f"{name}.parquet"
        detail = DeltaTable.forPath(spark, str(output)).detail().first()
        assert detail.partitionColumns == []
        assert detail.numFiles <= (1 if name.startswith("dim_") else 2)
        assert spark.read.format("delta").load(str(output)).count() == expected

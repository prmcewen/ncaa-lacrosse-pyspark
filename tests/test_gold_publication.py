import json
import os

import pytest
from fastapi.testclient import TestClient

from src.db import gold_storage as storage
from src.db import duckdb_client as database


def _tables(spark, version):
    stats = {name: 0 for name in ["total_shots", "shots_retained", "realized_shots_lost",
             "realized_shot_possessions_used", "saves_faced", "ground_balls", "turnovers",
             "clears_good", "clears_failed", "penalties", "penalty_seconds"]}
    stats.update(contest_id=1, team_id=10, team_short="T", goals=version,
                 shooting_pct=0.0, realized_shooting_efficiency=0.0)
    return {
        "dim_teams": spark.createDataFrame([(10, f"v{version}")], "team_id long, name_full string"),
        "dim_contests": spark.createDataFrame([(1, f"v{version}", "final")], "contest_id long, title string, status string"),
        "fact_plays": spark.createDataFrame([(f"v{version}", 1, 10)], "play_id string, contest_id long, team_id long"),
        "agg_team_game_stats": spark.createDataFrame([stats]),
    }


def _use_root(monkeypatch, root):
    monkeypatch.setattr(database, "GOLD_DIR", root)
    monkeypatch.setattr(database, "SILVER_PARQUET", root / "missing-silver")


def test_publication_switches_all_tables_and_retains_active_readers(spark, tmp_path, monkeypatch):
    _use_root(monkeypatch, tmp_path)
    old = storage.publish_gold_tables(_tables(spark, 1), tmp_path)
    reader = database.DuckDBClient()
    sync = storage._sync_tree

    def before_commit(staged):
        assert storage.resolve_gold_dir(tmp_path) == old
        assert reader.get_contests()[0]["title"] == "v1"
        sync(staged)

    monkeypatch.setattr(storage, "_sync_tree", before_commit)
    current = storage.publish_gold_tables(_tables(spark, 2), tmp_path)
    new_reader = database.DuckDBClient()
    try:
        assert current != old
        assert reader.get_contests()[0]["title"] == "v1"
        assert new_reader.get_contests()[0]["title"] == "v2"
        for client, version in [(reader, 1), (new_reader, 2)]:
            row = client.execute_query("""SELECT c.title, t.name_full, f.play_id, s.goals
                FROM dim_contests c JOIN fact_plays f USING(contest_id)
                JOIN dim_teams t USING(team_id)
                JOIN agg_team_game_stats s USING(contest_id,team_id)""")[0]
            assert row == {"title": f"v{version}", "name_full": f"v{version}", "play_id": f"v{version}", "goals": version}
    finally:
        reader.close()
        new_reader.close()


@pytest.mark.parametrize("failure_stage", ["write", "pointer"])
def test_failed_publication_preserves_previous_generation(spark, tmp_path, monkeypatch, failure_stage):
    old = storage.publish_gold_tables(_tables(spark, 1), tmp_path)
    previous_pointer = (tmp_path / "current.json").read_bytes()
    tables = _tables(spark, 2)
    with monkeypatch.context() as faults:
        if failure_stage == "write":
            class FailingFrame:
                @property
                def write(self):
                    raise RuntimeError("injected write failure")
            tables["fact_plays"] = FailingFrame()
        else:
            def fail_replace(*args):
                raise RuntimeError("injected pointer failure")
            faults.setattr(storage.os, "replace", fail_replace)
        with pytest.raises(RuntimeError, match="injected"):
            storage.publish_gold_tables(tables, tmp_path)
    assert storage.resolve_gold_dir(tmp_path) == old
    assert (tmp_path / "current.json").read_bytes() == previous_pointer
    assert all(any((old / f"{name}.parquet").glob("*.parquet")) for name in storage.GOLD_TABLES)
    assert not list((tmp_path / "versions").glob(".staging-*"))
    current = storage.publish_gold_tables(_tables(spark, 3), tmp_path)
    assert storage.resolve_gold_dir(tmp_path) == current
    assert old.exists()


def test_api_pins_nested_graphql_reads_and_next_request_sees_new_version(spark, tmp_path, monkeypatch):
    from src.api.main import app
    _use_root(monkeypatch, tmp_path)
    old = storage.publish_gold_tables(_tables(spark, 1), tmp_path)
    new = storage.publish_gold_tables(_tables(spark, 2), tmp_path)

    def point_to(version):
        candidate = tmp_path / "test-pointer.json"
        candidate.write_text(json.dumps({"version": version.name}))
        os.replace(candidate, tmp_path / "current.json")

    # API request startup must not inspect Silver while ETL is replacing it.
    database.SILVER_PARQUET.mkdir()
    (database.SILVER_PARQUET / "broken.parquet").write_bytes(b"partial ETL write")
    point_to(old)
    original = database.DuckDBClient.get_contests

    def publish_between_resolvers(client):
        result = original(client)
        point_to(new)
        return result

    monkeypatch.setattr(database.DuckDBClient, "get_contests", publish_between_resolvers)
    with TestClient(app) as client:
        query = {"query": "{ contests { title teamStats { goals } } }"}
        first = client.post("/graphql", json=query).json()
        assert "errors" not in first, first
        assert first["data"]["contests"] == [{"title": "v1", "teamStats": [{"goals": 1}]}]
        second = client.post("/graphql", json=query).json()
        assert second["data"]["contests"] == [{"title": "v2", "teamStats": [{"goals": 2}]}]
        assert client.get("/api/contests").json() == [{"contest_id": 1, "title": "v2", "status": "final"}]


def test_legacy_layout_and_invalid_pointer(tmp_path):
    assert storage.resolve_gold_dir(tmp_path) == tmp_path
    (tmp_path / "current.json").write_text('{"version": "../outside"}')
    with pytest.raises(ValueError, match="Invalid"):
        storage.resolve_gold_dir(tmp_path)

"""Readers must use Delta snapshots and reject raw or broken table storage."""
import json
from unittest.mock import MagicMock

import duckdb
import pytest

from src.db import duckdb_client as database
from src.db import gold_storage as storage


def _write_parquet(path):
    path.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    try:
        conn.execute(
            "COPY (SELECT 1 AS value) TO ? (FORMAT PARQUET)",
            [str(path / "part-00000.parquet")],
        )
    finally:
        conn.close()


def _use_root(monkeypatch, root, silver):
    monkeypatch.setattr(database, "GOLD_DIR", root)
    monkeypatch.setattr(database, "SILVER_PLAYS_DIR", silver)


@pytest.mark.parametrize("layout", ["flat", "versioned"])
def test_parquet_only_gold_is_rejected(tmp_path, monkeypatch, layout):
    root = tmp_path / "gold"
    _use_root(monkeypatch, root, tmp_path / "missing-silver")
    generation = root
    if layout == "versioned":
        version = "a" * 32
        generation = root / "versions" / version
    for name in storage.GOLD_TABLES:
        _write_parquet(generation / name)
    if layout == "versioned":
        (root / "current.json").write_text(json.dumps({"version": version}))
        with pytest.raises(ValueError, match="Delta logs required"):
            storage.resolve_gold_dir(root)
    with pytest.raises(ValueError, match="Delta.*log|Delta table"):
        database.DuckDBClient(include_silver=False)


def test_parquet_only_silver_is_rejected(tmp_path, monkeypatch):
    silver = tmp_path / "silver_plays"
    _write_parquet(silver)
    _use_root(monkeypatch, tmp_path / "missing-gold", silver)
    with pytest.raises(ValueError, match="missing _delta_log"):
        database.DuckDBClient()


@pytest.mark.parametrize("log_kind", ["empty", "corrupt", "file"])
def test_invalid_delta_log_is_rejected(tmp_path, monkeypatch, log_kind):
    silver = tmp_path / "silver_plays"
    _write_parquet(silver)
    log = silver / "_delta_log"
    _use_root(monkeypatch, tmp_path / "missing-gold", silver)
    if log_kind == "file":
        log.write_text("not a directory")
        with pytest.raises(ValueError, match="missing _delta_log"):
            database.DuckDBClient()
    else:
        log.mkdir()
        if log_kind == "corrupt":
            (log / "00000000000000000000.json").write_text("invalid JSON")
        with pytest.raises(duckdb.Error):
            database.DuckDBClient()


def test_delta_reader_excludes_obsolete_files_after_overwrite(spark, tmp_path, monkeypatch):
    silver = tmp_path / "silver_plays"
    _use_root(monkeypatch, tmp_path / "missing-gold", silver)
    original = spark.createDataFrame([(1,), (2,)], "value long")
    original.coalesce(1).write.format("delta").save(str(silver))
    replacement = spark.createDataFrame([(3,)], original.schema)
    replacement.coalesce(1).write.format("delta").mode("overwrite").save(str(silver))
    client = database.DuckDBClient()
    try:
        assert client.execute_query("SELECT value FROM silver_plays") == [{"value": 3}]
        assert client.execute_query(
            "SELECT count(*) n FROM read_parquet(?)",
            [str(silver / "*.parquet")],
        ) == [{"n": 3}]
    finally:
        client.close()


def test_delta_extension_failure_is_reported_and_connection_closed(monkeypatch):
    conn = MagicMock()
    failure = duckdb.IOException("extension unavailable")
    conn.execute.side_effect = failure
    monkeypatch.setattr(database.duckdb, "connect", lambda *args: conn)
    with pytest.raises(RuntimeError, match="Delta extension") as error:
        database.DuckDBClient()
    assert error.value.__cause__ is failure
    conn.close.assert_called_once()

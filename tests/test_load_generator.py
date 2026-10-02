import json

from benchmarks.load_data import FIRST_CONTEST_ID, contest_counts, generate, validate
from pyspark.sql import functions as F
from src.etl.transform import process_bronze_to_silver


def test_contest_counts_are_deterministic_and_exact():
    counts = contest_counts(100_000, 42)
    assert counts == contest_counts(100_000, 42)
    assert sum(counts) == 100_000
    assert all(200 <= count <= 300 for count in counts)
    assert FIRST_CONTEST_ID + len(counts) < 2**31


def test_generator_resume_and_validation(tmp_path):
    first = generate(tmp_path, 1000, seed=7)
    second = generate(tmp_path, 1000, seed=7)
    assert first["valid_plays"] == second["valid_plays"] == 1000
    entries = (tmp_path / "data/bronze/ingest_manifest.jsonl").read_text().splitlines()
    assert len(entries) == first["contests"]
    assert validate(tmp_path)["validated_plays"] == 1000


def test_generated_snapshot_parses_to_exact_silver_count(spark, tmp_path):
    generate(tmp_path, 1000, seed=11)
    manifest = tmp_path / "data/bronze/ingest_manifest.jsonl"
    entries = [json.loads(line) for line in manifest.read_text().splitlines()]
    snapshots = [(entry["contest_id"], entry["ingest_timestamp"],
                  tmp_path / entry["file_path"]) for entry in entries]
    silver = process_bronze_to_silver(spark, snapshots)
    assert silver.count() == 1000
    assert silver.select("play_id").distinct().count() == 1000
    assert silver.filter(F.col("team_id").isNotNull() & (F.col("team_id") < 9_000_000)).count() == 0

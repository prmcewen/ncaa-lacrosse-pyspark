"""Immutable Gold generations with one atomic publication pointer.

Old generations are deliberately retained: in-flight readers may still use them.
The legacy flat layout remains readable until the first versioned publication.
"""
import json
import os
from pathlib import Path
import re
import shutil
from typing import Any, Mapping, Optional
from uuid import uuid4

GOLD_TABLES = ("dim_teams", "dim_contests", "fact_plays", "agg_team_game_stats")


def resolve_gold_dir(root: Path) -> Path:
    """Resolve the pointer once; callers retain this immutable path while reading."""
    pointer = root / "current.json"
    try:
        manifest = json.loads(pointer.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return root
    version = manifest.get("version") if isinstance(manifest, dict) else None
    if not isinstance(version, str) or not re.fullmatch(r"[0-9a-f]{32}", version):
        raise ValueError(f"Invalid Gold publication pointer: {pointer}")
    resolved = root / "versions" / version
    if not all(
        (resolved / f"{table}.parquet" / "_delta_log").exists()
        or any((resolved / f"{table}.parquet").rglob("*.parquet"))
        for table in GOLD_TABLES
    ):
        raise ValueError(f"Incomplete published Gold generation: {resolved}")
    return resolved


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _sync_tree(root: Path) -> None:
    for directory, _, files in os.walk(root, topdown=False):
        for name in files:
            with (Path(directory) / name).open("rb") as stream:
                os.fsync(stream.fileno())
        _fsync_directory(Path(directory))


def _write_full_table(df: Any, output: Path, partition_by: Optional[str]) -> None:
    writer = df.write.format("delta")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(str(output))


def publish_gold_tables(tables: Mapping[str, Any], root: Path) -> Path:
    """Stage and validate every table, then atomically publish the whole generation.

    A failed write or validation leaves the prior pointer untouched. A finalized
    but unpublished version is safe to retain if pointer replacement fails.
    """
    if set(tables) != set(GOLD_TABLES):
        raise ValueError(f"A Gold publication requires exactly {GOLD_TABLES}")
    versions = root / "versions"
    versions.mkdir(parents=True, exist_ok=True)
    version = uuid4().hex
    staging = versions / f".staging-{version}"
    committed = versions / version
    pointer_tmp = root / f".current-{version}.json"
    staging.mkdir()
    try:
        for name in GOLD_TABLES:
            df = tables[name]
            output = staging / f"{name}.parquet"
            partition_by = (
                "contest_id" if name in {"fact_plays", "agg_team_game_stats"} else None
            )
            _write_full_table(df, output, partition_by)
            persisted = df.sparkSession.read.format("delta").load(str(output))
            if set(persisted.columns) != set(df.columns):
                raise ValueError(f"Gold schema mismatch after writing {name}")
            persisted.limit(1).collect()
        _sync_tree(staging)
        staging.rename(committed)
        _fsync_directory(versions)
        with pointer_tmp.open("x", encoding="utf-8") as stream:
            json.dump({"version": version}, stream)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pointer_tmp, root / "current.json")
        _fsync_directory(root)
        return committed
    finally:
        # Never remove a finalized generation: the pointer may already name it.
        if staging.exists():
            shutil.rmtree(staging)
        pointer_tmp.unlink(missing_ok=True)

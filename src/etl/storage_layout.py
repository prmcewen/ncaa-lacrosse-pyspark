"""Bounded, unpartitioned Delta output with useful contest/team file statistics."""
import os
from pathlib import Path


def resolve_table_path(path: Path) -> Path:
    """Prefer the table name, while keeping older .parquet directories readable."""
    legacy = path.with_name(f"{path.name}.parquet")
    return legacy if not path.exists() and legacy.exists() else path


def migrate_table_path(path: Path) -> Path:
    """Rename a legacy Silver directory without overwriting an existing table."""
    existing = resolve_table_path(path)
    if existing != path:
        existing.rename(path)
    return path


def prepare_delta_output(df, sort_key: str, *, small_table: bool = False):
    """Group nearby keys into files without creating a directory for each key.

    The task count bounds files for a fresh write, not for a table's lifetime:
    incremental replacements can add files and retain obsolete files for history.
    """
    partitions = int(os.environ.get("LAXPXP_DELTA_WRITE_PARTITIONS", "16"))
    if partitions < 1:
        raise ValueError("LAXPXP_DELTA_WRITE_PARTITIONS must be a positive integer")
    if small_table:
        return df.coalesce(1).sortWithinPartitions(sort_key)
    return df.repartitionByRange(partitions, sort_key).sortWithinPartitions(sort_key)

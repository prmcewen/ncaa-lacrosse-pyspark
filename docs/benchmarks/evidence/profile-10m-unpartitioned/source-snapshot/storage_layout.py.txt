"""Bounded, unpartitioned Delta output with useful contest/team file statistics."""
import os


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

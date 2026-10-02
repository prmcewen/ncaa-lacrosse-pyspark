# Small-files problem: diagnosis, implementation, and results

The user identified the small-files problem and proposed its likely cause. Codex confirmed the relevant writer configuration, implemented a new layout, and tested and benchmarked the change. On the existing 10-million-play dataset, the final writer strategy reduced pipeline runtime from **32 minutes 27 seconds to 13 minutes 47 seconds**, a **2.35× speedup** in this comparison.

## What the user diagnosed

The user initiated the profiling work, inspected jobs in Spark History Server, and requested explicit pipeline labels to make jobs easier to match with code. After scaling the test to 10 million plays, the user observed that Silver storage had **one directory and file per game**, resulting in more than 40,000 small files.

The user explicitly identified this as a possible small-files problem and suggested that `partitionBy("contest_id")` was responsible. That observation and hypothesis initiated the storage-layout fix; they were not discoveries made by Codex.

The user's observation concerned the storage layout: approximately 243 plays per game were being stored in separate, very small partitions.

## What Codex confirmed

Codex checked the implementation and benchmark output and confirmed:

- The dataset contained **10,000,000 plays across 41,152 games**.
- Each Silver table had **41,152 contest directories**.
- The physical partitioning was in `_write_silver_partitions()` in `src/etl/run_pipeline.py`, rather than in the Bronze-to-Silver transformation itself.
- Gold facts and aggregates also used contest-based storage partitioning, extending the problem beyond Silver.

The original Silver writer used:

```python
df.repartition("contest_id").write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("contest_id")
```

`repartition()` distributes computation among Spark tasks. The writer's `partitionBy()` creates storage directories for individual contest IDs. Reducing shuffle partitions alone would not remove those directories.

The layout increases file creation, metadata, and filesystem operations. Gold publication also syncs its files and directories. However, the original phase timings included computation as well as I/O, so they did not establish that file overhead explained all of the slow Silver phase.

## What Codex implemented

At the user's request, Codex changed the Silver and Gold writers to use **unpartitioned Delta tables with a bounded number of sorted output files**.

- Silver, Gold facts, and Gold aggregates range-distribute rows by `contest_id` and sort within each output task. Multiple games now share a file.
- Gold dimensions use one sorted output task each: by `team_id` for teams and `contest_id` for contests.
- `LAXPXP_DELTA_WRITE_PARTITIONS` controls the output task count, defaulting to 16. The benchmark CLI exposes it as `--write-partitions`.
- Incremental Silver updates continue to use Delta `replaceWhere`, now targeting the `contest_id` data column. Rows belonging to unaffected games are preserved even when they share a file with an updated game.
- Existing partitioned Silver tables require `--full-refresh` to migrate. Incremental writes against the legacy layout fail with a migration instruction.

The game-specific `Window.partitionBy()` calculations were preserved. They define analytical groups and do not create storage directories.

### Additional issue found during testing

Codex's first diagnostic run exposed an extra cost introduced by range distribution: range sampling evaluated the expensive Silver transformation before another pass over that lineage for the write.

Codex stopped that diagnostic run and added **temporary `DISK_ONLY` persistence during Silver writes**. Sampling and writing can then reuse the computed rows. The writer releases its temporary cache on success or failure and preserves caches owned by callers.

The stopped attempt was retained separately and excluded from the final timing comparison. The final solution therefore combines a file-layout change with temporary Silver caching; its speedup should not be attributed solely to removing contest directories.

## What Codex tested and benchmarked

Codex ran **16 focused regression checks**, then reran **four checks after adding disk persistence**. The latter were rechecks, not four additional unique tests. Coverage included:

- Updating a game in a file shared with another game, preserving the unaffected game's rows.
- Removing stale play rows when a game's replacement contains fewer plays.
- Rejecting incoming rows outside the replacement predicate without changing committed data.
- Migrating legacy partition metadata and verifying bounded fresh-write file counts.
- Publication failures, retry behavior, and DuckDB/API reader consistency.
- Releasing the writer's temporary cache after successful and failed writes.

For the final benchmark, Codex reused the **exact existing Bronze input** through a symlink and wrote Silver and Gold to a separate benchmark directory. The original baseline output was preserved. Both runs used **8 local workers, an 8 GiB driver heap, and 128 shuffle partitions**; the new writer used 16 output partitions.

After timing finished, Codex compared the resulting datasets against the baseline. Both contained 10 million unique fact play IDs and 41,152 games, with matching counts for every game. Full-row hash fingerprints matched for Silver, dimensions, and facts. All aggregate values matched, using a relative/absolute tolerance of `1e-12` for floating-point values.

## Measured results

| Phase | Original layout | Final writer strategy |
|---|---:|---:|
| Entire pipeline | 32m 27s | **13m 47s** |
| Silver plays | 15m 35s | **12m 08s** |
| Silver contest metadata | 2m 03s | **32s** |
| Gold execution and publication | 13m 00s | **36s** |

| Table | Original active files | New active files |
|---|---:|---:|
| Silver plays | 41,152 | 16 |
| Silver contests | 41,152 | 16 |
| Gold team dimension | 128 | 1 |
| Gold contest dimension | 128 | 1 |
| Gold facts | 41,152 | 16 |
| Gold aggregates | 77,020 | 16 |
| **Total** | **200,732** | **66** |

These counts describe **active Delta data files**, excluding Bronze JSON, transaction logs, and obsolete files retained for history. The largest measured phase improvement was Gold execution and publication, at approximately **21.7× faster**.

## Interpreting the results

This was one before/after measurement, not a repeated cold-cache experiment. Filesystem cache and other machine activity can affect timings. The 16-file setting bounds fresh writes, not a table's lifetime file count: incremental updates may rewrite shared files and add files over time. Delta can retain obsolete files after migration, and Bronze still contains one JSON file per game.

The measurements and validation artifacts for this fix are under `benchmarks/output/profile-10m-unpartitioned/`.

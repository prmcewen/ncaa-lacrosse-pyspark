# 10-million-play Delta layout comparison

Both runs use the same 10,000,000 plays in 41,152 games (seed 1729), 8 local workers, an 8 GiB Spark driver heap, and 128 shuffle partitions. The new writer uses 16 range-sorted output tasks for Silver/facts/aggregates and one for each Gold dimension. Silver transformations are temporarily persisted with DISK_ONLY storage during the write to avoid re-evaluation during range sampling; the cache is then released. Bronze files are shared through a symlink; Silver and Gold outputs are isolated.

| Phase | Contest-partitioned baseline (s) | Unpartitioned output (s) | Speedup |
|---|---:|---:|---:|
| End-to-end wall time | 1946.91 | 826.91 | 2.35× |
| Silver plays | 934.81 | 728.16 | 1.28× |
| Silver contests | 122.76 | 31.75 | 3.87× |
| Dimensions | 30.92 | 2.55 | 12.11× |
| Silver read / cache registration | 2.61 | 0.65 | 4.00× |
| Gold plan construction / key checks | 67.18 | 19.85 | 3.39× |
| Gold execution / publication | 779.53 | 35.92 | 21.70× |

Phase timings follow the existing runner: lazy work executes in downstream actions, and Gold publication includes computations, Delta writes, validation, and filesystem sync. End-to-end wall time includes startup/shutdown.

| Table | Rows | Baseline active files | New active files | Baseline data MiB | New data MiB |
|---|---:|---:|---:|---:|---:|
| silver_plays | 10,000,000 | 41,152 | 16 | 854.25 | 253.48 |
| silver_contests | 41,152 | 41,152 | 16 | 153.57 | 1.47 |
| dim_teams | 1,024 | 128 | 1 | 0.26 | 0.03 |
| dim_contests | 41,152 | 128 | 1 | 0.57 | 0.36 |
| fact_plays | 10,000,000 | 41,152 | 16 | 882.78 | 265.08 |
| agg_team_game_stats | 82,304 | 77,020 | 16 | 435.78 | 1.98 |

## Correctness

Both layouts contain exactly 10,000,000 unique fact play IDs and 41,152 games. Every game's fact count matches. Full-row hash fingerprints match for both Silver tables, dimensions, and facts. All aggregate values match, with relative/absolute tolerance 1e-12 for floating-point values. All new tables have no storage partition columns. Sixteen focused regression checks passed, followed by four checks rerun after adding scoped disk persistence, including selective replacement of games sharing a file, rejected out-of-predicate writes, layout migration, publication failure/retry, and DuckDB/API reader consistency.

## Remaining bottleneck

In the new run, Silver Stage 4 takes 651.30 seconds across 128 tasks, with 4,940.92 seconds of aggregate task CPU time out of 5,164.27 seconds of aggregate executor run time. It reports zero disk spill, compared with 428,916,513 bytes in the baseline stage. This compute-heavy Silver stage now accounts for approximately 79% of end-to-end wall time. The next profiling target is its native text parsing and game-window calculations. Aggregate task CPU/run times are summed across parallel tasks; they are not wall-clock durations. Full stage metrics are saved in `spark-stage-summary.json`.

## Reproduce

```bash
uv run python -m benchmarks.load_test run-etl \
  --run-root benchmarks/output/profile-10m-unpartitioned \
  --workers 8 --driver-memory 8g --shuffle-partitions 128 \
  --write-partitions 16
```

The run root contains `benchmark-provenance.json`, `implementation.patch`, `run-etl-result.json`, `run-etl-stages.json`, `layout-validation.json`, the validation scripts, the execution plan, and Spark event logs. The existing Bronze input must be present before reproducing.

## Limits

This is one before/after measurement on the same local machine, not a repeated cold-cache experiment. The baseline was recorded earlier; filesystem cache and other machine activity can affect timing. The change applies to both Silver and Gold and introduces temporary Silver disk persistence, so the total improvement cannot be attributed to removing contest directories alone. A preliminary attempt without that cache was stopped after event logs showed range sampling re-evaluating the expensive Silver lineage; its logs are retained separately and it is excluded from timing comparisons. Incremental writes may rewrite shared files and add files over time; the output task count bounds fresh writes, not lifetime file counts. Migrating existing Silver requires `--full-refresh`, and Delta retains obsolete files for history. Bronze still contains one JSON file per game.

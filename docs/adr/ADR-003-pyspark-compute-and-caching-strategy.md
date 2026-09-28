# ADR-003: Local Spark Execution, Caching, and Skew Checks

## Status

Accepted; updated to distinguish the production pipeline from reusable write helpers.

## Context

The pipeline reuses Silver data for fact enrichment and analytical aggregates. Its local execution plan needs bounded configuration, reduced repeated work, and checks that identify uneven data distributions before publishing Gold. Performance claims must be separated from the optimizations actually implemented.

## Decision

### Local execution

[spark_session.py](../../src/etl/spark_session.py) uses `local[*]`, four shuffle partitions, default parallelism of four, a configured 4 GB driver heap, adaptive query execution, whole-stage code generation, and Arrow support. Spark UI and console progress are disabled. These are local configuration choices, not a distributed cluster deployment or a resource guarantee.

Java is external to the Python environment. When `JAVA_HOME` is unset, the Spark helper dynamically resolves a Java 17+ runtime via `PATH` (using `shutil.which`), standard user-local JVM directories (`~/.local/share/jvm/temurin-17-jre` and user glob patterns), or system installations (`/usr/lib/jvm/default-java`, `/usr/lib/jvm/java-17-openjdk-amd64`), and automatically configures `JAVA_HOME` and prepends the binary directory to `PATH`. Python and package versions are controlled by [pyproject.toml](../../pyproject.toml), [.python-version](../../.python-version), and [uv.lock](../../uv.lock).

### Actual cache lifecycle

[run_pipeline.py](../../src/etl/run_pipeline.py) first writes pending Silver partitions. It then rebuilds dimensions from current Bronze snapshots, caches both dimension DataFrames, and explicitly counts them to materialize their caches.

The pipeline reads Silver back using Delta Lake format (`spark.read.format("delta").load(...)`), falling back to Parquet only if the Delta transaction log is missing, and passes that DataFrame to `cache_and_profile()`, which defaults to lazy `MEMORY_AND_DISK` persistence. The following skew-check action materializes it before downstream fact and aggregate work. The pipeline does not cache the original Bronze-to-Silver parsing DAG for reuse across Silver writes and dimension extraction.

A `finally` block calls `unpersist_dataframe(..., blocking=False)` for Silver and both dimensions, then stops Spark. Unpersist failures are logged. This is explicit cleanup with nonblocking eviction, not a promise that all memory is reclaimed synchronously.

### Broadcast joins

The implementation broadcasts the small team lookup during fact enrichment, snapshot timestamp lookups where used, and shot-result baseline rates when joining them to plays. `dim_contests` is published and queried separately; it is not broadcast into the fact table.

Broadcasting avoids repartitioning the large side for those joins. Window operations, groupings, dimension deduplication, and explicit repartitioning still require exchanges or sorts. The enriched fact join is constructed in both `generate_gold_tables()` and the runner, whose result becomes the published fact table; the code does not guarantee zero redundant computation.

### Storage layout (Delta Lake)

Silver is repartitioned by `contest_id` and written using Delta Lake format (`.format("delta").partitionBy("contest_id")`) with Snappy compression. This co-locates each contest's rows, records atomic partition transactions in `_delta_log/`, and supports targeted partition replacement.

Gold publication writes four unpartitioned Delta Lake tables (`dim_teams`, `dim_contests`, `fact_plays`, `agg_team_game_stats`) in a new generation. Each table maintains its own `_delta_log/` transaction log, and DuckDB readers query them via native `delta_scan`. Publication and reader isolation are described in [ADR-001](ADR-001-medallion-storage-and-audit-trail.md).

### Skew validation in the runner

`check_data_skew()` summarizes counts by logical keys, or by nonempty physical Spark partitions when no keys are supplied. It reports minimum, maximum, mean, standard deviation, max/mean ratio, and Spark's skewness statistic. Optional thresholds can check row-count skew, an absolute row limit, or absolute skewness.

The runner applies:

| Dataset | Grouping key | Maximum max/mean ratio | Maximum rows per key |
| --- | --- | --- | --- |
| Materialized Silver | `contest_id` | 2.5 | 2,000 |
| Team-game aggregates | `team_id` | 3.5 | Not configured |

The Silver check runs **after** pending Silver writes. Both checks run before Gold publication. A violation raises `DataSkewError` with diagnostics; it does not roll Silver back, detect duplicate play IDs, or prove that physical task skew has been eliminated. Dimension-key validation is a separate check before fact joins.

### Reusable partitioned-write helper

[optimizations.py](../../src/etl/optimizations.py) also exposes `save_partitioned_parquet()`, which is covered by tests but is not called by the current runner:

- By default, a cached input is checked before writing; an uncached input is checked by reading the written files afterward.
- `post_write_skew=True` forces a post-write check. `False` allows only a cached pre-write check; an uncached input then skips checking and logs a warning.
- `check_skew=False` disables these checks.
- A failed post-write check deletes the entire output path. This is destructive cleanup, not transactional rollback: previous data at that path can be lost, including when appending. It is distinct from the versioned Gold publisher, which preserves prior generations.

### Plan artifact and performance evidence

`save_explain_plan()` saves an extended plan for the runner's enriched-facts DataFrame to [spark_execution_plan.md](../spark_execution_plan.md). Because the runner reads Silver from Parquet before enrichment, this artifact does not necessarily contain the earlier Bronze parsing and score-window stages. It is not an execution trace or benchmark for the whole pipeline.

No maintained benchmark establishes a fixed caching speedup, sub-millisecond API latency, or zero-shuffle execution. Cache reuse, broadcast joins, and partition pruning are optimization mechanisms; their benefit depends on data, runtime, and workload.

## Consequences

- Reusing materialized Silver reduces repeated Parquet reads across downstream branches, at the cost of cache memory and disk use.
- Rebuilding and caching dimensions costs an additional Bronze scan but makes metadata updates and failed-run retries deterministic.
- Logical skew checks can stop an unsuitable Gold publication after Silver has changed; operators can inspect the reported distributions before retrying.
- Gold reader safety comes from immutable generations and request-level snapshot selection, not from caching or skew checks.

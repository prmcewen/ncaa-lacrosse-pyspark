# ADR-003: Local Spark Execution and Persistence Strategy

## Status

Accepted; updated to describe the current local execution path.

## Context

The pipeline incrementally writes changed Bronze snapshots to Silver, then uses the current Silver data to build a complete Gold generation. Its local execution plan needs bounded configuration and a way to reuse Silver while facts and aggregates are built. Performance claims must be separated from the optimizations actually implemented.

## Decision

### Local execution

[spark_session.py](../../src/etl/spark_session.py) uses `local[*]`, a default 16 GiB driver heap, a 4 GiB maximum result size, default parallelism of 16, and 32 shuffle partitions. The `LAXPXP_SPARK_DRIVER_MEMORY`, `LAXPXP_SPARK_DEFAULT_PARALLELISM`, and `LAXPXP_SPARK_SHUFFLE_PARTITIONS` environment variables override those values. Arrow support and Spark UI and console progress are enabled. Adaptive query execution is enabled with partition coalescing, Spark's adaptive skew-join handling, and a 128 MiB advisory partition size. These are local configuration choices, not a distributed cluster deployment or a resource guarantee.

Java is external to the Python environment. When `JAVA_HOME` is unset, the Spark helper dynamically resolves a Java 17+ runtime via `PATH` (using `shutil.which`), standard user-local JVM directories (`~/.local/share/jvm/temurin-17-jre` and user glob patterns), or system installations (`/usr/lib/jvm/default-java`, `/usr/lib/jvm/java-17-openjdk-amd64`), and automatically configures `JAVA_HOME` and prepends the binary directory to `PATH`. Python and package versions are controlled by [pyproject.toml](../../pyproject.toml), [.python-version](../../.python-version), and [uv.lock](../../uv.lock).

### Native parsing and Silver updates

[transform.py](../../src/etl/transform.py) reads Bronze snapshots with Spark's native JSON reader and explicit schemas. It does not route parsing through Python UDFs or a separate parser. The runner compares manifest-selected `(contest_id, ingest_timestamp)` pairs with each Silver table and processes only pending snapshots. Both Silver Delta tables are partitioned by `contest_id`; a full refresh overwrites them, while regular updates replace the pending contest partitions.

### Persistence lifecycle

[run_pipeline.py](../../src/etl/run_pipeline.py) rebuilds the dimensions from current selected Silver contest rows. It reads the Silver plays Delta table, filters to selected snapshots, and directly calls `persist()` on that DataFrame so fact enrichment and aggregate generation can reuse it. The dimensions use Spark DataFrame `cache()`. These operations are lazy; downstream Spark actions materialize the data as needed.

A `finally` block calls `unpersist_dataframe(..., blocking=False)` for Silver and both dimensions, then stops Spark. Unpersist failures are logged. Cleanup requests nonblocking eviction; it does not promise that all memory is reclaimed synchronously.

### Broadcast joins

The implementation broadcasts the small team lookup during fact enrichment, snapshot timestamp lookups where used, and shot-result baseline rates when joining them to plays. `dim_contests` is published and queried separately; it is not broadcast into the fact table.

Broadcasting avoids repartitioning the large side for those joins. Window operations, groupings, dimension deduplication, and explicit repartitioning still require exchanges or sorts. The fact enrichment join is constructed in `generate_gold_tables()`; the runner publishes the complete result.

### Storage layout (Delta Lake)

Both Silver tables are partitioned by `contest_id` and written using Delta Lake format (`.format("delta").partitionBy("contest_id")`) with Snappy compression. Regular Silver runs replace only changed contest partitions, while `--full-refresh` rewrites all selected snapshots.

Every pipeline run rebuilds and writes complete Gold DataFrames into a fresh generation. The fact and aggregate Delta tables are partitioned by `contest_id` to organize their output files; the publisher does not reuse prior files or replace only changed Gold partitions. Each table maintains its own `_delta_log/` transaction log, and DuckDB readers query the published tables via native `delta_scan`. Publication and reader isolation are described in [ADR-001](ADR-001-medallion-storage-and-audit-trail.md).

### Plan artifact and performance evidence

`save_explain_plan()` saves an extended plan for the runner's enriched-facts DataFrame to [spark_execution_plan.md](../spark_execution_plan.md). Because the runner reads Silver from Delta Lake before enrichment, this artifact does not necessarily contain the earlier Bronze parsing and Silver-write stages. It is not an execution trace or benchmark for the whole pipeline.

When `LAXPXP_BENCH_METRICS` is set, the runner records stage and total wall-clock timings as JSON at that path. The plan artifact and stage timings describe the observed run; they do not establish fixed caching speedups, API latency, or shuffle-free execution. Cache reuse and broadcast joins are optimization mechanisms whose benefits depend on data, runtime, and workload.

## Consequences

- Incremental Silver writes avoid reparsing snapshots that are already current, while Gold publication pays the cost of writing all four tables on every successful run.
- Persisting Silver allows both Gold branches to reuse it, at the cost of Spark memory and disk use.
- The complete Gold rebuild recalculates dimensions and aggregate rates from current Silver, and a failed publication leaves the previous generation available.
- Gold reader safety comes from immutable generations and request-level snapshot selection.

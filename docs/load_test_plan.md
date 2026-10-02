# 50–100 million play load test plan

## Goal and scope

Prepare this repository to measure its Bronze JSON → Silver Delta → Gold Delta pipeline, a Silver → Gold rebuild, and the REST/GraphQL API at **50 million and 100 million valid plays** on the local Ryzen 7 7730U (16 logical CPUs, 32 GiB installed RAM). Implement the generator and benchmark tools, then run only small calibration pilots. **Do not generate or run the 50 million or 100 million play workloads as part of this work.**

The existing 20 Bronze fixtures contain 4,854 nonblank plays, averaging **242.7 valid plays per contest** (sample standard deviation 19.1; range 210–282). At that density, the target is about **206,000 contests at 50 million** and **412,000 at 100 million**. One snapshot per contest is enough for the initial full run. The generator will aim for an exact count of valid plays, because blank play text is discarded by the Silver transform.

## Synthetic data design

1. Add a deterministic, resumable generator with a seed, target valid-play count, output root, and optional fixture set. Stream one compact NCAA-shaped JSON snapshot per contest into `data/bronze/contest_id=<id>/ingest_timestamp=<timestamp>.json`; append its `STORED_NEW_VERSION` manifest entry only after the snapshot is complete. Use synthetic IDs in a reserved range and a fixed timestamp for reproducible runs. Do not fetch the NCAA API or overwrite the real `data/` tree.
2. Sample contest play counts from a truncated normal distribution centered on the observed 243, with standard deviation near 19 and bounds of 200–300. Adjust the final few contests within those bounds to hit the target exactly. Use the 20 local games as event-pattern templates; preserve realistic proportions of shots, goals, clears, turnovers, ground balls, faceoffs, penalties, timeouts, and period endings. Include the occasional overtime game at roughly the fixture rate. Keep period clocks descending and score updates consistent with goal events.
3. Generate a bounded, reused team pool and contest-local player rosters. Rewrite team IDs, team names/short codes, and player names consistently in metadata and play text so existing parsing and attribution logic still exercises its normal paths. Vary fixture selection, team pairings, rosters, and event sequences deterministically to avoid millions of identical games. Keep every game normal sized; use no deliberately oversized contests.
4. Write a small sidecar summary per run: seed, code revision, fixture set, target/actual valid-play count, contest count, event counts, file count, and total Bronze bytes. Make generation resumable by contest ID, with atomic file replacement and manifest reconciliation so a killed generator cannot leave a selected partial snapshot.

The generator is outside ETL timing. Before a large run, validate a sample of generated snapshots with Spark's native JSON reader and compare their event mix and attribution/null rates to the source fixtures. The validation also checks unique `(contest_id, play_seq)`/`play_id`, legal periods and clocks, team references, and exact generated nonblank play count.

## Isolated benchmark workspace

Use `benchmarks/output/<run-id>/data/{bronze,silver,gold}` and an explicit data-root setting shared by the ETL and API. Keep the current repository `data/` as the default. The run directory and Spark event logs will be ignored by Git. Each 50 million and 100 million run gets its own root; the benchmark runner records the root and does not delete data automatically. The manifest paths remain relative to that run root in the same `data/bronze/...` form used today.

Provide commands for `generate`, `validate`, `run-etl`, `run-silver-gold`, `run-api`, and `report`. The commands should accept `--seed`, `--target-plays`, `--data-root`, and a run ID where appropriate, and print the exact configuration used. `run-silver-gold` starts from a materialized Silver snapshot in the same isolated root and writes a fresh Gold generation; it does not include Bronze parsing in its timed interval.

## Measurements needed before the large runs

- **Spark sizing:** Expose driver heap, shuffle partitions, and local worker count as benchmark settings. The current 16 GiB heap and 32 shuffle partitions are starting values, not assumed winners. Pilot 128, 256, and 512 shuffle partitions and choose by task size, spill, and runtime; record the final setting for both target sizes. Keep memory for the OS, DuckDB, and filesystem cache.
- **High-cardinality partition layout:** Current Silver plays and Gold facts are physically partitioned by `contest_id`. At 206,000–412,000 contests, that implies many contest directories and files in each fact layer and may overwhelm Delta metadata, file listing, and publication. Pilot the current layout first to quantify this. If file count or Delta metadata dominates, implement a benchmarked scale-ready layout (for example, a stable contest bucket with clustered contest IDs) and update Silver partition replacement, complete Gold writes, and DuckDB reads to preserve correctness. Report current-layout and scale-ready results separately; do not quietly change the measured architecture midway through a run.
- **Driver-side contest lists:** Snapshot selection and pending detection currently materialize contest IDs on the driver for incremental Silver writes. Profile their time and memory at each ramp size. Replace unbounded lists or large `replaceWhere` predicates with batched or Spark-side joins when the pilot shows they are a bottleneck. Verify that unchanged and changed-contest runs still select the correct snapshots.
- **Full Gold publication cost:** Every pipeline run writes complete dimensions, facts, and aggregates from current Silver into a new Delta generation. Track row and file counts, metadata size, write and validation time, sync duration, and total disk growth; avoid benchmark runs that exhaust disk or inode space. Preserve the atomic publication pointer and reader consistency checks when changing layout or batching. This plan adds no task-skew checks or skew-specific tests.

Any required scale changes will have focused correctness tests before timing the 50 million play run. The report will identify which changes were enabled; an unmodified current-layout pilot remains the baseline.

## Future run matrix (tools prepared now; large workloads not executed)

| Stage | Workload | What is timed |
| --- | --- | --- |
| Calibration | 100,000 and 1 million valid plays; expand only if needed to select settings | Generator speed/bytes separately; full ETL stages, file growth, memory, spill, and extrapolated disk need |
| Full ETL | 50 million, then 100 million | Manifest/snapshot selection, Bronze parse and Silver write, Silver read/persist, Gold build/write/validation/publication |
| Silver → Gold | Both target sizes, fresh Gold root | Read existing Silver, dimensions, facts/aggregates, Gold publication; excludes generator and Bronze parse |
| Silver update | Change 1% of contests; separate metadata-only update; no-op run | End-to-end update time, Silver partitions rewritten, complete Gold rebuild time, published generation correctness |
| API | Published Gold at both sizes | Point queries, filtered scans, global analytics, listings, REST and equivalent GraphQL queries |

The play update will change selected snapshots while keeping the same contest and play counts; Silver should rewrite only those contests and Gold should still rebuild completely. The metadata-only update will change team metadata for selected contests and measure the resulting Silver update and complete Gold rebuild. A no-op run measures the cost of a complete Gold rebuild when Silver has no pending snapshots.

For API tests, benchmark contest summary and first-page plays, selective event/team/player filters, a broad filtered play query, global shooting efficiency, and `/contests` as an intentionally full-result listing. Run single-client cold and warm samples, then a reproducible mixed request stream at concurrency 1, 4, 8, and 16. Use the same fixture-derived IDs and filters across REST and GraphQL, and record response size so a large listing is not compared with a small point query as if they were equivalent. Measure p50/p95/p99 latency, requests/s, timeouts/errors, and API/DuckDB memory. Preserve request-level Gold version consistency during a concurrent publication test.

## Measurements and gates

Emit one machine-readable result per run plus a short Markdown report. Include git revision, seed, target and actual rows, contest count, Spark/Java/Python versions, CPU/RAM settings, available disk/inodes, and the Spark configuration. Time each ETL stage with monotonic wall clocks. Enable Spark event logs and collect job/stage/task time, shuffle read/write, spill, GC, peak process RSS, and disk footprint by layer and Gold version. Report plays/second for full ETL and Silver → Gold separately.

Before scaling to the next size, verify exact Silver and Gold fact counts against the generator summary; unique play IDs; expected contest and dimension counts; representative event/aggregate totals; no missing published table; and API spot checks against direct DuckDB queries. A failed correctness check or incomplete publication stops the ramp. The 10 million play pilot estimates total footprint from measured Bronze/Silver/Gold and temporary spill, then proceeds only with enough space for the next target plus **at least 100 GiB free**. This machine currently reports about 302 GiB free, but the pilot determines whether 100 million plays fits after file and version overhead. Do not set an arbitrary speed pass mark before observing the baseline; the primary outcome is a reproducible capacity curve and the limiting stage.

## Current delivery scope

1. Implement isolated paths, generator, validation, and benchmark commands.
2. Measure Silver incremental writes and full Gold rebuilds, then profile remaining scale bottlenecks.
3. Run small pilots and choose initial Spark settings and a physical-layout recommendation based on measured file count, spill, and time. Deliver the pilot results and exact commands needed for a later 50–100 million play run.

The 50 million and 100 million play generations, ETL runs, Silver → Gold runs, and API load tests are outside the current execution scope. The tools should support those later runs without requiring another generator redesign.

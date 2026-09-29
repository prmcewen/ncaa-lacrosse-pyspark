# ADR-001: Medallion Storage, Snapshot Tracking, and Gold Publication

## Status

Accepted; updated to describe the current local-filesystem implementation.

## Context

Game feeds can change during play or after statistical corrections. The pipeline needs to track fetched payloads, replace affected Silver partitions, update dimensions, and publish a coherent set of analytical tables. The implementation uses local files and embedded DuckDB; it does not provide object-store transactions or compliance certification.

## Decision

### Bronze snapshot tracking

[ingest.py](../../src/ingestion/ingest.py) fetches the NCAA persisted GraphQL query over HTTP with a 30-second timeout and performs basic structural checks. Canonical JSON uses sorted object keys and compact separators before SHA-256 hashing. Array order is preserved.

The hash is compared with the last `STORED_NEW_VERSION` entry for the contest in `data/bronze/ingest_manifest.jsonl`. A matching payload returns `UNCHANGED` without writing a snapshot or manifest record. Otherwise the code writes:

```text
data/bronze/contest_id=<id>/ingest_timestamp=<YYYYMMDDTHHMMSSZ>.json
```

It then appends `contest_id`, `ingest_timestamp`, `sha256`, `file_path`, `status`, `game_status`, and `periods_count` to the manifest. Snapshot and manifest writes are separate operations.

ETL snapshot selection scans manifest order and retains the last stored record whose file exists for each contest. It does not sort by timestamp, recheck stored hashes, or validate file contents during selection. Malformed JSON in the manifest can stop this scan.

### Silver incremental materialization

[run_pipeline.py](../../src/etl/run_pipeline.py) compares selected `(contest_id, ingest_timestamp)` pairs independently with Silver plays and Silver contests. Pending contests are written as Delta Lake tables (Snappy-compressed Parquet data files + `_delta_log/` transaction logs) under `data/silver/`, partitioned by `contest_id`. `silver_plays.parquet` contains play rows; `silver_contests.parquet` contains one row per contest with nested team metadata. The contest extraction uses a narrow JSON schema that excludes period plays.

The initial write and `--full-refresh` use static overwrite. Other updates use Delta Lake partition replacement (`replaceWhere` or dynamic overwrite). Missing/unreadable Silver causes snapshot processing to be retried. Code changes alone do not make a snapshot pending; a full refresh is required when Silver transformation logic changes.

Silver writes occur before Gold validation and publication. Silver is a working dataset, not part of the Gold publication transaction.

### Current dimensions

[transform.py](../../src/etl/transform.py) rebuilds both dimensions from selected `silver_contests` rows on every pipeline run. The runner backfills missing Silver contest rows from Bronze, and retries after a failed Gold publication read the already committed Silver data. Selected Silver rows must contain exactly one row per current contest snapshot and nonempty team metadata.

Each dimension key selects a row by descending snapshot timestamp, then descending source contest ID, then ascending metadata fields with nulls last. `dim_teams` is unique on `team_id`; `dim_contests` is unique on `contest_id`. Null or duplicate keys are rejected before Gold fact enrichment. Rebuilding avoids arbitrary union/deduplication winners and stale metadata from a previous publication.

### Gold publication and readers

[gold_storage.py](../../src/db/gold_storage.py) requires these four tables:

- `dim_teams`
- `dim_contests`
- `fact_plays`
- `agg_team_game_stats`

Publication uses a fresh UUID and writes Delta Lake tables under `data/gold/versions/.staging-<version>/`. Facts and aggregates are partitioned by contest. The publisher copies prior transaction metadata and hard-links immutable data files into staging, then replaces changed contest partitions. Unchanged tables are reused. A legacy unpartitioned table is fully rewritten during migration. It opens each written table via Delta, checks column-name sets, and collects at most one row. It then syncs files/directories, renames the staging directory to the final version, and atomically replaces `data/gold/current.json` with a synced JSON pointer of the form `{"version": "<32-character UUID hex>"}`. This relies on local POSIX filesystem rename and directory-sync behavior. DuckDB readers scan Gold tables directly using DuckDB's native Delta Lake extension (`delta_scan`).

A write or validation failure before the pointer switch leaves the prior publication visible. On a changed shot-retention profile, all aggregate partitions are refreshed because normalized rates use global baselines. An unchanged run skips publication. Handled failures clean up staging and temporary pointer files. Finalized versions are retained even if pointer replacement fails; an abrupt process crash may leave additional staging files. Publication does not automatically delete old versions.

`resolve_gold_dir()` resolves one immutable generation. When no pointer exists, it supports the legacy flat Gold layout; a malformed pointer or a generation missing any required table is rejected instead of silently falling back.

[API dependencies](../../src/api/dependencies.py) create one `DuckDBClient(include_silver=False)` per REST/GraphQL request and close it afterward. The client resolves Gold once; all queries and nested GraphQL resolvers in that request share the same version. Each query uses its own cursor and closes it after fetching. New requests see a new publication while existing readers continue using retained files.

Standalone clients also pin Gold at construction. Their default `include_silver=True` additionally exposes mutable Silver and permits a Silver fact fallback when Gold facts are absent; those reads do not have Gold's publication consistency.

## Consequences and limits

- Updated metadata cannot create multiple dimension rows per key or multiply fact rows through the team join.
- API requests see a complete Gold version, while a failed pre-publication run leaves the previous version usable.
- Silver contest backfills and updates read only changed Bronze snapshots; retaining Gold generations costs disk space. Retention is manual and must account for active readers. Recursive scans of the entire Gold root can double-count versions.
- Bronze timestamps have second precision and files are opened for overwrite. Two changed payloads for one contest in one second can collide. There is no lock or transaction spanning the snapshot and manifest, so this is snapshot tracking rather than guaranteed immutable audit storage.
- Gold publication does not make concurrent ingestion or Silver updates transactional, and validation is not a full semantic audit of every published row.

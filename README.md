# NCAA Lacrosse Play-by-Play Data Platform

A local data pipeline and analytics API for NCAA men's lacrosse play-by-play data. Python ingestion writes JSON snapshots, PySpark produces Silver and Gold Delta Lake datasets, and DuckDB serves queries through FastAPI REST endpoints and Strawberry GraphQL using DuckDB's native Delta Lake extension.

The implementation runs Spark locally with Delta Lake extensions and stores data on the local filesystem. It does not implement a cloud deployment, authentication, or a compliance certification. Dependency constraints are in [pyproject.toml](pyproject.toml); [uv.lock](uv.lock) records the resolved versions.

## Architecture

```text
NCAA API / sdataprod
  |
  v
Bronze JSON snapshots + ingest_manifest.jsonl
  |
  +--> changed snapshots --> PySpark --> Silver plays, partitioned by contest_id
  |
  +--> changed metadata snapshots --> Silver contests, partitioned by contest_id
                                      |
                  Silver contests --> Gold team and contest dimensions
                  Silver plays ------> Gold facts and aggregates
                                      |
                                      v
                     Gold Delta tables in a new version directory
                                      |
                         atomic current.json replacement
                                      |
                     DuckDB snapshot per API request (via delta_scan)
                                      |
                          REST /api/... and /graphql
```

| Layer | Location | Behavior |
| --- | --- | --- |
| Bronze | `data/bronze/contest_id=<id>/ingest_timestamp=<timestamp>.json` | Canonical SHA-256 comparison skips unchanged payloads; changed payloads append a manifest record. |
| Silver plays | `data/silver/silver_plays.parquet/` | Delta Lake format (partitioned by `contest_id`); incremental processing updates changed contest partitions. Features 34 approved play columns. |
| Silver contests | `data/silver/silver_contests.parquet/` | One row per selected contest snapshot, including contest metadata and a nested team array. Changed contest partitions are replaced independently of Silver plays. |
| Gold | `data/gold/versions/<version>/<table>.parquet/` | Complete immutable generations rebuilt from current Silver on every pipeline run; fact and aggregate Delta tables are partitioned by `contest_id`. |
| Publication pointer | `data/gold/current.json` | Names the complete Gold version used by new readers. |

Gold contains `dim_teams`, `dim_contests`, `fact_plays`, and `agg_team_game_stats`. Dimensions have one row per team or contest key. `fact_plays` enriches the 34 Silver play columns with team color and full name. The aggregate table groups by `(contest_id, team_id, event_team_short)`, renaming the short-code column to `team_short`; multiple aliases within one contest can therefore produce separate aggregate rows. Alongside advanced shooting efficiency, it aggregates game-level totals for saves faced, ground balls, turnovers, successful and failed clears, and penalty minutes.

## Quickstart

Run commands from the repository root. The project accepts Python 3.11–3.12, and `.python-version` selects 3.12. Install `uv` and a Java runtime for Spark, and configure `JAVA_HOME`. `uv sync` installs Python dependencies, not Java. The Spark helper dynamically resolves Java via `JAVA_HOME`, `PATH`, or standard user/system JVM directories when `JAVA_HOME` is unset. Gold publication uses POSIX filesystem operations, including directory `fsync`.

```bash
uv sync --locked

# Fetch one example contest
uv run python -m src.ingestion.ingest --contest-id 6599996

# Skips storage only if the newly fetched payload has the same hash
uv run python -m src.ingestion.ingest --contest-id 6599996

# Update changed Silver partitions, then rebuild all Gold tables
uv run python -m src.etl.run_pipeline

# Rebuild all Silver partitions as well
uv run python -m src.etl.run_pipeline --full-refresh

uv run uvicorn src.api.main:app --port 8000 --reload
```

Use `--full-refresh` after changing Silver transformation logic. Each Silver table independently compares `(contest_id, ingest_timestamp)` with selected Bronze snapshots; this does not detect code changes. A missing Silver contests table is backfilled on the next run. Every pipeline run builds all four Gold tables from current Silver and publishes them as a new generation, even when Silver has no changed snapshots. This full rebuild also keeps team metadata and global shot-retention baselines consistent across facts and aggregates.

- [REST documentation](http://localhost:8000/docs)
- [GraphQL interface](http://localhost:8000/graphql)
- `GET /api/health` reports application liveness; it does not verify that data has been published.

### Tests

The full suite requires Java/Spark, the multi-contest Bronze fixtures, and generated Silver/Gold data. Several integration tests expect specific contests and players; fetching only the example contest above does not populate all required fixtures.

Pipeline write tests use temporary datasets. Tests of stored game data read the existing repository fixtures.

```bash
uv run pytest -v
```

Regression coverage includes faceoff attribution, concurrent queries, deterministic pagination, dimension corrections, failed Gold writes, retry recovery, and request-level publication consistency.

#### Testing Dependencies (`httpx` vs. `httpx2`)

Both `httpx` and `httpx2` are intentionally declared in [pyproject.toml](pyproject.toml):
- `httpx` is used for runtime HTTP data ingestion in [`src/ingestion/ingest.py`](src/ingestion/ingest.py).
- `httpx2` is required by `starlette.testclient.TestClient` (re-exported and used through `fastapi.testclient`) in Starlette 1.6.0+. In these releases, Starlette's test client explicitly checks for and prefers `httpx2`, emitting `StarletteDeprecationWarning: Using 'httpx' with 'starlette.testclient' is deprecated; install 'httpx2' instead.` when only legacy `httpx` is present. Listing `httpx2>=2.13.1` directly satisfies the test client dependency.

## Storage and publication behavior

Ingestion performs basic structural checks for `data.playbyplay`, `periods`, and `teams`. Snapshot timestamps use UTC seconds (`YYYYMMDDTHHMMSSZ`). Two changed payloads for the same contest in the same second can overwrite the same file; Bronze is not an immutable or tamper-proof audit store. Snapshot selection takes the last stored manifest entry with an existing file for each contest, in manifest order, without rechecking its hash. See [ADR-001](docs/adr/ADR-001-medallion-storage-and-audit-trail.md) for the exact guarantees and limitations.

A successful pipeline run writes all four Gold tables from their complete DataFrames into a fresh generation. Fact and aggregate files are partitioned by `contest_id`; each table is checked for matching column names and read for validation before the publisher syncs files and directories and atomically replaces `current.json`. Writes or validation failures before publication leave the previous version available. This publication boundary covers Gold; Silver remains an independently updated working dataset.

Each REST or GraphQL request creates a DuckDB client that resolves the pointer once and reads that Gold version for the entire request. Queries use separate cursors, closed after fetching, and the client closes when the request finishes. API clients do not read mutable Silver. A standalone `DuckDBClient` also pins Gold at construction, but exposes Silver by default; use `include_silver=False` for a Gold-only reader and create a new client to see a later publication.

Old Gold versions, legacy flat directories, and finalized but unpublished versions are retained. A process crash can also leave a staging directory. Retention and cleanup are manual and must preserve files used by active readers. `resolve_gold_dir()` in [gold_storage.py](src/db/gold_storage.py) supports the legacy flat layout only when no pointer exists; a malformed or incomplete publication is rejected. Do not recursively scan all of `data/gold`, which can include multiple versions of the same records.

## Event modeling

- Nested array positions define play order. `play_id` combines `contest_id` and the derived `play_seq`; inserting earlier source plays can change later IDs.
- Clocks backward-fill within a period, defaulting to `0:00` after the last available clock or when no clock is present. Elapsed time models 900-second regulation periods and 240-second overtime periods.
- Reported scores forward-fill across a contest, defaulting to zero. Goal rows include their reported updated score, so their margins are not pre-goal margins.
- Native Spark expressions normalize player names, classify recognized text patterns, and separate turnover committers from caused-by defenders. Unsupported patterns remain `UNKNOWN`.
- Faceoff winner/loser names use contest-local player/team evidence, including later plays and embedded ground-ball text. Ambiguous or unsupported assignments remain null; participant order is not treated as home/away order.
- `possession_team_id` identifies the event's team for shots, goals, ground balls, faceoffs, turnovers, and clears. It is not a continuous possession state. Non-goal shot retention compares that team with the next possession-indicating event in the same period; no subsequent indicator yields `false`.

[ADR-002](docs/adr/ADR-002-stateful-windowing-and-event-modeling.md) describes these modeling choices and the shooting metrics.

## Query examples

### Python and SQL

Construct a client to query one published Gold version:

```python
from src.db.duckdb_client import DuckDBClient

client = DuckDBClient(include_silver=False)
try:
    print(client.get_contests())
finally:
    client.close()
```

The following SQL can be passed to that client's `execute_query()` method while it is open. It selects caused turnovers with the defending team down by one in the last minute of regulation. The event team is the turnover committer's team, so a positive event-team margin means the defender is behind.

```sql
SELECT
    play_id, period_number, clock_display,
    primary_player_name AS turnover_committer,
    caused_by_player_name AS caused_by_player,
    caused_by_team_id,
    -event_team_margin AS defending_team_margin,
    play_text
FROM fact_plays
WHERE event_type = 'TURNOVER'
  AND caused_by_player_name IS NOT NULL
  AND period_number = 4
  AND period_seconds_remaining <= 60
  AND event_team_margin = 1
ORDER BY contest_id, play_seq, play_id;
```

### GraphQL

```graphql
query ListContests {
  contests {
    contestId
    title
    status
  }
}

query GetContest {
  contest(id: 6599996) {
    contestId
    title
    status
    teamStats {
      teamShort
      goals
      totalShots
      turnovers
      groundBalls
    }
  }
}
```

Both direct arguments and a structured filter are supported:

```graphql
query SelectPlays {
  turnovers: plays(playerName: "Mueller", eventType: "TURNOVER") {
    playId
    causedByPlayerName
    playText
  }
  goals: plays(filter: {team: "ND", eventType: "GOAL", maxGameSecondsRemaining: 1800}) {
    playId
    eventTeamShort
    gameSecondsRemainingReg
    primaryPlayerName
  }
}
```

### REST

```bash
curl "http://localhost:8000/api/contests"
curl "http://localhost:8000/api/contests/6599996/summary"
curl "http://localhost:8000/api/contests/6599996/plays?limit=50&offset=0"
curl "http://localhost:8000/api/plays?player_name=Mueller&event_type=TURNOVER"
curl "http://localhost:8000/api/plays?team=ND&max_game_seconds_remaining=1800&event_type=GOAL"
curl "http://localhost:8000/api/plays?max_period_seconds_remaining=60&limit=50&offset=0"
```

General play queries default to `limit=100`, `offset=0`, and ascending `play_seq`. Supported sort columns are `play_seq`, `game_seconds_elapsed`, `game_seconds_remaining_reg`, `period_seconds_remaining`, `period_number`, and `play_id`. Ties use ascending `contest_id`, `play_seq`, and `play_id`, omitting any key already used as the primary sort. Offset pagination is deterministic for an unchanged dataset; successive requests can see a new publication.

REST and structured GraphQL filters validate limits of 1–500 and nonnegative offsets. Direct GraphQL arguments currently bypass revalidation when merged into the filter model; they do not enforce the same bounds.

### Shooting efficiency

```bash
curl "http://localhost:8000/api/shooting-efficiency"
curl "http://localhost:8000/api/shooting-efficiency?min_shots=5&top_players_limit=10&top_teams_limit=10"
curl "http://localhost:8000/api/shooting-efficiency?contest_id=6599996&order_by=normalized"
```

The endpoint (also accessible via `/api/shooting-efficiency/summary` and `/api/analytics/shooting-efficiency`) returns pooled shooting metrics, separate averages of Gold aggregate-row efficiencies, retention by shot result, and up to ten qualifying players and ten teams by default. Players require at least ten shots unless `min_shots` is changed. Ranking defaults to realized efficiency; `order_by=normalized` uses expected shot-loss rates instead.

- `total_shots` includes both `SHOT` and `GOAL` events.
- Shooting percentage is `goals / total_shots`.
- Realized shooting efficiency is `goals / (goals + realized_shots_lost)`.
- Normalized shooting efficiency substitutes expected losses based on shot-result retention rates across the entire published dataset. These baselines remain global even when the response is filtered to one contest; its retention breakdown does respect that contest filter.
- Raw play filters use `POST` and `CROSSBAR`; the retention summary displays them as `HIT POST` and `HIT CROSSBAR`, alongside `SAVE`, `WIDE`, `HIGH`, and `BLOCKED`. Unrecognized shot results are not included in those six breakdown buckets.

## Architecture decisions

- [ADR-001: Medallion Storage, Snapshot Tracking, and Gold Publication](docs/adr/ADR-001-medallion-storage-and-audit-trail.md)
- [ADR-002: Stateful Windowing and Event Modeling](docs/adr/ADR-002-stateful-windowing-and-event-modeling.md)
- [ADR-003: Local Spark Execution and Persistence Strategy](docs/adr/ADR-003-pyspark-compute-and-caching-strategy.md)

The pipeline writes a generated plan for the enriched-facts DataFrame to [docs/spark_execution_plan.md](docs/spark_execution_plan.md). It is a plan artifact, not a benchmark or a record of every pipeline stage.

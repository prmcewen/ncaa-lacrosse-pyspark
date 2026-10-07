# NCAA Lacrosse Play-by-Play Data Platform

[![CI](https://github.com/prmcewen/ncaa-lacrosse-pyspark/actions/workflows/ci.yml/badge.svg)](https://github.com/prmcewen/ncaa-lacrosse-pyspark/actions/workflows/ci.yml)

Turn NCAA men's lacrosse play-by-play descriptions into structured data for situational analysis. The platform helps a coach or analyst find defensive plays in close games, examine what happens after missed shots, and compare team and player shooting efficiency without manually reading game logs.

PySpark transforms nested JSON into Delta Lake tables; DuckDB serves the results through REST, GraphQL, and SQL. The repository includes **20 real-game Bronze fixtures** for a local demo and a generator for larger synthetic workloads.

Shooting efficiency accounts for what happens after a missed shot: keeping the ball gives the offense another chance to score. The platform calculates three measures for teams and players:

| Measure | Calculation |
| --- | --- |
| Shooting percentage | Goals / total shots, including goals |
| Realized shooting efficiency | Goals / (goals + non-goal shots inferred to lose possession) |
| Normalized shooting efficiency | Goals / (goals + expected non-goal shot losses) |

For example, Princeton scored **16 goals on 53 shots** in the bundled Notre Dame–Princeton game (`6599996`). Of its 37 non-goal shots, 27 were inferred to retain possession and 10 to lose it. Shooting percentage was **30.19%** (`16 / 53`); realized shooting efficiency was **61.54%** (`16 / (16 + 10)`).

Retention is inferred from the next possession-indicating event in the same period. Normalized efficiency replaces each non-goal shot's inferred loss with the average loss rate for its result (such as a save, wide shot, or blocked shot) across the full published dataset, then sums those probabilities into expected losses. This gives teams and players a common baseline for comparing their shot outcomes. [Query examples](#shooting-efficiency) expose the metrics and rankings.

## PySpark work demonstrated

- **Native transformations:** explicit JSON schemas, positional array explosions, text classification, and player-name normalization using Spark SQL expressions.
- **Window calculations:** contest-local sequencing, missing-clock handling, running scores, shot-retention lookahead, and contest-wide player/team evidence for faceoff attribution.
- **Incremental Delta writes:** replace changed contests in Silver while preserving unaffected rows, including games sharing a data file.
- **Execution and storage choices:** broadcast lookups, scoped persistence, adaptive execution, range distribution, and sorted output with bounded file counts for fresh writes.
- **Failure handling:** publish complete Gold generations and pin each API request to one version, with regression tests for failed writes, retries, and concurrent readers.

In a [recorded benchmark with 10 million synthetic plays](docs/benchmarks/unpartitioned-10m.md), changing the file layout and adding scoped disk persistence reduced local pipeline runtime from **32m 27s to 13m 47s (2.35×)**. The [recorded evidence](docs/benchmarks/evidence/README.md) includes timings, configuration, correctness results, and implementation provenance. This was one before/after comparison on the same machine; the report explains the configuration, correctness checks, and measurement limits. The [small-files case study](docs/case-studies/small-files/README.md) documents the diagnosis and contributions.

## Quickstart

Prerequisites: **Python 3.11–3.12, Java 17+, and `uv`**. `.python-version` selects Python 3.12. Java must be on `PATH` or configured through `JAVA_HOME`; the Spark helper also checks standard JVM installation locations. Gold publication requires POSIX filesystem operations, including directory `fsync`.

Run from the repository root. The bundled Bronze fixtures let you build the demo without fetching new NCAA data. Dependency versions are recorded in [uv.lock](uv.lock).

```bash
uv sync --locked

# Modest local configuration for the bundled games (same as CI)
set -a; source .env.example; set +a

# Build Silver and publish Gold
uv run python -m src.etl.run_pipeline

# Start the API
uv run uvicorn src.api.main:app --port 8000 --reload
```

Copy `.env.example` to `.env` and edit it for your machine if you want
different Spark settings; `.env` is git-ignored.

Spark resolves Delta Java dependencies on its first run. DuckDB installs its Delta extension if it is not already available, so initial setup can require network access even when using the bundled game data.

Open [REST documentation](http://localhost:8000/docs) or the [GraphQL interface](http://localhost:8000/graphql). In another terminal:

```bash
curl "http://localhost:8000/api/contests/6599996/summary"
```

The response includes these verified totals for the bundled game:

| Team | Goals | Total shots | Ground balls | Shooting percentage |
| --- | ---: | ---: | ---: | ---: |
| Notre Dame | 9 | 41 | 30 | 0.2195 |
| Princeton | 16 | 53 | 46 | 0.3019 |

### Fetch updates and rebuild

```bash
# Fetch a contest; an unchanged payload hash skips the snapshot write
uv run python -m src.ingestion.ingest --contest-id 6599996

# Replace changed Silver contests and rebuild Gold
uv run python -m src.etl.run_pipeline

# Use after changing Silver transformations or migrating legacy partitioned tables
uv run python -m src.etl.run_pipeline --full-refresh
```

Silver freshness compares `(contest_id, ingest_timestamp)` with the selected Bronze snapshots, so code changes require `--full-refresh`. A Gold-only change takes effect on the next normal run. Each successful run rebuilds all four Gold tables, even when Silver has no changed snapshots.

## Architecture

```text
NCAA API / sdataprod
  |
  v
Bronze JSON snapshots + ingest_manifest.jsonl
  |
  v
PySpark: parse, attribute players and teams, calculate windows
  |
  v
Silver Delta: plays + contest metadata
  |
  v
Gold Delta: dimensions + facts + team-game aggregates
  |
  v
Atomic current.json publication pointer
  |
  v
DuckDB request snapshot (delta_scan)
  |
  +--> FastAPI REST /api/...
  +--> Strawberry GraphQL /graphql
```

| Layer | Location | Purpose |
| --- | --- | --- |
| Bronze | `data/bronze/contest_id=<id>/ingest_timestamp=<timestamp>.json` | Raw snapshots and a manifest; canonical SHA-256 comparison skips unchanged payloads. |
| Silver plays | `data/silver/silver_plays/` | 34 structured play columns, with selective replacement of changed contests. |
| Silver contests | `data/silver/silver_contests/` | One row per selected contest snapshot, including nested team metadata. |
| Gold | `data/gold/versions/<version>/<table>/` | A complete generation of analytical tables. |
| Publication pointer | `data/gold/current.json` | Selects the Gold generation for new readers. |

Silver and Gold are stored as **Delta tables**. Each table directory contains Parquet data files and a `_delta_log/` transaction log.

Readers require a valid Delta transaction log and DuckDB's Delta extension. Missing or corrupt logs fail explicitly; there is no raw Parquet fallback that could include obsolete data files.

For data created by earlier versions, the next pipeline run automatically renames the Silver directories and publishes Gold with these names. Older Gold generations remain readable.

Gold contains `dim_teams`, `dim_contests`, `fact_plays`, and `agg_team_game_stats`. Facts enrich Silver plays with team color and full name. Aggregates have one row per `(contest_id, team_id)`, combining text aliases and including attributed plays without a text abbreviation. The `team_short` display label comes from `dim_teams.name_short`, with a deterministic fallback to an observed abbreviation and then the team ID.

Game totals include shots, goals, saves faced, ground balls, turnovers, clears, and penalties. Ground balls include explicit pickups embedded in faceoff descriptions, credited to the pickup team; facts keep the original play rows.

### Why Spark and DuckDB

The bundled games are small enough for a single-machine tool. Spark is used here to demonstrate batch transformations over nested data, ordered windows, Delta updates, and execution profiling as the workload grows. DuckDB lets the API query the resulting tables directly through its native Delta extension. The implementation runs locally; the benchmark measures local execution.

## Query examples

### Python and SQL

This shows the shooting efficiency calculations above for both teams against one published Gold version:

```python
from src.db.duckdb_client import DuckDBClient

query = """
SELECT
    team_short,
    goals,
    total_shots,
    shots_retained,
    realized_shots_lost,
    normalized_shots_lost,
    shooting_pct,
    realized_shooting_efficiency,
    normalized_shooting_efficiency
FROM agg_team_game_stats
WHERE contest_id = 6599996
ORDER BY team_short;
"""

client = DuckDBClient(include_silver=False)
try:
    print(client.execute_query(query))
finally:
    client.close()
```

The three rates are returned as fractions rounded to four decimal places; for example, `0.6154` represents 61.54%. `shots_retained` counts non-goal shots, while `normalized_shots_lost` is the sum of their expected loss probabilities.

### REST

```bash
# List games and inspect a game summary
curl "http://localhost:8000/api/contests"
curl "http://localhost:8000/api/contests/6599996/summary"

# Find a defender's caused turnovers or a team's scoring plays
curl "http://localhost:8000/api/plays?player_name=Mueller&event_type=TURNOVER"
curl "http://localhost:8000/api/plays?team=ND&max_game_seconds_remaining=1800&event_type=GOAL"

# Page through a game's plays
curl "http://localhost:8000/api/contests/6599996/plays?limit=50&offset=0"
```

General play queries default to `limit=100`, `offset=0`, and ascending `play_seq`. REST, direct GraphQL arguments, and structured GraphQL filters enforce limits of 1–500 and nonnegative offsets.

Supported sort columns are `play_seq`, `game_seconds_elapsed`, `game_seconds_remaining_reg`, `period_seconds_remaining`, `period_number`, and `play_id`. Ties use ascending `contest_id`, `play_seq`, and `play_id`, omitting the primary sort key. Pagination is deterministic for a fixed publication; successive requests can see a newer generation.

### GraphQL

```graphql
query InspectGame {
  contest(id: 6599996) {
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
  turnovers: plays(playerName: "Mueller", eventType: "TURNOVER", limit: 10) {
    playId
    causedByPlayerName
    playText
  }
  goals: plays(filter: {team: "ND", eventType: "GOAL", maxGameSecondsRemaining: 1800}) {
    playId
    primaryPlayerName
    gameSecondsRemainingReg
  }
}
```

### Shooting efficiency

```bash
curl "http://localhost:8000/api/shooting-efficiency?min_shots=5&top_players_limit=10&top_teams_limit=10"
curl "http://localhost:8000/api/shooting-efficiency?contest_id=6599996&order_by=normalized"
```

The endpoint returns pooled shooting metrics, averages of team-game efficiencies, retention by shot result, and player/team rankings. Players require at least ten shots by default; `min_shots` changes that threshold. Rankings default to realized efficiency; `order_by=normalized` uses expected shot losses.

| Metric | Definition |
| --- | --- |
| Total shots | `SHOT` plus `GOAL` events |
| Shooting percentage | Goals / total shots |
| Realized shooting efficiency | Goals / (goals + non-goal shots inferred lost) |
| Normalized shooting efficiency | Goals / (goals + expected non-goal shot losses) |

Expected losses use shot-result retention rates across the full published dataset, including when a response is filtered to one contest. The retention breakdown itself respects the contest filter. These metrics depend on inferred retention, described below.

Raw play filters use `POST` and `CROSSBAR`; the retention summary displays `HIT POST` and `HIT CROSSBAR`, alongside `SAVE`, `WIDE`, `HIGH`, and `BLOCKED`. Unrecognized shot results are omitted from those six breakdown buckets.

## Tests

Build the bundled data with the quickstart pipeline before running the suite. The Bronze fixtures are tracked; generated Silver and Gold tables are ignored by Git. Some API and SQL integration tests read those generated tables, while pipeline write tests use temporary datasets.

```bash
uv run pytest tests -v
uv run ruff check src tests benchmarks
```

[CI](.github/workflows/ci.yml) runs on pushes and pull requests using Ubuntu, Python 3.12, and Temurin 17. It installs locked dependencies, checks lint, builds Silver and Gold from the bundled Bronze fixtures, runs the tests, and checks that tracked files remain unchanged. Live NCAA fetches are not needed. The workflow can also be started manually from GitHub Actions.

Limiting discovery to `tests/` avoids scanning large generated benchmark directories. Coverage includes player parsing, faceoff attribution, embedded ground balls, mixed team aliases, deterministic pagination, concurrent queries, dimension corrections, publication failures, retries, and request-level consistency.

Dependencies are declared in [pyproject.toml](pyproject.toml). `httpx` handles ingestion; the dev group adds `pytest`, `ruff`, and `httpx2` for the locked Starlette test client.

## Modeling and operational limits

- **Ordering and clocks:** source array positions define play order and sequence-derived IDs. Earlier inserted plays can change later IDs. Missing clocks backward-fill within a period, defaulting to `0:00` when no later clock exists. Time calculations use 15-minute regulation periods and four-minute overtime periods.
- **Scores and attribution:** reported scores forward-fill; goal margins include the updated score. Native expressions parse recognized text patterns. Unsupported events remain `UNKNOWN`, and ambiguous faceoff participants remain null. Faceoff attribution can use later contest evidence and change as a live game is refreshed.
- **Retention inference:** `possession_team_id` identifies the event team, including the committing team on turnovers. Shot retention compares that ID with the next possession-indicating event in the same period; no later indicator yields `false`. It is an estimate from the recorded feed.
- **Bronze snapshots:** one-second timestamps are an accepted tradeoff for this human-entered feed. There is no built-in polling or scheduled ingestion; fetches are manually initiated. A collision would require two manual fetches of the same contest to capture different payloads and write snapshots within the same second, with the human-entered feed changing between those captures. This combination is considered effectively negligible under the intended workflow, so extra collision handling is omitted. Live-game refreshes are supported, but analysis of recorded games is the primary intended use. Selection follows manifest order and checks file existence without rechecking hashes. Snapshot and manifest writes are separate operations.
- **Publication and retention:** API requests pin a complete Gold generation. Failures before publication preserve the previous version, while Silver updates independently. Old Gold generations and crash leftovers require manual cleanup that preserves files used by active readers.
- **Local operation:** there is no cloud deployment or authentication. `/api/health` reports application liveness; it does not verify published data. Gold publication relies on local POSIX rename and sync behavior.

## Implementation details

- [Storage, snapshot tracking, and Gold publication](docs/adr/ADR-001-medallion-storage-and-audit-trail.md)
- [Event modeling and shooting metrics](docs/adr/ADR-002-stateful-windowing-and-event-modeling.md)
- [Spark execution and persistence strategy](docs/adr/ADR-003-pyspark-compute-and-caching-strategy.md)
- [10-million-play Delta layout benchmark](docs/benchmarks/unpartitioned-10m.md)
- [Small-files diagnosis and case study](docs/case-studies/small-files/README.md)

Silver and Gold use unpartitioned Delta files, range-distributed and sorted by contest ID; dimensions use one sorted output task. `LAXPXP_DELTA_WRITE_PARTITIONS` defaults to 16 and controls fresh-write task count, not a file-size target or lifetime file count. Incremental Silver updates use a data-column `replaceWhere` predicate and can rewrite shared files. Delta retains obsolete files for history; legacy partitioned Silver requires `--full-refresh` to migrate.

Spark jobs have groups and descriptions for pipeline actions, making them easier to locate in the Spark UI or History Server. Set `LAXPXP_SPARK_EVENT_LOG_DIR` to retain event logs. Lazy transformations execute within downstream actions; labels add no extra counts or cache warm-ups.

The committed [execution plan](docs/spark_execution_plan.md) is a static reference for the enriched-facts DataFrame, rather than every pipeline stage. Normal runs do not generate a plan. Set `LAXPXP_PLAN_OUTPUT` to an output path to capture one; the benchmark runner sets this automatically inside its ignored run directory.

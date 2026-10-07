# ADR-002: Stateful Windowing and Event Modeling

## Status

Accepted; updated for native Spark parsing and evidence-based faceoff attribution.

## Context

The feed contains sparse scores, missing clock strings, varying player-name formats, and text descriptions whose participant order is not a reliable indication of team affiliation. These fields need explicit interpretation before situational queries can use them.

## Decision

### Sequence, clocks, and scores

[transform.py](../../src/etl/transform.py) uses positional explosions over periods, stat containers, and plays. Blank play text is removed. `row_number()` ordered by those positions within a contest produces `play_seq`, and `play_id` is `<contest_id>_<play_seq>`. These preserve source array order, not an independently verified real-time event order; inserting an earlier play can renumber subsequent IDs.

Clock selection first coalesces the play clock with its stat-container clock, then treats empty strings as missing and backward-fills within `(contest_id, period_number)`. If no subsequent clock is available in the period (e.g., at the end of the quarter or game), it defaults to `0:00`. Elapsed time uses 900-second regulation periods and 240-second overtime periods. Regulation time remaining is clamped to zero after regulation.

Scores use a contest-wide window through the current row: `last(score, ignorenulls=True)`, defaulting to zero. `home_score_margin` is home minus visitor score; `event_team_margin` changes sign for an away-team event and is null when its perspective is unresolved. Goal rows include their reported updated score, so these fields do not provide the pre-goal margin.

### Event and player interpretation

Production ETL uses native Spark expressions for text classification and [player-name normalization](../../src/etl/player_cleaning.py). Comma patterns and suffixes are standardized, but ordinary name casing is preserved rather than automatically title-cased. Recognized patterns include goals, shots, turnovers, faceoffs, ground balls, penalties, clears, timeouts, goalie changes, and period ends. Other descriptions remain `UNKNOWN`; unsupported clock-violation text is not automatically converted into another turnover.

Team attribution infers contest-local aliases from team metadata (names, initials, and truncated display labels) and play labels paired with stat-container team IDs. Native Spark expressions use the longest matching label to separate team and player names; aliases identifying multiple teams remain unresolved unless the play supplies a source team ID. Stat-container team IDs remain the fallback. On turnovers, `primary_player_name` identifies the committer; `caused_by_player_name` and `caused_by_team_id` identify the caused-by player and opposing team. These, along with shot retention, faceoffs, penalties, clears, and running scores, form the 34 structured columns of the Silver plays dataset (`fact_plays` further enriches these with team color and full name). These are columns on play records, not separate player dimension tables.

### Faceoffs

Both participant names are parsed without assuming home/away ordering. Batch ETL builds contest-local player/team evidence from primary players, assistants, saving goalkeepers, caused-by players, and explicit ground-ball text embedded in faceoff descriptions. It uses the entire contest, including later plays.

One uniquely identified participant can establish which side the other participant represents. Conflicting team evidence, identical participant names, generic team placeholders, or no usable evidence leave winner and loser null. A ground-ball collector who is not a participant does not, by itself, identify the winning participant. The standalone single-play parser has no contest history and uses only embedded ground-ball evidence.

### Possession indicators and shot retention

`possession_team_id` is the event team for `SHOT`, `GOAL`, `GROUND_BALL`, `FACEOFF`, `TURNOVER`, and `CLEAR`. Other event types are null. A turnover therefore identifies the committing team, and a failed clear identifies the attempting team; this field is not a continuous post-event possession state.

For a non-goal shot, a forward window within the same contest and period finds the next non-null possession indicator. Equal team IDs yield `shot_possession_retained=true`; a different team or no subsequent indicator yields `false`. Non-shot events, including goals, have null retention. This is an inference from recorded events, not direct possession telemetry.

### Shooting analytics

[Gold aggregation](../../src/etl/transform.py) groups attributed plays by `(contest_id, team_id)`, merging different text aliases into one team-game row. Plays with a resolved team ID are included even when their text abbreviation is missing. The `team_short` display label comes from `dim_teams.name_short`, with a deterministic fallback to the lexicographically first nonblank event abbreviation and then the team ID. The core metrics are:

| Metric | Definition |
| --- | --- |
| `total_shots` | Count of `SHOT` plus `GOAL` events |
| `shots_retained` | Non-goal shots with retention true |
| `realized_shots_lost` | Non-goal shots with retention false |
| `realized_shot_possessions_used` | Goals plus realized shots lost |
| `shooting_pct` | Goals divided by total shots |
| `realized_shooting_efficiency` | Goals divided by realized shot possessions used |
| `normalized_shots_lost` | Sum of expected loss probabilities for non-goal shots |
| `normalized_shot_possessions_used` | Goals plus normalized shots lost |
| `normalized_shooting_efficiency` | Goals divided by normalized shot possessions used |

Expected loss probabilities are one minus the observed retention rate for each shot-result category over the full input dataset; missing categories use an overall loss-rate fallback. Gold and the [DuckDB summary](../../src/db/duckdb_client.py) compute these rates from Silver and published facts respectively. Rates are rounded to four decimal places. A contest-scoped API summary still uses global published baselines, while its shot-result retention breakdown is contest-scoped. Pooled overall efficiencies and averages of team-game efficiencies are separate response fields.

Alongside shooting efficiency, Gold aggregation also calculates game totals for each `(contest_id, team_id)` row: `saves_faced`, `ground_balls`, `turnovers`, `clears_good`, `clears_failed`, `penalties`, and `penalty_seconds`. Ground-ball totals count standalone events plus explicit pickups embedded in faceoff descriptions. Embedded pickups are resolved against the event team and its opponent using team metadata and aliases observed in resolved Silver plays from the same contest; unresolved teams are excluded. They are aggregate-only contributions, so Silver and Gold facts keep one original row per play and faceoff counts are unchanged.

Gold aggregation corrections operate on existing Silver columns and take effect on the next normal pipeline run. To apply updated team-label parsing to already processed contests, run the pipeline with `--full-refresh` to rebuild their Silver plays.

Play data uses `POST` and `CROSSBAR`. The API retention breakdown labels these `HIT POST` and `HIT CROSSBAR`, and returns six named buckets including zero-count categories. Unknown results are omitted from that breakdown.

### Query ordering

General `get_plays()` queries use a whitelisted primary sort followed by ascending `contest_id`, `play_seq`, and `play_id` tie-breakers, omitting the selected primary key where repeated. This makes offset pages deterministic for a fixed dataset. It does not pin a sequence of separate requests to one publication. Queries operate over DuckDB views backed by Delta tables, not application-created indexes.

## Consequences and limits

- Structured play fields support score, time, player-role, and team filters without repeating text parsing in API clients.
- Future contest evidence can resolve an earlier faceoff, so attribution may change when a live snapshot is refreshed.
- Missing or unsupported text can leave fields unknown. Retention estimates depend on the completeness and order of recorded events.
- Corrections can alter sequence-derived IDs; score margins on goals represent the current reported score rather than the score before the goal.

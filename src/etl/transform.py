import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Tuple, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
)
from pyspark.sql.window import Window

from src.etl.player_cleaning import clean_player_name_native
from src.etl.schemas import NCAA_CONTEST_SCHEMA, NCAA_PBP_SCHEMA

logger = logging.getLogger(__name__)


DATA_DIR = Path(os.environ.get("LAXPXP_DATA_DIR", Path(__file__).resolve().parents[2] / "data")).resolve()
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = Path(os.environ.get("LAXPXP_GOLD_DIR", DATA_DIR / "gold")).resolve()


def _normalize_team_alias(code: F.Column) -> F.Column:
    return F.upper(F.regexp_replace(F.trim(F.regexp_replace(F.trim(code), r"\.+$", "")), r"\s+", " "))


def _metadata_team_aliases(team_id: F.Column, short: F.Column, six: F.Column, full: F.Column) -> F.Column:
    """Derive names, initials and truncated labels from the supplied team metadata."""
    short = _normalize_team_alias(F.coalesce(short, F.lit("")))
    names = F.array(short, _normalize_team_alias(six), _normalize_team_alias(full))
    initials = F.transform(names, lambda name: F.concat_ws("", F.transform(
        F.split(name, r"\s+"), lambda word: F.substring(word, 1, 1),
    )))
    # NCAA text often truncates a display name (e.g. an eight-character label).
    prefixes = F.when(F.length(short) >= 3, F.transform(
        F.sequence(F.lit(3), F.length(short)),
        lambda length: F.trim(F.call_function("substring", short, F.lit(1), length)),
    )).otherwise(F.array().cast("array<string>"))
    aliases = F.array_distinct(F.filter(F.concat(names, initials, prefixes),
                                      lambda alias: alias.isNotNull() & (alias != "")))
    return F.transform(aliases, lambda alias: F.struct(
        alias.alias("code"), team_id.cast("long").alias("team_id"),
        F.lit(False).alias("observed"),
    ))


def _observed_team_alias(code: F.Column, team_id: F.Column) -> F.Column:
    code = _normalize_team_alias(code)
    return F.when((code != "") & team_id.isNotNull(), F.struct(
        code.alias("code"), team_id.cast("long").alias("team_id"),
        F.lit(True).alias("observed"),
    ))


def _with_team_pattern(df: DataFrame) -> DataFrame:
    codes = F.array_distinct(F.transform("_team_aliases", lambda alias: alias["code"]))
    # Match the longest label first so multiword team names stay out of player names.
    codes = F.array_sort(codes, lambda left, right: (
        F.when(F.length(left) > F.length(right), -1)
        .when(F.length(left) < F.length(right), 1).otherwise(0)
    ))
    escaped = F.transform(codes, lambda code: F.concat(F.lit(r"\Q"), code, F.lit(r"\E\.?")))
    return df.withColumn("_team_pattern", F.concat(
        F.lit("(?i:(?:"), F.concat_ws("|", escaped), F.lit(r"|\w+\.?)(?=\s|[,.(]|$))"),
    ))


def _extract_team_text(text: F.Column, prefix: str, suffix: str = "", group: int = 1) -> F.Column:
    # call_function accepts a per-row regex, keeping inference inside the Spark DAG.
    return F.call_function("regexp_extract", text,
                           F.concat(F.lit(prefix), F.col("_team_pattern"), F.lit(suffix)), F.lit(group))


def _lookup_team(code: F.Column) -> F.Column:
    matches = F.filter(
        F.col("_team_aliases"), lambda alias: alias["code"] == _normalize_team_alias(code),
    )
    observed = F.filter(matches, lambda alias: alias["observed"])
    # Recorded label/ID pairs take precedence over initials or name truncations.
    candidates = F.when(F.size(observed) > 0, observed).otherwise(matches)
    teams = F.array_distinct(F.transform(candidates, lambda alias: alias["team_id"]))
    # A shared abbreviation is not evidence for either participant.
    return F.when(F.size(teams) == 1, F.element_at(teams, 1))


def _infer_contest_team_aliases(df: DataFrame) -> DataFrame:
    """Learn additional labels from plays whose syntax separates team and player."""
    text = F.trim(F.col("play_text"))
    code = (
        F.when(text.contains("at goalie for"), F.regexp_extract(text, r"at goalie for\s+(.+?)\.?$", 1))
        .when(text.startswith("Faceoff"), F.regexp_extract(text, r"\s+won by\s+([^,(]+)", 1))
        .when(text.startswith("Clear attempt by"), F.regexp_extract(text, r"^Clear attempt by\s+(.+?)\s+(?:good|failed)\b", 1))
        .when(text.startswith("Timeout by"), F.regexp_extract(text, r"^Timeout by\s+(.+?)\.?$", 1))
        .otherwise(F.regexp_extract(text, r"^(?:(?:GOAL|Shot|Turnover|Ground ball pickup) by|Penalty on)\s+(\S+)", 1))
    )
    valid_team = F.when(
        (F.col("stat_team_id") == F.col("home_tid")) | (F.col("stat_team_id") == F.col("away_tid")),
        F.col("stat_team_id"),
    )
    df = df.withColumn("_observed_alias", _observed_team_alias(code, valid_team))
    observed = F.collect_set("_observed_alias").over(Window.partitionBy("contest_id"))
    df = df.withColumn("_team_aliases", F.array_distinct(F.concat(
        _metadata_team_aliases(F.col("home_tid"), F.col("home_short"), F.col("home_6char"), F.col("home_full")),
        _metadata_team_aliases(F.col("away_tid"), F.col("away_short"), F.col("away_6char"), F.col("away_full")),
        observed,
    ))).drop("_observed_alias")
    return _with_team_pattern(df)


def _faceoff_participants(text_col: F.Column) -> Tuple[F.Column, F.Column]:
    """Read both names without assuming that either team is listed first."""
    text_col = F.trim(text_col)
    first = clean_player_name_native(F.regexp_replace(
        F.trim(F.regexp_extract(text_col, r"^Faceoff\s+(.+?)\s+vs\s+", 1)), r"\.+$", ""
    ))
    second = clean_player_name_native(F.regexp_replace(
        F.trim(F.regexp_extract(text_col, r"\s+vs\s+(.+?)\s+won by\s+", 1)), r"\.+$", ""
    ))
    return first, second


def _resolve_faceoff_players(
    first_name: F.Column,
    second_name: F.Column,
    first_team: F.Column,
    second_team: F.Column,
    first_team_count: F.Column,
    second_team_count: F.Column,
    winning_team: F.Column,
    losing_team: F.Column,
) -> Tuple[F.Column, F.Column]:
    """Resolve a faceoff only when player/team evidence is consistent and unique."""
    valid = (
        first_name.isNotNull() & second_name.isNotNull()
        & (first_name != second_name)
        & ~F.upper(first_name).isin("TEAM", "TM", "BENCH")
        & ~F.upper(second_name).isin("TEAM", "TM", "BENCH")
        & winning_team.isNotNull() & losing_team.isNotNull()
        & (first_team_count <= 1) & (second_team_count <= 1)
        & (first_team.isNotNull() | second_team.isNotNull())
    )
    first_won = valid & (
        ((first_team == winning_team) & (second_team.isNull() | (second_team == losing_team)))
        | (first_team.isNull() & (second_team == losing_team))
    )
    second_won = valid & (
        ((second_team == winning_team) & (first_team.isNull() | (first_team == losing_team)))
        | (second_team.isNull() & (first_team == losing_team))
    )
    winner = F.when(first_won, first_name).when(second_won, second_name)
    loser = F.when(first_won, second_name).when(second_won, first_name)
    return winner, loser


def _faceoff_ground_ball(text: F.Column) -> Tuple[F.Column, F.Column]:
    """Extract explicit ground-ball evidence embedded in a faceoff description."""
    text = F.trim(text)
    code = _normalize_team_alias(_extract_team_text(
        text, r"^Faceoff.*Ground ball pickup by (", r")\s+",
    ))
    player = clean_player_name_native(F.regexp_replace(_extract_team_text(
        text, r"^Faceoff.*Ground ball pickup by ", r"\s+(.+?)\.?$",
    ), r"\.+$", ""))
    return player, code


def _attribute_faceoff_players(df: DataFrame) -> DataFrame:
    """Use contest-local evidence, including later plays, without guessing order.

    A full-contest window keeps the parsed play lineage in one branch. Ambiguous
    names and faceoffs with no identifiable participant remain unassigned.
    """
    first, second = _faceoff_participants(F.col("play_text"))
    gb_player, gb_code = _faceoff_ground_ball(F.col("play_text"))
    df = df.withColumn("_fo_first", first).withColumn("_fo_second", second)
    df = df.withColumn("_fo_gb_player", gb_player).withColumn("_fo_gb_code", gb_code)
    df = df.withColumn("_fo_gb_team", _lookup_team(F.col("_fo_gb_code")))

    def evidence(name, team):
        valid = (name.isNotNull() & ~F.upper(name).isin("TEAM", "TM", "BENCH")
                 & ((team == F.col("home_tid")) | (team == F.col("away_tid"))))
        return F.when(valid, F.struct(name.alias("name"), team.cast("long").alias("team_id")))

    secondary_team = (F.when(F.col("event_type") == "GOAL", F.col("team_id"))
                      .when(F.col("event_type") == "SHOT", F.col("opponent_team_id")))
    df = df.withColumn("_fo_evidence", F.filter(F.array(
        evidence(F.col("primary_player_name"), F.col("team_id")),
        evidence(F.col("secondary_player_name"), secondary_team),
        evidence(F.col("caused_by_player_name"), F.col("caused_by_team_id")),
        evidence(F.col("_fo_gb_player"), F.col("_fo_gb_team")),
    ), lambda entry: entry.isNotNull()))
    df = df.withColumn("_fo_evidence", F.array_distinct(F.flatten(
        F.collect_list("_fo_evidence").over(Window.partitionBy("contest_id"))
    )))
    for side in ("first", "second"):
        df = df.withColumn(f"_fo_{side}_teams", F.array_distinct(F.transform(
            F.filter(F.col("_fo_evidence"), lambda entry: entry["name"] == F.col(f"_fo_{side}")),
            lambda entry: entry["team_id"],
        )))
        df = df.withColumn(f"_fo_{side}_team", F.when(
            F.size(f"_fo_{side}_teams") == 1, F.element_at(f"_fo_{side}_teams", 1)
        ))
    winner, loser = _resolve_faceoff_players(
        F.col("_fo_first"), F.col("_fo_second"), F.col("_fo_first_team"), F.col("_fo_second_team"),
        F.size("_fo_first_teams"), F.size("_fo_second_teams"), F.col("team_id"), F.col("opponent_team_id"),
    )
    return (df.withColumn("faceoff_winner_player", winner)
            .withColumn("faceoff_loser_player", loser)
            .drop(*[name for name in df.columns if name.startswith("_fo_")]))

def get_latest_valid_snapshots(manifest_path: Path = BRONZE_DIR / "ingest_manifest.jsonl") -> List[Tuple[int, str, Path]]:
    """Scan manifest to find the newest stored snapshot for each contest."""
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found at {manifest_path}")

    latest_by_contest: Dict[int, Tuple[str, Path]] = {}
    project_root = manifest_path.parents[2]

    with manifest_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            entry = json.loads(line)
            if entry.get("status") == "STORED_NEW_VERSION":
                cid = entry["contest_id"]
                ts = entry["ingest_timestamp"]
                rel_path = entry["file_path"]
                full_path = project_root / rel_path
                if full_path.exists():
                    latest_by_contest[cid] = (ts, full_path)

    return [(cid, ts, path) for cid, (ts, path) in latest_by_contest.items()]



def classify_event_type_native(play_text_col: F.Column) -> F.Column:
    """
    Classify play event type using 100% native Spark SQL expressions.
    Enables Catalyst pushdown and zero-IPC filtering on event_type.
    """
    return (
        F.when(play_text_col.startswith("GOAL by"), "GOAL")
        .when(play_text_col.startswith("Shot by"), "SHOT")
        .when(play_text_col.startswith("Turnover by"), "TURNOVER")
        .when(play_text_col.startswith("Faceoff"), "FACEOFF")
        .when(play_text_col.startswith("Ground ball"), "GROUND_BALL")
        .when(play_text_col.startswith("Penalty on"), "PENALTY")
        .when(play_text_col.startswith("Clear attempt by"), "CLEAR")
        .when(play_text_col.startswith("Timeout by"), "TIMEOUT")
        .when(play_text_col.contains("at goalie for"), "GOALIE_CHANGE")
        .when(play_text_col.contains("End-of-period"), "PERIOD_END")
        .otherwise("UNKNOWN")
    )


def parse_clock_native(clock_col: F.Column) -> F.Column:
    """Convert mm:ss clock string to total seconds using native Spark SQL expressions."""
    parts = F.split(clock_col, ":")
    return F.when(
        clock_col.contains(":"),
        (parts.getItem(0).cast(IntegerType()) * 60) + parts.getItem(1).cast(IntegerType())
    ).otherwise(F.lit(0))


def get_pending_snapshots(
    spark: SparkSession,
    snapshots: List[Tuple[int, str, Path]],
    silver_output_path: Path = SILVER_DIR / "silver_plays",
    full_refresh: bool = False,
) -> List[Tuple[int, str, Path]]:
    """
    Identify contest snapshots that need to be processed into Silver.
    If full_refresh is True or the Silver dataset does not exist, returns all snapshots.
    Otherwise, inspects existing partitions and only returns snapshots whose
    (contest_id, ingest_timestamp) is not already materialized in Silver.
    Silver is read through Delta Lake only; a Parquet-only Silver (left by the
    removed write fallback) raises ValueError instead of being read as Parquet.
    """
    if full_refresh or not silver_output_path.exists():
        return snapshots

    if not (silver_output_path / "_delta_log").exists():
        if any(silver_output_path.rglob("*.parquet")):
            raise ValueError(
                f"Silver at {silver_output_path} holds Parquet data with no Delta "
                "transaction log; re-run with --full-refresh to rebuild it as Delta."
            )
        return snapshots

    try:
        existing_df = spark.read.format("delta").load(str(silver_output_path))
        existing_records = {
            (int(row["contest_id"]), str(row["ingest_timestamp"]))
            for row in existing_df.select("contest_id", "ingest_timestamp").distinct().collect()
        }
        pending = [s for s in snapshots if (int(s[0]), str(s[1])) not in existing_records]
        return pending
    except Exception as e:
        logger.warning(f"Could not inspect existing Silver dataset ({e}); defaulting to all snapshots.")
        return snapshots


def _latest_dimension_rows(df: DataFrame, key: str) -> DataFrame:
    # Timestamp decides recency; contest and metadata settle equal timestamps
    # deterministically, independently of Spark partition or input file order.
    metadata = sorted(c for c in df.columns if not c.startswith("_source_"))
    order = [F.col("_source_timestamp").desc_nulls_last(), F.col("_source_contest").desc_nulls_last()]
    order += [F.col(c).asc_nulls_last() for c in metadata if c != key]
    return (df.withColumn("_dimension_rank", F.row_number().over(Window.partitionBy(key).orderBy(*order)))
            .filter(F.col("_dimension_rank") == 1)
            .drop("_dimension_rank", "_source_timestamp", "_source_contest"))


def extract_silver_contests_from_pbp(
    pbp_df: DataFrame, snapshots: List[Tuple[int, str, Path]]
) -> DataFrame:
    """Keep one contest row and its team metadata from each selected snapshot."""
    stamped = _with_snapshot_timestamps(pbp_df, snapshots)
    return stamped.select(
        F.col("contestId").alias("contest_id"),
        F.col("_snapshot_timestamp").alias("ingest_timestamp"),
        "title", "status",
        F.transform("teams", lambda team: F.struct(
            team["teamId"].cast("long").alias("team_id"),
            team["nameShort"].alias("name_short"),
            team["nameFull"].alias("name_full"),
            team["name6Char"].alias("name_6char"),
            team["seoname"].alias("seoname"),
            team["color"].alias("color"),
            team["isHome"].alias("is_home"),
        )).alias("teams"),
    )


def process_bronze_to_silver_contests(
    spark: SparkSession, snapshots: List[Tuple[int, str, Path]]
) -> DataFrame:
    """Parse only contest and team metadata for the Silver contest table."""
    if not snapshots:
        raise ValueError("No snapshots provided to process_bronze_to_silver_contests")
    raw = spark.read.option("multiline", "true").schema(NCAA_CONTEST_SCHEMA).json([str(s[2]) for s in snapshots])
    return extract_silver_contests_from_pbp(raw.select("data.playbyplay.*"), snapshots)


def extract_dimensions_from_silver_contests(silver_contests: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Build both Gold dimensions from the materialized Silver contest rows."""
    dim_teams = silver_contests.select(
        F.col("ingest_timestamp").alias("_source_timestamp"),
        F.col("contest_id").alias("_source_contest"),
        F.explode("teams").alias("team"),
    ).select("_source_timestamp", "_source_contest", "team.*").drop("is_home")
    dim_contests = silver_contests.select(
        F.col("ingest_timestamp").alias("_source_timestamp"),
        F.col("contest_id").alias("_source_contest"),
        "contest_id", "title", "status",
    )
    return _latest_dimension_rows(dim_teams, "team_id"), _latest_dimension_rows(dim_contests, "contest_id")


def _with_snapshot_timestamps(pbp_df: DataFrame, snapshots: List[Tuple[int, str, Path]]) -> DataFrame:
    timestamps = pbp_df.sparkSession.createDataFrame(
        [(int(cid), ts) for cid, ts, _ in snapshots], "_snapshot_contest long, _snapshot_timestamp string"
    )
    return pbp_df.join(F.broadcast(timestamps), pbp_df.contestId == timestamps._snapshot_contest, "left").drop("_snapshot_contest")


def validate_dimension_keys(df: DataFrame, key: str) -> None:
    if df.filter(F.col(key).isNull()).limit(1).count():
        raise ValueError(f"Dimension {key} contains null keys")
    if df.groupBy(key).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"Dimension {key} contains duplicate keys")


def process_bronze_to_silver(
    spark: SparkSession,
    snapshots: Union[Tuple[int, str, Path], List[Tuple[int, str, Path]]],
) -> DataFrame:
    """
    Transform raw Bronze JSON snapshot(s) into a structured 34-column Silver DataFrame.
    Supports either a single snapshot tuple (cid, ingest_ts, path) or a list of snapshot tuples
    for multi-game batch execution in a single PySpark DAG.
    """
    if isinstance(snapshots, tuple):
        snapshot_list = [snapshots]
    else:
        snapshot_list = list(snapshots)

    if not snapshot_list:
        raise ValueError("No snapshots provided to process_bronze_to_silver")

    paths = [str(s[2]) for s in snapshot_list]
    logger.debug(f"Processing {len(snapshot_list)} contest snapshot(s)...")

    raw_df = spark.read.option("multiline", "true").schema(NCAA_PBP_SCHEMA).json(paths)
    pbp_df = raw_df.select(F.col("data.playbyplay.*"))

    # Extract teams metadata per contest using native PySpark array expressions (no driver collect)
    home_team = F.filter(F.col("teams"), lambda t: t["isHome"] == True).getItem(0)
    away_team = F.filter(F.col("teams"), lambda t: t["isHome"] == False).getItem(0)

    # Positional explode to strictly guarantee preserved chronological ordering
    exploded_periods = pbp_df.select(
        F.col("contestId").alias("contest_id"),
        home_team["teamId"].cast("long").alias("home_tid"),
        away_team["teamId"].cast("long").alias("away_tid"),
        home_team["nameShort"].alias("home_short"),
        away_team["nameShort"].alias("away_short"),
        home_team["name6Char"].alias("home_6char"),
        away_team["name6Char"].alias("away_6char"),
        home_team["nameFull"].alias("home_full"),
        away_team["nameFull"].alias("away_full"),
        F.posexplode("periods").alias("period_pos", "period"),
    )

    exploded_stats = exploded_periods.select(
        F.col("contest_id"),
        F.col("home_tid"),
        F.col("away_tid"),
        F.col("home_short"),
        F.col("away_short"),
        F.col("home_6char"),
        F.col("away_6char"),
        F.col("home_full"),
        F.col("away_full"),
        F.col("period_pos"),
        F.col("period.periodNumber").alias("period_number"),
        F.col("period.periodDisplay").alias("period_display"),
        F.posexplode("period.playbyplayStats").alias("stat_pos", "stat"),
    )

    exploded_plays = exploded_stats.select(
        F.col("contest_id"),
        F.col("home_tid"),
        F.col("away_tid"),
        F.col("home_short"),
        F.col("away_short"),
        F.col("home_6char"),
        F.col("away_6char"),
        F.col("home_full"),
        F.col("away_full"),
        F.col("period_pos"),
        F.col("period_number"),
        F.col("period_display"),
        F.col("stat_pos"),
        F.col("stat.teamId").alias("stat_team_id"),
        F.col("stat.clock").alias("stat_clock"),
        F.posexplode("stat.plays").alias("play_pos", "play"),
    ).select(
        F.col("contest_id"),
        F.col("home_tid"),
        F.col("away_tid"),
        F.col("home_short"),
        F.col("away_short"),
        F.col("home_6char"),
        F.col("away_6char"),
        F.col("home_full"),
        F.col("away_full"),
        F.col("period_pos"),
        F.col("period_number"),
        F.col("period_display"),
        F.col("stat_pos"),
        F.col("play_pos"),
        F.col("stat_team_id"),
        F.coalesce(F.col("play.clock"), F.col("stat_clock")).alias("raw_clock"),
        F.col("play.playText").alias("play_text"),
        F.col("play.homeScore").alias("raw_home_score"),
        F.col("play.visitorScore").alias("raw_visitor_score"),
    )

    # Filter blank lines
    valid_plays = exploded_plays.filter(
        F.col("play_text").isNotNull() & (F.trim(F.col("play_text")) != "")
    )

    # Monotonic sequential play ID based on document sequence
    seq_window = Window.partitionBy("contest_id").orderBy("period_pos", "stat_pos", "play_pos")
    sequenced_df = valid_plays.withColumn("play_seq", F.row_number().over(seq_window))

    # Clock backward-fill within period for events that lack an explicit clock (e.g. turnovers)
    period_clock_window = Window.partitionBy("contest_id", "period_number").orderBy("play_seq").rowsBetween(
        Window.currentRow, Window.unboundedFollowing
    )
    clock_cleaned_df = sequenced_df.withColumn(
        "non_empty_clock",
        F.when(F.trim(F.col("raw_clock")) != "", F.trim(F.col("raw_clock"))),
    ).withColumn(
        "clock_display",
        F.coalesce(F.first("non_empty_clock", ignorenulls=True).over(period_clock_window), F.lit("0:00")),
    )

    # Clock to seconds (Native Spark SQL expression)
    timed_df = clock_cleaned_df.withColumn(
        "period_seconds_remaining",
        parse_clock_native(F.col("clock_display")),
    )

    # Regulation quarter is 15 minutes (900 seconds)
    timed_df = timed_df.withColumn(
        "game_seconds_elapsed",
        F.when(F.col("period_number") <= 4,
               (F.col("period_number") - 1) * 900 + (900 - F.col("period_seconds_remaining")))
        .otherwise(3600 + (F.col("period_number") - 5) * 240 + (240 - F.col("period_seconds_remaining"))),
    ).withColumn(
        "game_seconds_remaining_reg",
        F.greatest(F.lit(0), F.lit(3600) - F.col("game_seconds_elapsed")),
    )

    # Window Forward-Fill for Running Scores in chronological sequence
    # Running score_window early avoids carrying unmaterialized text mining expressions into SortExec
    score_window = Window.partitionBy("contest_id").orderBy("play_seq").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    scored_df = timed_df.withColumn(
        "running_home_score",
        F.coalesce(F.last("raw_home_score", ignorenulls=True).over(score_window), F.lit(0)),
    ).withColumn(
        "running_visitor_score",
        F.coalesce(F.last("raw_visitor_score", ignorenulls=True).over(score_window), F.lit(0)),
    ).withColumn(
        "home_score_margin",
        F.col("running_home_score") - F.col("running_visitor_score"),
    )

    # Infer aliases within each contest before parsing event and player labels.
    scored_df = _infer_contest_team_aliases(scored_df)

    # Step A: Event type and team code extraction
    df_a = scored_df.withColumn(
        "norm_text", F.trim(F.coalesce(F.col("play_text"), F.lit("")))
    ).withColumn(
        "event_type", classify_event_type_native(F.col("norm_text"))
    ).withColumn(
        "by_team",
        _extract_team_text(
            F.col("norm_text"),
            r"^(?:GOAL|Shot|Turnover|Clear attempt|Timeout|Ground ball pickup) by (", r")",
        )
    ).withColumn(
        "ev_team_short_raw",
        F.when(F.col("by_team") != "", F.col("by_team"))
        .when(F.col("norm_text").startswith("Faceoff"), _extract_team_text(F.col("norm_text"), r"^Faceoff\s+.+?\s+vs\s+.+?\s+won by\s+(", r")"))
        .when(F.col("norm_text").startswith("Penalty on"), _extract_team_text(F.col("norm_text"), r"^Penalty on (", r")"))
        .when(F.col("norm_text").contains("at goalie for"), _extract_team_text(F.col("norm_text"), r"at goalie for (", r")"))
        .otherwise(F.lit(None).cast(StringType()))
    ).withColumn(
        "event_team_short",
        F.when(
            F.trim(F.regexp_replace(F.trim(F.col("ev_team_short_raw")), r"\.+$", "")) != "",
            F.trim(F.regexp_replace(F.trim(F.col("ev_team_short_raw")), r"\.+$", ""))
        ).otherwise(F.lit(None).cast(StringType()))
    ).drop("by_team", "ev_team_short_raw")

    # Step B: Resolve only contest-local aliases, retaining the source team ID fallback.
    df_b = df_a.withColumn(
        "team_id", F.coalesce(_lookup_team(F.col("event_team_short")), F.col("stat_team_id").cast(LongType())),
    )

    df_b = df_b.withColumn(
        "event_team_is_home",
        F.when(F.col("team_id").isNull(), F.lit(None).cast(BooleanType()))
        .when(F.col("team_id") == F.col("home_tid"), F.lit(True))
        .when(F.col("team_id") == F.col("away_tid"), F.lit(False))
        .otherwise(F.lit(None).cast(BooleanType()))
    ).withColumn(
        "opponent_team_id",
        F.when(
            F.col("team_id").isNotNull() & F.col("home_tid").isNotNull() & F.col("away_tid").isNotNull(),
            F.when(F.col("team_id") == F.col("home_tid"), F.col("away_tid").cast(LongType()))
            .when(F.col("team_id") == F.col("away_tid"), F.col("home_tid").cast(LongType()))
            .otherwise(F.lit(None).cast(LongType()))
        ).otherwise(F.lit(None).cast(LongType()))
    ).withColumn(
        "possession_team_id",
        F.when(
            F.col("norm_text").startswith("GOAL by")
            | F.col("norm_text").startswith("Shot by")
            | F.col("norm_text").startswith("Turnover by")
            | F.col("norm_text").startswith("Faceoff")
            | F.col("norm_text").startswith("Ground ball")
            | F.col("norm_text").startswith("Clear attempt by"),
            F.col("team_id")
        ).otherwise(F.lit(None).cast(LongType()))
    ).withColumn(
        "event_team_margin",
        F.when(F.col("event_team_is_home") == True, F.col("home_score_margin"))
        .when(F.col("event_team_is_home") == False, -F.col("home_score_margin"))
        .otherwise(F.lit(None)),
    )

    # Step C: Lookahead forward window: next possession-indicating play within the same period
    next_poss_window = Window.partitionBy("contest_id", "period_number").orderBy("play_seq").rowsBetween(
        1, Window.unboundedFollowing
    )
    df_c = df_b.withColumn(
        "next_possession_team_id",
        F.first(F.col("possession_team_id"), ignorenulls=True).over(next_poss_window),
    ).withColumn(
        "shot_possession_retained",
        F.when(
            F.col("event_type") == "SHOT",
            F.coalesce(F.col("possession_team_id") == F.col("next_possession_team_id"), F.lit(False)),
        ).otherwise(F.lit(None)),
    )

    # Step D: Event-specific details and player attribution
    def clean_pbp_player(raw_col: F.Column) -> F.Column:
        stripped = F.regexp_replace(F.trim(raw_col), r"\.+$", "")
        return clean_player_name_native(stripped)

    norm_text_col = F.col("norm_text")
    norm_text_no_dot = F.regexp_replace(norm_text_col, r"\.+$", "")

    shot_res = (
        F.when(norm_text_col.startswith("GOAL by"), F.lit("GOAL"))
        .when(
            norm_text_col.startswith("Shot by"),
            F.when(norm_text_col.contains(", SAVE ") | norm_text_col.contains(", SAVE,") | norm_text_col.contains("SAVE by"), F.lit("SAVE"))
            .when(norm_text_no_dot.endswith(" WIDE"), F.lit("WIDE"))
            .when(norm_text_no_dot.endswith(" HIGH"), F.lit("HIGH"))
            .when(norm_text_no_dot.endswith(" HIT POST"), F.lit("POST"))
            .when(norm_text_no_dot.endswith(" HIT CROSSBAR"), F.lit("CROSSBAR"))
            .when(norm_text_no_dot.endswith(" BLOCKED"), F.lit("BLOCKED"))
            .otherwise(F.lit(None).cast(StringType()))
        ).otherwise(F.lit(None).cast(StringType()))
    )

    primary_raw = (
        F.when(norm_text_col.startswith("GOAL by"), _extract_team_text(norm_text_col, r"^GOAL by ", r"\s+(.+?)(?:,\s*Assist by|\s*\([^)]+\)|,\s*goal number|\.?$)"))
        .when(norm_text_col.startswith("Shot by"), _extract_team_text(norm_text_col, r"^Shot by ", r"\s+(.+?)(?:,\s*SAVE|,\s*TEAM SAVE|\s+HIGH|\s+WIDE|\s+HIT POST|\s+HIT CROSSBAR|\s+BLOCKED|\.?$)"))
        .when(
            norm_text_col.startswith("Turnover by"),
            F.when(F.trim(_extract_team_text(norm_text_col, r"^Turnover by ", r"(?:\s+(.+?)(?:\s*\([^)]+\)|\.?$))?")) != "",
                   _extract_team_text(norm_text_col, r"^Turnover by ", r"(?:\s+(.+?)(?:\s*\([^)]+\)|\.?$))?")).otherwise(F.lit("TEAM"))
        )
        .when(norm_text_col.startswith("Ground ball"), _extract_team_text(norm_text_col, r"^Ground ball pickup by ", r"\s+(.+?)\.?$"))
        .when(norm_text_col.startswith("Penalty on"), _extract_team_text(norm_text_col, r"^Penalty on ", r"\s+([^(]+)"))
        .when(norm_text_col.contains("at goalie for"), _extract_team_text(norm_text_col, r"^(.+?)\s+at goalie for "))
        .otherwise(F.lit(None).cast(StringType()))
    )

    secondary_raw = (
        F.when(norm_text_col.contains(", Assist by "), F.regexp_extract(norm_text_col, r",\s*Assist by (.+?)(?:,\s*goal number|$)", 1))
        .when(norm_text_col.contains(", SAVE "), F.regexp_extract(norm_text_col, r",\s*SAVE\s+(?:by\s+)?(.+?)\.?$", 1))
        .otherwise(F.lit(None).cast(StringType()))
    )

    caused_by_raw = F.when(
        norm_text_col.contains("(caused by "),
        F.regexp_extract(norm_text_col, r"\(caused by ([^)]+)\)", 1)
    ).otherwise(F.lit(None).cast(StringType()))

    is_fo = norm_text_col.startswith("Faceoff")

    df_d = df_c.withColumn(
        "shot_result", shot_res
    ).withColumn(
        "primary_player_name", clean_pbp_player(primary_raw)
    ).withColumn(
        "secondary_player_name", clean_pbp_player(secondary_raw)
    ).withColumn(
        "caused_by_player_name", clean_pbp_player(caused_by_raw)
    ).withColumn(
        "caused_by_team_id",
        F.when(F.col("caused_by_player_name").isNotNull(), F.col("opponent_team_id")).otherwise(F.lit(None).cast(LongType()))
    ).withColumn(
        "is_faceoff_violation",
        F.when(is_fo, norm_text_col.contains("on faceoff violation")).otherwise(F.lit(None).cast(BooleanType()))
    ).withColumn(
        "penalty_type",
        F.when(
            norm_text_col.startswith("Penalty on") & (F.regexp_extract(norm_text_col, r"\(([^/]+)/\d+:\d+\)", 1) != ""),
            F.trim(F.regexp_extract(norm_text_col, r"\(([^/]+)/\d+:\d+\)", 1))
        ).otherwise(F.lit(None).cast(StringType()))
    ).withColumn(
        "penalty_duration_seconds",
        F.when(
            norm_text_col.startswith("Penalty on")
            & (F.regexp_extract(norm_text_col, r"\([^/]+/(\d+):(\d+)\)", 1) != "")
            & (F.regexp_extract(norm_text_col, r"\([^/]+/(\d+):(\d+)\)", 2) != ""),
            F.regexp_extract(norm_text_col, r"\([^/]+/(\d+):(\d+)\)", 1).cast(IntegerType()) * 60
            + F.regexp_extract(norm_text_col, r"\([^/]+/(\d+):(\d+)\)", 2).cast(IntegerType())
        ).otherwise(F.lit(None).cast(IntegerType()))
    ).withColumn(
        "is_extra_man_opportunity",
        F.when(norm_text_col.startswith("Penalty on"), norm_text_col.contains("Extra-man opportunity")).otherwise(F.lit(None).cast(BooleanType()))
    ).withColumn(
        "clear_result",
        F.when(
            norm_text_col.startswith("Clear attempt by"),
            F.when(norm_text_col.contains(" good"), F.lit("GOOD")).otherwise(F.lit("FAILED"))
        ).otherwise(F.lit(None).cast(StringType()))
    ).drop("norm_text")
    df_d = _attribute_faceoff_players(df_d)

    # Ingest timestamp mapping (broadcast lookup for multi-game, literal for single-game)
    if len(snapshot_list) == 1:
        df_final = df_d.withColumn("ingest_timestamp", F.lit(snapshot_list[0][1]))
    else:
        ts_data = list({(cid, ts) for cid, ts, _ in snapshot_list})
        ts_df = spark.createDataFrame(ts_data, ["c_id", "ingest_timestamp"])
        df_final = df_d.join(F.broadcast(ts_df), df_d.contest_id == ts_df.c_id, "left").drop("c_id")

    df_final = df_final.withColumn(
        "play_id",
        F.concat(F.col("contest_id").cast("string"), F.lit("_"), F.col("play_seq").cast("string")),
    )

    # Project the 34 structured Silver columns
    projected_silver_df = df_final.select(
        F.col("play_id"),
        F.col("contest_id"),
        F.col("play_seq"),
        F.col("ingest_timestamp"),
        F.col("period_number"),
        F.col("period_display"),
        F.col("clock_display"),
        F.col("period_seconds_remaining"),
        F.col("game_seconds_elapsed"),
        F.col("game_seconds_remaining_reg"),
        F.col("event_type"),
        F.col("play_text"),
        F.col("team_id"),
        F.col("event_team_short"),
        F.col("event_team_is_home"),
        F.col("opponent_team_id"),
        F.col("possession_team_id"),
        F.col("primary_player_name"),
        F.col("secondary_player_name"),
        F.col("caused_by_player_name"),
        F.col("caused_by_team_id"),
        F.col("shot_result"),
        F.col("shot_possession_retained"),
        F.col("faceoff_winner_player"),
        F.col("faceoff_loser_player"),
        F.col("is_faceoff_violation"),
        F.col("penalty_type"),
        F.col("penalty_duration_seconds"),
        F.col("is_extra_man_opportunity"),
        F.col("clear_result"),
        F.col("running_home_score"),
        F.col("running_visitor_score"),
        F.col("home_score_margin"),
        F.col("event_team_margin"),
    )

    return projected_silver_df



def _embedded_ground_balls(silver_df: DataFrame, dim_teams: DataFrame) -> DataFrame:
    """Represent a faceoff's explicit ground ball as an aggregate-only event.

    Resolve the collector's team against the two contest participants rather
    than assuming it is the faceoff winner. Facts keep the original play rows.
    """
    # Silver's resolved event labels provide evidence even when metadata changes.
    faceoffs = silver_df.withColumn("_observed_aliases", F.collect_set(
        _observed_team_alias(F.col("event_team_short"), F.col("team_id")),
    ).over(Window.partitionBy("contest_id"))).filter(F.col("event_type") == "FACEOFF").select(
        "contest_id", "team_id", "opponent_team_id", "play_text", "_observed_aliases",
    )
    for source, prefix in (("team_id", "_event"), ("opponent_team_id", "_opponent")):
        metadata = dim_teams.select(
            F.col("team_id").alias(f"{prefix}_id"),
            F.col("name_short").alias(f"{prefix}_short"),
            F.col("name_6char").alias(f"{prefix}_6char"),
            F.col("name_full").alias(f"{prefix}_full"),
        )
        faceoffs = faceoffs.join(
            F.broadcast(metadata), F.col(source) == F.col(f"{prefix}_id"), "left",
        )
    faceoffs = faceoffs.withColumn("_team_aliases", F.array_distinct(F.concat(
        _metadata_team_aliases(F.col("team_id"), F.col("_event_short"),
                               F.col("_event_6char"), F.col("_event_full")),
        _metadata_team_aliases(F.col("opponent_team_id"), F.col("_opponent_short"),
                               F.col("_opponent_6char"), F.col("_opponent_full")),
        F.filter("_observed_aliases", lambda alias: (
            (alias["team_id"] == F.col("team_id")) | (alias["team_id"] == F.col("opponent_team_id"))
        )),
    )))
    faceoffs = _with_team_pattern(faceoffs)
    _, code = _faceoff_ground_ball(F.col("play_text"))
    faceoffs = faceoffs.withColumn("_gb_code", code).filter(F.col("_gb_code") != "")
    collector_team = _lookup_team(F.col("_gb_code"))
    return faceoffs.select(
        "contest_id", collector_team.alias("team_id"),
        F.col("_gb_code").alias("event_team_short"),
        F.lit("GROUND_BALL").alias("event_type"),
    ).filter(F.col("team_id").isNotNull())


def generate_gold_tables(
    spark: SparkSession,
    silver_df: DataFrame,
    dim_teams: DataFrame,
    dim_contests: DataFrame,
) -> Dict[str, DataFrame]:
    """Build Gold facts and aggregates from the current Silver data."""
    validate_dimension_keys(dim_teams, "team_id")
    validate_dimension_keys(dim_contests, "contest_id")

    shot_plays = silver_df.filter(F.col("event_type") == "SHOT")
    stats = shot_plays.agg(
        F.count(F.lit(1)).alias("total"),
        F.count(F.when(F.col("shot_possession_retained") == True, 1)).alias("retained"),
    ).first()
    total = (stats["total"] or 0) if stats else 0
    retained = (stats["retained"] or 0) if stats else 0
    overall_default_loss_rate = 1.0 - (retained / total) if total > 0 else 0.5

    baseline_rates_df = shot_plays.filter(F.col("shot_result").isNotNull()).groupBy("shot_result").agg(
        (F.lit(1.0) - (F.count(F.when(F.col("shot_possession_retained") == True, 1)) * 1.0 / F.count(F.lit(1)))).alias("expected_loss_rate")
    )

    silver_annotated = (
        silver_df.join(F.broadcast(baseline_rates_df), on="shot_result", how="left")
        .withColumn(
            "expected_shot_loss_prob",
            F.when(F.col("event_type") != "SHOT", F.lit(0.0))
            .otherwise(F.coalesce(F.col("expected_loss_rate"), F.lit(float(overall_default_loss_rate))))
        )
        .drop("expected_loss_rate")
    )

    stat_plays = silver_annotated.unionByName(
        _embedded_ground_balls(silver_df, dim_teams), allowMissingColumns=True,
    )
    agg_team_game_stats = stat_plays.filter(F.col("team_id").isNotNull()).groupBy(
        "contest_id", "team_id"
    ).agg(
        F.min(F.when(F.trim("event_team_short") != "", F.trim("event_team_short"))).alias("_fallback_team_short"),
        F.count(F.when(F.col("event_type").isin("SHOT", "GOAL"), 1)).alias("total_shots"),
        F.count(F.when(F.col("event_type") == "GOAL", 1)).alias("goals"),
        F.count(F.when((F.col("event_type") == "SHOT") & (F.col("shot_possession_retained") == True), 1)).alias("shots_retained"),
        F.count(F.when((F.col("event_type") == "SHOT") & (F.col("shot_possession_retained") == False), 1)).alias("realized_shots_lost"),
        F.round(F.sum(F.when(F.col("event_type") == "SHOT", F.col("expected_shot_loss_prob"))), 4).alias("normalized_shots_lost"),
        F.count(F.when(F.col("shot_result") == "SAVE", 1)).alias("saves_faced"),
        F.count(F.when(F.col("event_type") == "GROUND_BALL", 1)).alias("ground_balls"),
        F.count(F.when(F.col("event_type") == "TURNOVER", 1)).alias("turnovers"),
        F.count(F.when((F.col("event_type") == "CLEAR") & (F.col("clear_result") == "GOOD"), 1)).alias("clears_good"),
        F.count(F.when((F.col("event_type") == "CLEAR") & (F.col("clear_result") == "FAILED"), 1)).alias("clears_failed"),
        F.count(F.when(F.col("event_type") == "PENALTY", 1)).alias("penalties"),
        F.sum(F.when(F.col("event_type") == "PENALTY", F.col("penalty_duration_seconds"))).alias("penalty_seconds")
    ).withColumn(
        "realized_shot_possessions_used",
        F.col("goals") + F.col("realized_shots_lost")
    ).withColumn(
        "normalized_shot_possessions_used",
        F.round(F.col("goals") + F.col("normalized_shots_lost"), 4)
    ).withColumn(
        "shooting_pct",
        F.round(
            F.when(F.col("total_shots") > 0, F.col("goals") / F.col("total_shots")).otherwise(F.lit(0.0)),
            4
        )
    ).withColumn(
        "realized_shooting_efficiency",
        F.round(
            F.when(F.col("realized_shot_possessions_used") > 0, F.col("goals") / F.col("realized_shot_possessions_used")).otherwise(F.lit(0.0)),
            4
        )
    ).withColumn(
        "normalized_shooting_efficiency",
        F.round(
            F.when(F.col("normalized_shot_possessions_used") > 0, F.col("goals") / F.col("normalized_shot_possessions_used")).otherwise(F.lit(0.0)),
            4
        )
    )
    team_labels = dim_teams.select(
        "team_id",
        F.when(F.trim("name_short") != "", F.trim("name_short")).alias("_canonical_team_short"),
    )
    agg_team_game_stats = (
        agg_team_game_stats.join(F.broadcast(team_labels), "team_id", "left")
        .withColumn("team_short", F.coalesce(
            F.col("_canonical_team_short"), F.col("_fallback_team_short"), F.col("team_id").cast("string"),
        ))
        .drop("_canonical_team_short", "_fallback_team_short")
    )

    # The validated team dimension is small enough for a broadcast lookup join.
    fact_plays = silver_df.join(
        F.broadcast(dim_teams.select("team_id", "color", "name_full")),
        on="team_id",
        how="left",
    )

    return {
        "dim_teams": dim_teams,
        "dim_contests": dim_contests,
        "fact_plays": fact_plays,
        "agg_team_game_stats": agg_team_game_stats,
    }

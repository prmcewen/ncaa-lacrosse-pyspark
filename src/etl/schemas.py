from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Raw NCAA JSON Nested Schema
PLAY_ITEM_SCHEMA = StructType([
    StructField("clock", StringType(), True),
    StructField("playText", StringType(), True),
    StructField("homeScore", IntegerType(), True),
    StructField("visitorScore", IntegerType(), True),
])

PLAY_BY_PLAY_STAT_SCHEMA = StructType([
    StructField("clock", StringType(), True),
    StructField("teamId", LongType(), True),
    StructField("plays", ArrayType(PLAY_ITEM_SCHEMA), True),
])

PERIOD_SCHEMA = StructType([
    StructField("periodNumber", IntegerType(), True),
    StructField("periodDisplay", StringType(), True),
    StructField("playbyplayStats", ArrayType(PLAY_BY_PLAY_STAT_SCHEMA), True),
])

TEAM_SCHEMA = StructType([
    StructField("teamId", StringType(), True),
    StructField("nameShort", StringType(), True),
    StructField("nameFull", StringType(), True),
    StructField("name6Char", StringType(), True),
    StructField("seoname", StringType(), True),
    StructField("color", StringType(), True),
    StructField("isHome", BooleanType(), True),
])

NCAA_PBP_SCHEMA = StructType([
    StructField("data", StructType([
        StructField("playbyplay", StructType([
            StructField("contestId", LongType(), True),
            StructField("title", StringType(), True),
            StructField("status", StringType(), True),
            StructField("teams", ArrayType(TEAM_SCHEMA), True),
            StructField("periods", ArrayType(PERIOD_SCHEMA), True),
        ]), True)
    ]), True)
])


# Narrow Bronze projection for contest and team metadata. Period plays are not parsed.
NCAA_CONTEST_SCHEMA = StructType([
    StructField("data", StructType([
        StructField("playbyplay", StructType([
            StructField("contestId", LongType(), True),
            StructField("title", StringType(), True),
            StructField("status", StringType(), True),
            StructField("teams", ArrayType(TEAM_SCHEMA), True),
        ]), True),
    ]), True),
])

# 34-Column Approved Silver Table Schema
SILVER_PLAY_SCHEMA = StructType([
    # Identification & Lineage
    StructField("play_id", StringType(), False),
    StructField("contest_id", LongType(), False),
    StructField("play_seq", IntegerType(), False),
    StructField("ingest_timestamp", StringType(), False),

    # Chronology
    StructField("period_number", IntegerType(), False),
    StructField("period_display", StringType(), False),
    StructField("clock_display", StringType(), False),
    StructField("period_seconds_remaining", IntegerType(), False),
    StructField("game_seconds_elapsed", IntegerType(), False),
    StructField("game_seconds_remaining_reg", IntegerType(), False),

    # Event Classification
    StructField("event_type", StringType(), False),
    StructField("play_text", StringType(), False),

    # Teams & Perspective
    StructField("team_id", LongType(), True),
    StructField("event_team_short", StringType(), True),
    StructField("event_team_is_home", BooleanType(), True),
    StructField("opponent_team_id", LongType(), True),
    StructField("possession_team_id", LongType(), True),

    # Cleaned Player Attribution
    StructField("primary_player_name", StringType(), True),
    StructField("secondary_player_name", StringType(), True),
    StructField("caused_by_player_name", StringType(), True),
    StructField("caused_by_team_id", LongType(), True),

    # Event-Specific Details
    StructField("shot_result", StringType(), True),
    StructField("shot_possession_retained", BooleanType(), True),
    StructField("faceoff_winner_player", StringType(), True),
    StructField("faceoff_loser_player", StringType(), True),
    StructField("is_faceoff_violation", BooleanType(), True),
    StructField("penalty_type", StringType(), True),
    StructField("penalty_duration_seconds", IntegerType(), True),
    StructField("is_extra_man_opportunity", BooleanType(), True),
    StructField("clear_result", StringType(), True),

    # Windowed Running Game State
    StructField("running_home_score", IntegerType(), False),
    StructField("running_visitor_score", IntegerType(), False),
    StructField("home_score_margin", IntegerType(), False),
    StructField("event_team_margin", IntegerType(), True),
])

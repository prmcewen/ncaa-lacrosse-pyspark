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

"""Regression checks for team-game grain and compound faceoff descriptions."""
import pytest
from pyspark.sql import functions as F

from src.etl.transform import (
    extract_dimensions_from_silver_contests,
    generate_gold_tables,
    get_latest_valid_snapshots,
    process_bronze_to_silver,
    process_bronze_to_silver_contests,
)


def _gold(spark, plays, teams):
    silver = spark.createDataFrame(plays,
        "play_id string, contest_id long, team_id long, event_team_short string, "
        "opponent_team_id long, event_type string, play_text string, shot_result string, "
        "shot_possession_retained boolean, clear_result string, penalty_duration_seconds long",
    )
    dimensions = spark.createDataFrame(teams,
        "team_id long, name_short string, name_6char string, name_full string, color string",
    )
    contests = silver.select("contest_id").distinct().withColumn(
        "title", F.lit("Test contest"),
    ).withColumn("status", F.lit("final"))
    return generate_gold_tables(spark, silver, dimensions, contests)


def test_bundled_game_includes_faceoff_ground_balls(spark):
    snapshot = next(s for s in get_latest_valid_snapshots() if s[0] == 6599996)
    silver = process_bronze_to_silver(spark, snapshot).cache()
    try:
        teams, contests = extract_dimensions_from_silver_contests(
            process_bronze_to_silver_contests(spark, [snapshot])
        )
        gold = generate_gold_tables(spark, silver, teams, contests)
        stats = {row.team_id: row for row in gold["agg_team_game_stats"].collect()}
        assert set(stats) == {43731, 43861}
        assert stats[43731].ground_balls == 46  # 34 standalone + 12 embedded
        assert stats[43861].ground_balls == 30  # 19 standalone + 11 embedded
        assert stats[43731].team_short == "Princeton"
        assert stats[43861].team_short == "Notre Dame"
        assert gold["fact_plays"].count() == 259
        assert gold["fact_plays"].filter("event_type = 'FACEOFF'").count() == 30
    finally:
        silver.unpersist()


def test_embedded_ground_ball_credits_collector_team_without_extra_faceoffs(spark):
    texts = [
        "Faceoff Alex Adams vs Ben Baker won by PU, [15:00] Ground ball pickup by PU Alex Adams.",
        "Faceoff Alex Adams vs Ben Baker won by PU, Ground ball pickup by NOTRE DA Ben Baker.",
        "Faceoff Alex Adams vs Ben Baker won by PU (on faceoff violation).",
        "Faceoff Alex Adams vs Ben Baker won by PU, Ground ball pickup by OTHER Unknown Player.",
    ]
    plays = [
        (f"1_{i}", 1, 43731, "PU", 43861, "FACEOFF", text, None, None, None, None)
        for i, text in enumerate(texts)
    ]
    plays.append(("1_4", 1, 43731, "PRIN", 43861, "GROUND_BALL",
                  "Ground ball pickup by PRIN Alex Adams.", None, None, None, None))
    gold = _gold(spark, plays, [
        (43731, "Princeton", "PRINCE", "Princeton University", "orange"),
        (43861, "Notre Dame", "N DAME", "University of Notre Dame", "blue"),
    ])
    stats = {row.team_id: row for row in gold["agg_team_game_stats"].collect()}
    assert stats[43731].ground_balls == 2
    # This team has only an embedded ground ball, so it must still get a row.
    assert stats[43861].ground_balls == 1
    assert all(row.total_shots == row.goals == row.turnovers == 0 for row in stats.values())
    assert gold["fact_plays"].count() == len(plays)


def test_aliases_and_missing_labels_share_one_team_game_row(spark):
    gold = _gold(spark, [
        ("1_1", 1, 10, "A", 20, "SHOT", "Shot by A Alex WIDE.", "WIDE", True, None, None),
        ("1_2", 1, 10, "ALIAS", 20, "GOAL", "GOAL by ALIAS Alex.", "GOAL", None, None, None),
        ("1_3", 1, 10, None, 20, "TURNOVER", "Unsupported team label", None, None, None, None),
        ("1_4", 1, 20, "B", 10, "SHOT", "Shot by B Ben WIDE.", "WIDE", False, None, None),
        ("2_1", 2, 10, "ALIAS", 20, "GOAL", "GOAL by ALIAS Alex.", "GOAL", None, None, None),
    ], [
        (10, "Canonical A", "A", "Team A", "red"),
        (20, "Canonical B", "B", "Team B", "blue"),
    ])
    rows = gold["agg_team_game_stats"].collect()
    stats = {(row.contest_id, row.team_id): row for row in rows}
    assert len(rows) == len(stats) == 3
    team_a = stats[(1, 10)]
    assert team_a.team_short == "Canonical A"
    assert (team_a.total_shots, team_a.goals, team_a.shots_retained, team_a.turnovers) == (2, 1, 1, 1)
    assert team_a.shooting_pct == 0.5
    assert team_a.normalized_shots_lost == 0.5
    assert stats[(1, 20)].team_short == "Canonical B"
    assert sum(row.realized_shooting_efficiency for row in rows) / len(rows) == pytest.approx(2 / 3)


@pytest.mark.parametrize("labels,expected", [(["Z", "A"], "A"), ([None, None], "10")])
def test_missing_dimension_label_has_deterministic_fallback(spark, labels, expected):
    plays = [
        (f"1_{i}", 1, 10, label, 20, "GROUND_BALL", "Ground ball text", None, None, None, None)
        for i, label in enumerate(labels)
    ]
    gold = _gold(spark, plays, [(10, None, None, "Team A", "red")])
    rows = gold["agg_team_game_stats"].collect()
    assert len(rows) == 1
    assert rows[0].team_short == expected
    assert rows[0].ground_balls == 2


def test_corrected_team_game_stats_reach_rest_graphql_and_averages(spark, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from src.api.main import app
    from src.db import duckdb_client as database
    from src.db.gold_storage import publish_gold_tables

    gold = _gold(spark, [
        ("1_1", 1, 10, "HOME", 20, "SHOT", "Shot by HOME Alex WIDE.", "WIDE", True, None, None),
        ("1_2", 1, 10, "ALIAS", 20, "GOAL", "GOAL by ALIAS Alex.", "GOAL", None, None, None),
        ("1_3", 1, 20, "AWAY", 10, "SHOT", "Shot by AWAY Ben WIDE.", "WIDE", False, None, None),
        ("1_4", 1, 10, "HOME", 20, "FACEOFF",
         "Faceoff Alex Adams vs Ben Baker won by HOME, Ground ball pickup by AWAY Ben Baker.",
         None, None, None, None),
        ("1_5", 1, 10, None, 20, "GROUND_BALL", "Ground ball pickup by HOME Alex Adams.",
         None, None, None, None),
    ], [
        (10, "Home Team", "HOME", "Home Team", "red"),
        (20, "Away Team", "AWAY", "Away Team", "blue"),
    ])
    gold["fact_plays"] = gold["fact_plays"].withColumn("primary_player_name", F.lit("Alex Adams"))
    publish_gold_tables(gold, tmp_path)
    monkeypatch.setattr(database, "GOLD_DIR", tmp_path)
    with TestClient(app) as client:
        response = client.get("/api/contests/1/summary")
        assert response.status_code == 200
        stats = {row["team_id"]: row for row in response.json()["team_stats"]}
        assert len(stats) == 2
        assert stats[10]["total_shots"] == 2
        assert stats[10]["team_short"] == "Home Team"
        assert stats[10]["ground_balls"] == stats[20]["ground_balls"] == 1

        body = client.post("/graphql", json={"query":
            "{ contest(id: 1) { teamStats { teamId teamShort groundBalls totalShots } } }"
        }).json()
        assert not body.get("errors"), body
        graphql_stats = {row["teamId"]: row for row in body["data"]["contest"]["teamStats"]}
        assert len(graphql_stats) == 2
        assert graphql_stats[10]["teamShort"] == "Home Team"
        assert graphql_stats[10]["totalShots"] == 2
        assert graphql_stats[10]["groundBalls"] == graphql_stats[20]["groundBalls"] == 1

        response = client.get("/api/shooting-efficiency?contest_id=1&min_shots=1")
        assert response.status_code == 200
        assert response.json()["overall"]["average_team_game_realized_efficiency"] == 0.5

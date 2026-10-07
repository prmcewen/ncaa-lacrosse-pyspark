import pytest

from src.db.duckdb_client import DuckDBClient


@pytest.fixture(scope="module")
def db_client():
    return DuckDBClient()


def test_silver_table_schema_and_row_count(db_client):
    rows = db_client.execute_query("SELECT count(*) as cnt FROM silver_plays WHERE contest_id = 6599996")
    assert len(rows) == 1
    assert rows[0]["cnt"] == 259
    total_rows = db_client.execute_query("SELECT count(*) as cnt FROM silver_plays")
    assert total_rows[0]["cnt"] >= 259


def test_forward_filled_scores(db_client):
    # Ensure scores are never null and monotonically non-decreasing
    rows = db_client.execute_query("""
        SELECT 
            min(running_home_score) as min_h, 
            max(running_home_score) as max_h,
            min(running_visitor_score) as min_v, 
            max(running_visitor_score) as max_v
        FROM silver_plays
        WHERE contest_id = 6599996
    """)
    assert rows[0]["min_h"] == 0
    assert rows[0]["max_h"] == 16
    assert rows[0]["min_v"] == 0
    assert rows[0]["max_v"] == 9

    # Across all games, minimum scores should start at 0
    all_rows = db_client.execute_query("""
        SELECT min(running_home_score) as min_h, min(running_visitor_score) as min_v
        FROM silver_plays
    """)
    assert all_rows[0]["min_h"] == 0
    assert all_rows[0]["min_v"] == 0


def test_faceoff_attribution(db_client):
    rows = db_client.execute_query("""
        SELECT 
            faceoff_winner_player, 
            faceoff_loser_player, 
            event_team_short,
            is_faceoff_violation
        FROM silver_plays
        WHERE event_type = 'FACEOFF' AND contest_id = 6599996
    """)
    assert len(rows) == 30
    for r in rows:
        assert r["faceoff_winner_player"] is not None
        assert r["faceoff_loser_player"] is not None
        assert r["event_team_short"] in ["ND", "PU", "PRINCE"]


def test_caused_turnover_query_situation(db_client):
    # Query: Caused turnovers when the defending team is down by 1 goal
    # In our game: Luke Miller turned it over, caused by Cooper Mueller when Princeton was down 3-2
    results = db_client.get_caused_turnovers_situational(
        contest_id=6599996,
        defending_team_down_by=1,
        min_period=1
    )
    assert len(results) >= 1
    sample = results[0]
    assert sample["caused_by_player"] == "Cooper Mueller"
    assert sample["turnover_committer"] == "Luke Miller"
    assert sample["defending_team_margin"] == -1
    assert sample["period_number"] == 1


def test_penalty_durations_and_nullability(db_client):
    # Extra man opportunity should be non-null for penalties and null for non-penalties
    penalties = db_client.execute_query("""
        SELECT penalty_type, penalty_duration_seconds, is_extra_man_opportunity
        FROM silver_plays
        WHERE event_type = 'PENALTY' AND contest_id = 6599996
    """)
    assert len(penalties) == 4
    for p in penalties:
        assert p["penalty_duration_seconds"] in [30, 60, 120, 180]
        assert p["is_extra_man_opportunity"] in [True, False]

    non_penalties = db_client.execute_query("""
        SELECT count(*) as cnt 
        FROM silver_plays 
        WHERE event_type != 'PENALTY' AND is_extra_man_opportunity IS NOT NULL
    """)
    assert non_penalties[0]["cnt"] == 0


def test_general_play_selector_situational_equivalence(db_client):
    # Verify that general get_plays can reproduce situational queries
    plays = db_client.get_plays(
        contest_id=6599996,
        event_type="TURNOVER",
        player_name="Cooper Mueller",
        min_period=1,
        max_period_seconds_remaining=900,
        event_team_margin_min=1,
        event_team_margin_max=1
    )
    assert len(plays) == 1
    play = plays[0]
    assert play["play_id"] == "6599996_45"
    assert play["caused_by_player_name"] == "Cooper Mueller"
    assert play["primary_player_name"] == "Luke Miller"
    assert play["event_team_margin"] == 1

    # Verify multiple event types and team filter
    nd_scoring_plays = db_client.get_plays(
        contest_id=6599996,
        event_types=["SHOT", "GOAL"],
        team="ND",
        max_game_seconds_remaining=600  # Last 10 minutes of regulation
    )
    assert len(nd_scoring_plays) >= 1
    for p in nd_scoring_plays:
        assert p["event_team_short"] == "ND"
        assert p["event_type"] in ["SHOT", "GOAL"]
        assert p["game_seconds_remaining_reg"] <= 600


def test_possession_team_id_attribution(db_client):
    # All indicating event types must have a non-null possession_team_id
    indicating_plays = db_client.execute_query("""
        SELECT count(*) as cnt
        FROM silver_plays
        WHERE event_type IN ('SHOT', 'GOAL', 'GROUND_BALL', 'FACEOFF', 'TURNOVER', 'CLEAR')
          AND possession_team_id IS NULL
    """)
    assert indicating_plays[0]["cnt"] == 0

    # Non-indicating event types (TIMEOUT, PENALTY, GOALIE_CHANGE, PERIOD_END) must have NULL possession_team_id
    non_indicating_plays = db_client.execute_query("""
        SELECT count(*) as cnt
        FROM silver_plays
        WHERE event_type IN ('TIMEOUT', 'PENALTY', 'GOALIE_CHANGE', 'PERIOD_END')
          AND possession_team_id IS NOT NULL
    """)
    assert non_indicating_plays[0]["cnt"] == 0

    # Test filtering by possession_team_id via db_client.get_plays
    nd_poss_plays = db_client.get_plays(possession_team_id=43861, contest_id=6599996)
    assert len(nd_poss_plays) >= 1
    for p in nd_poss_plays:
        assert p["possession_team_id"] == 43861
        assert p["event_type"] in ['SHOT', 'GOAL', 'GROUND_BALL', 'FACEOFF', 'TURNOVER', 'CLEAR']


def test_shot_possession_retained(db_client):
    # 1. 100% of SHOT events have a non-null boolean shot_possession_retained
    shot_nulls = db_client.execute_query("""
        SELECT count(*) as cnt
        FROM silver_plays
        WHERE event_type = 'SHOT' AND shot_possession_retained IS NULL
    """)
    assert shot_nulls[0]["cnt"] == 0

    # 2. 100% of non-SHOT events (including GOAL) must have NULL shot_possession_retained
    non_shot_non_nulls = db_client.execute_query("""
        SELECT count(*) as cnt
        FROM silver_plays
        WHERE event_type != 'SHOT' AND shot_possession_retained IS NOT NULL
    """)
    assert non_shot_non_nulls[0]["cnt"] == 0

    # 3. Verify shots with retained = True and retained = False exist
    retained_counts = db_client.execute_query("""
        SELECT 
            count(CASE WHEN shot_possession_retained = true THEN 1 END) as true_cnt,
            count(CASE WHEN shot_possession_retained = false THEN 1 END) as false_cnt
        FROM silver_plays
        WHERE event_type = 'SHOT'
    """)
    assert retained_counts[0]["true_cnt"] > 0
    assert retained_counts[0]["false_cnt"] > 0

    # 4. Period-ending shots must have shot_possession_retained = false
    buzzer_shots = db_client.execute_query("""
        SELECT shot_possession_retained
        FROM silver_plays
        WHERE play_id IN ('6599994_59', '6599994_181', '6599996_66', '6599996_130')
    """)
    assert len(buzzer_shots) == 4
    for s in buzzer_shots:
        assert s["shot_possession_retained"] is False

    # 5. db_client.get_plays filter by shot_possession_retained
    retained_shots = db_client.get_plays(event_type="SHOT", shot_possession_retained=True)
    assert len(retained_shots) >= 1
    for s in retained_shots:
        assert s["shot_possession_retained"] is True


def test_gold_shooting_efficiency_metrics(db_client):
    rows = db_client.execute_query("""
        SELECT contest_id, team_id, team_short, goals, total_shots, shots_retained,
               shooting_pct,
               realized_shots_lost, realized_shot_possessions_used, realized_shooting_efficiency,
               normalized_shots_lost, normalized_shot_possessions_used, normalized_shooting_efficiency
        FROM agg_team_game_stats
        ORDER BY contest_id, team_short
    """)
    assert len(rows) >= 4
    by_key = {(r["contest_id"], r["team_id"]): r for r in rows}

    # Verify contest 6599996 (ND vs PU)
    pu_stats = by_key[(6599996, 43731)]
    assert pu_stats["goals"] == 16
    assert pu_stats["shots_retained"] == 27
    assert pu_stats["realized_shots_lost"] == 10
    # Goals count as shots in total_shots (37 non-goal shots + 16 goals = 53 total shots)
    assert pu_stats["total_shots"] == 53
    assert pu_stats["shooting_pct"] == 0.3019
    assert pu_stats["realized_shot_possessions_used"] == 26
    assert pu_stats["realized_shooting_efficiency"] == 0.6154
    assert pu_stats["normalized_shots_lost"] > 0
    assert pu_stats["normalized_shot_possessions_used"] == round(pu_stats["goals"] + pu_stats["normalized_shots_lost"], 4)
    assert pu_stats["normalized_shooting_efficiency"] > 0

    nd_stats = by_key[(6599996, 43861)]
    assert nd_stats["goals"] == 9
    assert nd_stats["shots_retained"] == 16
    assert nd_stats["realized_shots_lost"] == 16
    # Goals count as shots in total_shots (32 non-goal shots + 9 goals = 41 total shots)
    assert nd_stats["total_shots"] == 41
    assert nd_stats["shooting_pct"] == 0.2195
    assert nd_stats["realized_shot_possessions_used"] == 25
    assert nd_stats["realized_shooting_efficiency"] == 0.3600
    assert nd_stats["normalized_shots_lost"] > 0
    assert nd_stats["normalized_shot_possessions_used"] == round(nd_stats["goals"] + nd_stats["normalized_shots_lost"], 4)
    assert nd_stats["normalized_shooting_efficiency"] > 0

    # Verify formula integrity on all rows:
    # 1. total_shots = shots_retained + realized_shots_lost + goals
    # 2. shooting_pct = round(goals / total_shots, 4)
    # 3. realized_shot_possessions_used = goals + realized_shots_lost
    # 4. realized_shooting_efficiency = round(goals / realized_shot_possessions_used, 4)
    # 5. normalized shooting efficiency = round(goals / normalized_shot_possessions_used, 4)
    for r in rows:
        assert r["total_shots"] == r["shots_retained"] + r["realized_shots_lost"] + r["goals"]
        assert r["realized_shot_possessions_used"] == r["goals"] + r["realized_shots_lost"]
        if r["total_shots"] > 0:
            assert r["shooting_pct"] == round(r["goals"] / r["total_shots"], 4)
        if r["realized_shot_possessions_used"] > 0:
            assert abs(r["realized_shooting_efficiency"] - (r["goals"] / r["realized_shot_possessions_used"])) <= 0.0001
        if r["normalized_shot_possessions_used"] > 0:
            expected_norm_eff = round(r["goals"] / r["normalized_shot_possessions_used"], 4)
            assert abs(r["normalized_shooting_efficiency"] - expected_norm_eff) <= 0.0001


def test_player_last_first_canonicalization(db_client):
    # Verify that players with 'Last, First' formatting in raw play text
    # (e.g. 'GOAL by PU Palumbo, Chad...') are properly canonicalized to 'First Last'
    # across both goals and shots, rather than getting isolated by only last name.
    chad_events = db_client.execute_query("""
        SELECT event_type, count(*) as cnt 
        FROM fact_plays 
        WHERE primary_player_name = 'Chad Palumbo'
        GROUP BY event_type
    """)
    event_counts = {r["event_type"]: r["cnt"] for r in chad_events}
    assert event_counts.get("GOAL", 0) == 12
    assert event_counts.get("SHOT", 0) == 23

    # Ensure uncanonicalized 'Palumbo' is absent
    unconverted = db_client.execute_query("""
        SELECT count(*) as cnt 
        FROM fact_plays 
        WHERE primary_player_name = 'Palumbo'
    """)
    assert unconverted[0]["cnt"] == 0

    # Ensure shots and goals have proper attribution for other 'Last, First' players
    colin_burns = db_client.execute_query("""
        SELECT event_type, count(*) as cnt 
        FROM fact_plays 
        WHERE primary_player_name = 'Colin Burns'
        GROUP BY event_type
    """)
    burns_counts = {r["event_type"]: r["cnt"] for r in colin_burns}
    assert burns_counts.get("GOAL", 0) > 0
    assert burns_counts.get("SHOT", 0) > 0


def test_multi_word_team_and_player_parsing(db_client):
    # Verify multi-word teams (North Carolina, Robert Morris, Stony Brook, Penn State, Army West Point, Notre Dame)
    # are parsed as full prefixes ('NORTH CA', 'ROBERT M', 'STONY BR', 'PENN ST', 'ARMY WES', 'NOTRE DA')
    # and their second words are not prepended to player names.
    pietramala_plays = db_client.execute_query("""
        SELECT event_type, event_team_short, primary_player_name, secondary_player_name
        FROM fact_plays
        WHERE play_text LIKE '%NORTH CA Dominic Pietramala%'
    """)
    assert len(pietramala_plays) > 0
    for p in pietramala_plays:
        assert p["primary_player_name"] == "Dominic Pietramala"
        assert p["event_team_short"] == "NORTH CA"

    # Ensure no players have team suffixes leaked into their names
    corrupted_players = db_client.execute_query("""
        SELECT count(*) as cnt
        FROM fact_plays
        WHERE primary_player_name LIKE 'CA %'
           OR primary_player_name LIKE 'M %'
           OR primary_player_name LIKE 'BR %'
           OR primary_player_name LIKE 'ST %'
           OR primary_player_name LIKE 'WES %'
           OR primary_player_name LIKE 'DA %'
    """)
    assert corrupted_players[0]["cnt"] == 0

    # Ensure single-word truncations are absent
    truncated_teams = db_client.execute_query("""
        SELECT count(*) as cnt
        FROM fact_plays
        WHERE event_team_short IN ('NORTH', 'ROBERT', 'STONY', 'PENN', 'ARMY', 'NOTRE')
    """)
    assert truncated_teams[0]["cnt"] == 0

    # Ensure all 6 multi-word team abbreviations are properly captured
    multi_teams = db_client.execute_query("""
        SELECT DISTINCT event_team_short
        FROM fact_plays
        WHERE event_team_short IN ('NORTH CA', 'ROBERT M', 'STONY BR', 'PENN ST', 'ARMY WES', 'NOTRE DA')
    """)
    found_teams = {r["event_team_short"] for r in multi_teams}
    assert found_teams == {"NORTH CA", "ROBERT M", "STONY BR", "PENN ST", "ARMY WES", "NOTRE DA"}

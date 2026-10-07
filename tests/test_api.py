import pytest
from fastapi.testclient import TestClient

from src.api.main import app


@pytest.fixture
def client():
    return TestClient(app)


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_contests_rest(client):
    r = client.get("/api/contests")
    assert r.status_code == 200
    contests = r.json()
    assert len(contests) >= 1
    contest_ids = [c["contest_id"] for c in contests]
    assert 6599996 in contest_ids


def test_contest_summary_rest(client):
    r = client.get("/api/contests/6599996/summary")
    assert r.status_code == 200
    data = r.json()
    assert data["contest_id"] == 6599996
    assert "team_stats" in data
    assert len(data["team_stats"]) == 2


def test_plays_filtering_rest(client):
    r = client.get("/api/contests/6599996/plays?event_type=GOAL")
    assert r.status_code == 200
    goals = r.json()
    assert len(goals) == 25
    for g in goals:
        assert g["event_type"] == "GOAL"
        assert g["shot_result"] == "GOAL"


def test_clutch_caused_turnovers_via_plays_rest(client):
    r = client.get("/api/contests/6599996/plays?event_type=TURNOVER&player_name=Mueller&min_period=1")
    assert r.status_code == 200
    plays = r.json()
    assert len(plays) >= 1
    assert plays[0]["event_type"] == "TURNOVER"
    assert plays[0]["caused_by_player_name"] == "Cooper Mueller"


def test_graphql_query(client):
    query = """
    query {
        contests {
            contestId
            title
            status
            teamStats {
                teamShort
                goals
                totalShots
                turnovers
            }
        }
    }
    """
    r = client.post("/graphql", json={"query": query})
    assert r.status_code == 200
    data = r.json().get("data", {})
    assert "contests" in data
    contest_ids = [c["contestId"] for c in data["contests"]]
    assert 6599996 in contest_ids
    target = next(c for c in data["contests"] if c["contestId"] == 6599996)
    assert len(target["teamStats"]) == 2


def test_general_plays_rest_selector(client):
    # Test filtering by player name and event type across all games
    r = client.get("/api/plays?player_name=Mueller&event_type=TURNOVER")
    assert r.status_code == 200
    plays = r.json()
    assert len(plays) >= 1
    assert plays[0]["event_type"] == "TURNOVER"
    assert "Mueller" in (plays[0]["caused_by_player_name"] or "")

    # Test filtering by team and time left in game
    r2 = client.get("/api/plays?team=ND&max_game_seconds_remaining=1800&event_type=GOAL")
    assert r2.status_code == 200
    nd_goals = r2.json()
    assert len(nd_goals) >= 1
    for g in nd_goals:
        assert g["event_team_short"] == "ND"
        assert g["game_seconds_remaining_reg"] <= 1800

    # Test filtering by time left in quarter
    r3 = client.get("/api/plays?max_period_seconds_remaining=60")
    assert r3.status_code == 200
    late_quarter_plays = r3.json()
    assert len(late_quarter_plays) >= 1
    for p in late_quarter_plays:
        assert p["period_seconds_remaining"] <= 60


def test_graphql_general_play_selector_direct_args(client):
    query = """
    query {
        plays(
            contestId: 6599996
            playerName: "Mueller"
            eventType: "TURNOVER"
            maxPeriodSecondsRemaining: 900
        ) {
            playId
            eventType
            periodNumber
            periodSecondsRemaining
            causedByPlayerName
            playText
        }
    }
    """
    r = client.post("/graphql", json={"query": query})
    assert r.status_code == 200
    body = r.json()
    assert "errors" not in body or body["errors"] is None
    plays = body["data"]["plays"]
    assert len(plays) == 1
    assert plays[0]["eventType"] == "TURNOVER"
    assert plays[0]["causedByPlayerName"] == "Cooper Mueller"
    assert plays[0]["periodSecondsRemaining"] <= 900


def test_graphql_general_play_selector_filter_object(client):
    query = """
    query {
        plays(
            filter: {
                team: "ND"
                eventType: "GOAL"
                maxGameSecondsRemaining: 1800
            }
        ) {
            playId
            eventType
            eventTeamShort
            gameSecondsRemainingReg
            shotResult
        }
    }
    """
    r = client.post("/graphql", json={"query": query})
    assert r.status_code == 200
    body = r.json()
    assert "errors" not in body or body["errors"] is None
    plays = body["data"]["plays"]
    assert len(plays) >= 1
    for p in plays:
        assert p["eventType"] == "GOAL"
        assert p["eventTeamShort"] == "ND"
        assert p["gameSecondsRemainingReg"] <= 1800


def _graphql_plays(client, query):
    r = client.post("/graphql", json={"query": query})
    assert r.status_code == 200
    return r.json()


def _plays_of(body):
    """Plays list, or None when the query failed before producing data."""
    data = body.get("data")
    if data is None:
        return None
    return data.get("plays")


def test_graphql_plays_rejects_unbounded_limit(client):
    # A huge limit previously bypassed validation and returned every row.
    body = _graphql_plays(client, "{ plays(limit: 100000000) { playId } }")
    assert _plays_of(body) is None
    errors = body.get("errors") or []
    assert errors, "expected a validation error"
    assert "limit" in errors[0]["message"]
    assert "BinderException" not in errors[0]["message"]
    assert "duckdb" not in errors[0]["message"].lower()


def test_graphql_plays_rejects_negative_limit_and_offset(client):
    body = _graphql_plays(client, "{ plays(limit: -1) { playId } }")
    errors = body.get("errors") or []
    assert errors and "limit" in errors[0]["message"]
    assert "BinderException" not in errors[0]["message"]

    body = _graphql_plays(client, "{ plays(offset: -5) { playId } }")
    errors = body.get("errors") or []
    assert errors and "offset" in errors[0]["message"]


def test_graphql_plays_filter_rejects_null_limit(client):
    # An explicit null used to reach SQL as `LIMIT NULL`, disabling pagination.
    body = _graphql_plays(
        client,
        '{ plays(filter: {contestId: 6599996, limit: null}) { playId } }',
    )
    assert _plays_of(body) is None
    assert body.get("errors"), "expected a rejection of an explicit null limit"


def test_graphql_plays_filter_rejects_out_of_range_values(client):
    body = _graphql_plays(client, '{ plays(filter: {minPeriod: 99}) { playId } }')
    errors = body.get("errors") or []
    assert errors and "min_period" in errors[0]["message"]


def test_graphql_plays_valid_inputs_still_respected(client):
    body = _graphql_plays(
        client,
        '{ plays(contestId: 6599996, eventType: "GOAL", limit: 5) { playId } }',
    )
    assert not body.get("errors")
    plays = body["data"]["plays"]
    assert len(plays) == 5

    body = _graphql_plays(client, "{ plays(limit: 500) { playId } }")
    assert not body.get("errors")
    assert len(body["data"]["plays"]) <= 500


def test_possession_team_id_rest_and_graphql(client):
    # Test REST possession_team_id filtering
    r = client.get("/api/contests/6599996/plays?possession_team_id=43861")
    assert r.status_code == 200
    plays = r.json()
    assert len(plays) >= 1
    for p in plays:
        assert p["possession_team_id"] == 43861

    # Test GraphQL possessionTeamId filtering and field retrieval
    query = """
    query {
        plays(possessionTeamId: 43861, contestId: 6599996) {
            playId
            possessionTeamId
            eventType
        }
    }
    """
    r_gql = client.post("/graphql", json={"query": query})
    assert r_gql.status_code == 200
    gql_data = r_gql.json()
    assert "errors" not in gql_data or gql_data["errors"] is None
    gql_plays = gql_data["data"]["plays"]
    assert len(gql_plays) >= 1
    for p in gql_plays:
        assert p["possessionTeamId"] == 43861


def test_shot_possession_retained_rest_and_graphql(client):
    # Test REST shot_possession_retained filter
    r = client.get("/api/contests/6599996/plays?event_type=SHOT&shot_possession_retained=true")
    assert r.status_code == 200
    plays = r.json()
    assert len(plays) >= 1
    for p in plays:
        assert p["event_type"] == "SHOT"
        assert p["shot_possession_retained"] is True

    # Test GraphQL shotPossessionRetained filtering and field retrieval
    query = """
    query {
        plays(
            contestId: 6599996
            eventType: "SHOT"
            shotPossessionRetained: true
        ) {
            playId
            eventType
            shotResult
            shotPossessionRetained
        }
    }
    """
    r_gql = client.post("/graphql", json={"query": query})
    assert r_gql.status_code == 200
    gql_data = r_gql.json()
    assert "errors" not in gql_data or gql_data["errors"] is None
    gql_plays = gql_data["data"]["plays"]
    assert len(gql_plays) >= 1
    for p in gql_plays:
        assert p["eventType"] == "SHOT"
        assert p["shotPossessionRetained"] is True


def test_gold_shooting_efficiency_api_and_graphql(client):
    # Test REST contest summary has realized shooting efficiency and shooting percentage metrics
    r = client.get("/api/contests/6599996/summary")
    assert r.status_code == 200
    data = r.json()
    assert "team_stats" in data
    stats = data["team_stats"]
    assert len(stats) == 2
    for s in stats:
        assert "shooting_pct" in s
        assert "realized_shooting_efficiency" in s
        assert "normalized_shooting_efficiency" in s
        assert "realized_shot_possessions_used" in s
        assert "shots_retained" in s
        assert "realized_shots_lost" in s
        assert s["total_shots"] == s["shots_retained"] + s["realized_shots_lost"] + s["goals"]
        assert s["realized_shot_possessions_used"] == s["goals"] + s["realized_shots_lost"]
        if s["total_shots"] > 0:
            assert s["shooting_pct"] == round(s["goals"] / s["total_shots"], 4)

    # Test GraphQL teamStats query with shooting percentage and efficiency fields
    query = """
    query {
        contest(id: 6599996) {
            contestId
            teamStats {
                teamShort
                goals
                totalShots
                shotsRetained
                realizedShotsLost
                realizedShotPossessionsUsed
                shootingPct
                realizedShootingEfficiency
                normalizedShootingEfficiency
            }
        }
    }
    """
    r_gql = client.post("/graphql", json={"query": query})
    assert r_gql.status_code == 200
    gql_data = r_gql.json()
    assert "errors" not in gql_data or gql_data["errors"] is None
    team_stats = gql_data["data"]["contest"]["teamStats"]
    assert len(team_stats) == 2
    for ts in team_stats:
        assert ts["totalShots"] == ts["shotsRetained"] + ts["realizedShotsLost"] + ts["goals"]
        assert ts["realizedShotPossessionsUsed"] == ts["goals"] + ts["realizedShotsLost"]
        assert ts["shootingPct"] > 0
        assert ts["realizedShootingEfficiency"] > 0
        assert ts["normalizedShootingEfficiency"] > 0


def test_rest_pydantic_schemas():
    from src.api.rest import (
        Contest,
        ContestSummary,
        HealthCheckResponse,
        Play,
        TeamStats,
    )
    from src.api.rest.schema import (
        ContestResponse,
        ContestSummaryResponse,
        HealthResponse,
        PlayFilterQueryParams,
        PlayQueryParams,
        PlayResponse,
        TeamStatsResponse,
    )

    # Verify Aliases
    assert HealthCheckResponse is HealthResponse
    assert Contest is ContestResponse
    assert ContestSummary is ContestSummaryResponse
    assert TeamStats is TeamStatsResponse
    assert Play is PlayResponse

    # Direct validation of models
    health = HealthResponse(status="ok", engine="FastAPI + DuckDB")
    assert health.status == "ok"

    contest = ContestResponse(contest_id=1, title="Test Game", status="final")
    assert contest.contest_id == 1

    team_stat = TeamStatsResponse(
        contest_id=1,
        team_id=10,
        team_short="TEST",
        total_shots=10,
        goals=2,
        shots_retained=5,
        realized_shots_lost=3,
        realized_shot_possessions_used=5,
        shooting_pct=0.2,
        realized_shooting_efficiency=0.4,
        saves_faced=4,
        ground_balls=15,
        turnovers=5,
        clears_good=8,
        clears_failed=1,
        penalties=1,
        penalty_seconds=30,
    )
    assert team_stat.goals == 2

    summary = ContestSummaryResponse(
        contest_id=1,
        title="Test Game",
        status="final",
        team_stats=[team_stat],
    )
    assert len(summary.team_stats) == 1

    query_params = PlayQueryParams(event_type="GOAL", limit=50)
    assert query_params.event_type == "GOAL"
    assert query_params.limit == 50

    filter_params = PlayFilterQueryParams(contest_id=1, team="ND")
    assert filter_params.contest_id == 1
    assert filter_params.team == "ND"

    from src.api.rest.schema import (
        OverallShootingEfficiency,
        PlayerShootingEfficiency,
        ShootingEfficiencySummary,
        ShootingEfficiencySummaryResponse,
        ShotResultRetention,
        TeamShootingEfficiency,
    )
    assert ShootingEfficiencySummary is ShootingEfficiencySummaryResponse

    summary_model = ShootingEfficiencySummaryResponse(
        min_player_shots=10,
        overall=OverallShootingEfficiency(
            realized_shooting_efficiency=0.5,
            normalized_shooting_efficiency=0.48,
            shooting_pct=0.3,
            total_shots=100,
            goals=30,
            shots_retained=40,
            realized_shots_lost=30,
            normalized_shots_lost=30.0,
            realized_shot_possessions_used=60,
            normalized_shot_possessions_used=60.0,
            average_team_game_realized_efficiency=0.48,
            average_team_game_normalized_efficiency=0.47,
        ),
        retention_by_shot_result=[
            ShotResultRetention(
                shot_result="SAVE",
                total_shots=40,
                shots_retained=15,
                shots_lost=25,
                retention_rate=0.375,
            )
        ],
        top_players=[
            PlayerShootingEfficiency(
                player_name="Test Shooter",
                team_name="Notre Dame",
                team_id=43861,
                total_shots=15,
                goals=6,
                shots_retained=5,
                realized_shots_lost=4,
                normalized_shots_lost=4.0,
                realized_shot_possessions_used=10,
                normalized_shot_possessions_used=10.0,
                realized_shooting_efficiency=0.6,
                normalized_shooting_efficiency=0.55,
                shooting_pct=0.4,
            )
        ],
        top_teams=[
            TeamShootingEfficiency(
                team_id=43861,
                team_name="Notre Dame",
                team_short_names=["ND", "NOTRE DA"],
                games_played=2,
                total_shots=80,
                goals=25,
                shots_retained=35,
                realized_shots_lost=20,
                normalized_shots_lost=20.0,
                realized_shot_possessions_used=45,
                normalized_shot_possessions_used=45.0,
                realized_shooting_efficiency=0.5556,
                normalized_shooting_efficiency=0.52,
                shooting_pct=0.3125,
            )
        ],
    )
    assert summary_model.overall.realized_shooting_efficiency == 0.5
    assert len(summary_model.retention_by_shot_result) == 1
    assert len(summary_model.top_players) == 1
    assert len(summary_model.top_teams) == 1
    assert summary_model.top_teams[0].team_short_names == ["ND", "NOTRE DA"]


def test_shooting_efficiency_summary_endpoint(client):
    r = client.get("/api/shooting-efficiency")
    assert r.status_code == 200
    data = r.json()

    # 1. Overall Average Shooting Efficiency
    assert "overall" in data
    overall = data["overall"]
    assert overall["total_shots"] > 0
    assert overall["goals"] > 0
    assert overall["realized_shot_possessions_used"] > 0
    assert overall["total_shots"] == overall["shots_retained"] + overall["realized_shots_lost"] + overall["goals"]
    assert overall["realized_shot_possessions_used"] == overall["goals"] + overall["realized_shots_lost"]
    assert abs(overall["realized_shooting_efficiency"] - round(overall["goals"] / overall["realized_shot_possessions_used"], 4)) <= 0.0001
    assert abs(overall["shooting_pct"] - round(overall["goals"] / overall["total_shots"], 4)) <= 0.0001
    assert "shots_lost" not in overall
    assert "shot_possessions_used" not in overall
    assert overall["average_team_game_realized_efficiency"] is not None
    assert 0.0 < overall["average_team_game_realized_efficiency"] < 1.0

    # 2. Retention by Shot Result (SAVE, WIDE, HIGH, HIT POST, HIT CROSSBAR, BLOCKED)
    assert "retention_by_shot_result" in data
    retention_list = data["retention_by_shot_result"]
    expected_results = ["SAVE", "WIDE", "HIGH", "HIT POST", "HIT CROSSBAR", "BLOCKED"]
    actual_results = [item["shot_result"] for item in retention_list]
    assert actual_results == expected_results

    for ret in retention_list:
        assert ret["total_shots"] == ret["shots_retained"] + ret["shots_lost"]
        if ret["total_shots"] > 0:
            expected_rate = round(ret["shots_retained"] / ret["total_shots"], 4)
            assert abs(ret["retention_rate"] - expected_rate) <= 0.0001
        else:
            assert ret["retention_rate"] == 0.0

    # WIDE and HIGH shots should have high retention (>70% in lacrosse), SAVE should be lower
    wide_ret = next(item for item in retention_list if item["shot_result"] == "WIDE")
    save_ret = next(item for item in retention_list if item["shot_result"] == "SAVE")
    assert wide_ret["retention_rate"] > 0.7
    assert save_ret["retention_rate"] < 0.5

    # 3. Top 10 Most Efficient Shooting Players (meeting min_shots threshold)
    assert "top_players" in data
    players = data["top_players"]
    assert len(players) == 10
    assert data["min_player_shots"] == 10
    prev_eff = 2.0
    for p in players:
        assert p["total_shots"] >= data["min_player_shots"]
        assert p["player_name"] is not None
        assert p["player_name"] not in ("TEAM", "TM")
        assert "team_short" not in p  # Verified removed from overall player stats
        assert p["realized_shot_possessions_used"] == p["goals"] + p["realized_shots_lost"]
        assert p["total_shots"] == p["shots_retained"] + p["realized_shots_lost"] + p["goals"]
        expected_p_eff = round(p["goals"] / p["realized_shot_possessions_used"], 4)
        assert abs(p["realized_shooting_efficiency"] - expected_p_eff) <= 0.0001
        assert "shots_lost" not in p
        assert "shot_possessions_used" not in p
        assert p["realized_shooting_efficiency"] <= prev_eff
        prev_eff = p["realized_shooting_efficiency"]

    # 4. Top 10 Most Efficient Shooting Teams
    assert "top_teams" in data
    teams = data["top_teams"]
    assert len(teams) == 10
    prev_team_eff = 2.0
    teams_by_name = {t["team_name"]: t for t in teams}
    # Verify multi-code teams contain their full set of short abbreviations
    if "North Carolina" in teams_by_name:
        assert "NORTH CA" in teams_by_name["North Carolina"]["team_short_names"]
        assert "UNC" in teams_by_name["North Carolina"]["team_short_names"]
    if "Notre Dame" in teams_by_name:
        assert "ND" in teams_by_name["Notre Dame"]["team_short_names"]
        assert "NOTRE DA" in teams_by_name["Notre Dame"]["team_short_names"]

    for t in teams:
        assert t["team_id"] > 0
        assert t["team_name"] is not None
        assert t["games_played"] >= 1
        assert "team_short" not in t  # Verified single ambiguous team_short removed
        assert "team_short_names" in t
        assert isinstance(t["team_short_names"], list)
        assert len(t["team_short_names"]) >= 1
        assert t["realized_shot_possessions_used"] == t["goals"] + t["realized_shots_lost"]
        expected_t_eff = round(t["goals"] / t["realized_shot_possessions_used"], 4)
        assert abs(t["realized_shooting_efficiency"] - expected_t_eff) <= 0.0001
        assert "shots_lost" not in t
        assert "shot_possessions_used" not in t
        assert t["realized_shooting_efficiency"] <= prev_team_eff
        prev_team_eff = t["realized_shooting_efficiency"]


def test_shooting_efficiency_summary_custom_params(client):
    # Test with custom min_shots and limit parameters
    r = client.get("/api/shooting-efficiency?min_shots=5&top_players_limit=5&top_teams_limit=5")
    assert r.status_code == 200
    data = r.json()
    assert data["min_player_shots"] == 5
    assert len(data["top_players"]) == 5
    for p in data["top_players"]:
        assert p["total_shots"] >= 5
    assert len(data["top_teams"]) == 5

    # Test filtering by contest_id
    r_cid = client.get("/api/shooting-efficiency?contest_id=6599996")
    assert r_cid.status_code == 200
    cid_data = r_cid.json()
    assert cid_data["contest_id"] == 6599996
    assert cid_data["overall"]["goals"] == 25
    # Two teams in contest 6599996
    assert len(cid_data["top_teams"]) == 2

    # Test 404 on invalid contest_id
    r_404 = client.get("/api/shooting-efficiency?contest_id=9999999")
    assert r_404.status_code == 404


def test_shooting_efficiency_summary_aliases(client):
    r_summary = client.get("/api/shooting-efficiency/summary")
    assert r_summary.status_code == 200
    r_analytics = client.get("/api/analytics/shooting-efficiency")
    assert r_analytics.status_code == 200
    assert r_summary.json()["overall"] == r_analytics.json()["overall"]


def test_openapi_schema_generation(client):
    r = client.get("/openapi.json")
    assert r.status_code == 200
    schema = r.json()
    schemas = schema["components"]["schemas"]
    assert "HealthResponse" in schemas
    assert "ContestResponse" in schemas
    assert "ContestSummaryResponse" in schemas
    assert "PlayResponse" in schemas
    assert "ShootingEfficiencySummaryResponse" in schemas
    assert "OverallShootingEfficiency" in schemas
    assert "ShotResultRetention" in schemas
    assert "PlayerShootingEfficiency" in schemas
    assert "TeamShootingEfficiency" in schemas


def test_realized_and_normalized_shooting_efficiency(client):
    r = client.get("/api/shooting-efficiency")
    assert r.status_code == 200
    data = r.json()

    # Overall realized vs normalized
    overall = data["overall"]
    assert "realized_shooting_efficiency" in overall
    assert "normalized_shooting_efficiency" in overall
    assert "realized_shots_lost" in overall
    assert "normalized_shots_lost" in overall
    assert "realized_shot_possessions_used" in overall
    assert "normalized_shot_possessions_used" in overall
    assert overall["realized_shot_possessions_used"] == overall["goals"] + overall["realized_shots_lost"]
    assert abs(overall["realized_shooting_efficiency"] - round(overall["goals"] / overall["realized_shot_possessions_used"], 4)) <= 0.0001
    assert abs(overall["normalized_shooting_efficiency"] - round(overall["goals"] / overall["normalized_shot_possessions_used"], 4)) <= 0.0001
    assert overall["average_team_game_realized_efficiency"] is not None
    assert overall["average_team_game_normalized_efficiency"] is not None

    # Players realized vs normalized
    for p in data["top_players"]:
        assert "realized_shooting_efficiency" in p
        assert "normalized_shooting_efficiency" in p
        assert "realized_shots_lost" in p
        assert "normalized_shots_lost" in p
        assert p["realized_shot_possessions_used"] == p["goals"] + p["realized_shots_lost"]
        assert abs(p["realized_shooting_efficiency"] - round(p["goals"] / p["realized_shot_possessions_used"], 4)) <= 0.0001
        if p["normalized_shot_possessions_used"] > 0:
            assert abs(p["normalized_shooting_efficiency"] - round(p["goals"] / p["normalized_shot_possessions_used"], 4)) <= 0.0001

    # Teams realized vs normalized
    for t in data["top_teams"]:
        assert "realized_shooting_efficiency" in t
        assert "normalized_shooting_efficiency" in t
        assert "realized_shots_lost" in t
        assert "normalized_shots_lost" in t
        assert t["realized_shot_possessions_used"] == t["goals"] + t["realized_shots_lost"]
        assert abs(t["realized_shooting_efficiency"] - round(t["goals"] / t["realized_shot_possessions_used"], 4)) <= 0.0001
        if t["normalized_shot_possessions_used"] > 0:
            assert abs(t["normalized_shooting_efficiency"] - round(t["goals"] / t["normalized_shot_possessions_used"], 4)) <= 0.0001


def test_shooting_efficiency_order_by_normalized(client):
    r_norm = client.get("/api/shooting-efficiency?order_by=normalized")
    assert r_norm.status_code == 200
    data = r_norm.json()
    assert data["order_by"] == "normalized"

    # Verify players are ordered monotonically descending by normalized_shooting_efficiency
    players = data["top_players"]
    prev_norm_eff = 2.0
    for p in players:
        assert p["normalized_shooting_efficiency"] <= prev_norm_eff
        prev_norm_eff = p["normalized_shooting_efficiency"]

    # Verify teams are ordered monotonically descending by normalized_shooting_efficiency
    teams = data["top_teams"]
    prev_team_norm_eff = 2.0
    for t in teams:
        assert t["normalized_shooting_efficiency"] <= prev_team_norm_eff
        prev_team_norm_eff = t["normalized_shooting_efficiency"]

    # Verify contest summary has realized and normalized fields
    r_cs = client.get("/api/contests/6599996/summary")
    assert r_cs.status_code == 200
    cs_data = r_cs.json()
    for ts in cs_data["team_stats"]:
        assert "realized_shooting_efficiency" in ts
        assert "normalized_shooting_efficiency" in ts
        assert "realized_shots_lost" in ts
        assert "normalized_shots_lost" in ts
        assert "realized_shot_possessions_used" in ts
        assert "normalized_shot_possessions_used" in ts
        assert "shots_lost" not in ts
        assert "shot_possessions_used" not in ts
        assert ts["realized_shooting_efficiency"] > 0
        assert "shooting_efficiency" not in ts






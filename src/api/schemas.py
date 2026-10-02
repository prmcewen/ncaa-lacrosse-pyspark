from typing import Any, List, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator


class BaseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# --- Response Schemas ---

class HealthResponse(BaseSchema):
    status: str
    engine: str


class TeamStatsResponse(BaseSchema):
    contest_id: int
    team_id: int
    team_short: str
    total_shots: int
    goals: int
    shots_retained: int
    realized_shots_lost: int
    normalized_shots_lost: Optional[float] = None
    realized_shot_possessions_used: int
    normalized_shot_possessions_used: Optional[float] = None
    shooting_pct: float
    realized_shooting_efficiency: float
    normalized_shooting_efficiency: Optional[float] = None
    saves_faced: int
    ground_balls: int
    turnovers: int
    clears_good: int
    clears_failed: int
    penalties: int
    penalty_seconds: Optional[int] = None


class ContestResponse(BaseSchema):
    contest_id: int
    title: str
    status: str


class ContestSummaryResponse(BaseSchema):
    contest_id: int
    title: str
    status: str
    team_stats: List[TeamStatsResponse] = Field(default_factory=list)


class PlayResponse(BaseSchema):
    play_id: str
    contest_id: int
    play_seq: int
    ingest_timestamp: Optional[str] = None
    period_number: int
    period_display: str
    clock_display: str
    period_seconds_remaining: int
    game_seconds_elapsed: int
    game_seconds_remaining_reg: int
    event_type: str
    play_text: str
    team_id: Optional[int] = None
    event_team_id: Optional[int] = None
    event_team_short: Optional[str] = None
    event_team_is_home: Optional[bool] = None
    opponent_team_id: Optional[int] = None
    possession_team_id: Optional[int] = None
    primary_player_name: Optional[str] = None
    secondary_player_name: Optional[str] = None
    caused_by_player_name: Optional[str] = None
    caused_by_team_id: Optional[int] = None
    shot_result: Optional[str] = None
    shot_possession_retained: Optional[bool] = None
    faceoff_winner_player: Optional[str] = None
    faceoff_loser_player: Optional[str] = None
    is_faceoff_violation: Optional[bool] = None
    penalty_type: Optional[str] = None
    penalty_duration_seconds: Optional[int] = None
    is_extra_man_opportunity: Optional[bool] = None
    clear_result: Optional[str] = None
    running_home_score: int
    running_visitor_score: int
    home_score_margin: int
    event_team_margin: Optional[int] = None

    @model_validator(mode="before")
    @classmethod
    def sync_team_id(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "team_id" in data and "event_team_id" not in data:
                data["event_team_id"] = data["team_id"]
            elif "event_team_id" in data and "team_id" not in data:
                data["team_id"] = data["event_team_id"]
        return data


class OverallShootingEfficiency(BaseSchema):
    realized_shooting_efficiency: float = Field(..., description="Realized shooting efficiency based on actual play outcomes (goals / realized possessions used)")
    normalized_shooting_efficiency: float = Field(..., description="Normalized shooting efficiency based on average retention rates by shot result")
    shooting_pct: float = Field(..., description="Overall shooting percentage (goals / total shots)")
    total_shots: int = Field(..., description="Total shots taken (including goals)")
    goals: int = Field(..., description="Total goals scored")
    shots_retained: int = Field(..., description="Non-goal shots where offense retained possession")
    realized_shots_lost: int = Field(..., description="Actual non-goal shots lost to defense")
    normalized_shots_lost: float = Field(..., description="Expected non-goal shots lost based on shot result baselines")
    realized_shot_possessions_used: int = Field(..., description="Actual possessions ended by shots (goals + realized shots lost)")
    normalized_shot_possessions_used: float = Field(..., description="Expected possessions ended by shots (goals + normalized shots lost)")
    average_team_game_realized_efficiency: Optional[float] = Field(None, description="Average realized shooting efficiency across team games")
    average_team_game_normalized_efficiency: Optional[float] = Field(None, description="Average normalized shooting efficiency across team games")


class ShotResultRetention(BaseSchema):
    shot_result: str = Field(..., description="Shot result type (SAVE, WIDE, HIGH, HIT POST, HIT CROSSBAR, BLOCKED)")
    total_shots: int = Field(..., description="Total non-goal shots with this result")
    shots_retained: int = Field(..., description="Shots where offensive possession was retained")
    shots_lost: int = Field(..., description="Shots where offensive possession was lost")
    retention_rate: float = Field(..., description="Proportion of shots where offensive possession was retained")


class PlayerShootingEfficiency(BaseSchema):
    player_name: str = Field(..., description="Canonical player name")
    team_name: Optional[str] = Field(None, description="Team school name")
    team_id: Optional[int] = Field(None, description="Team ID")
    total_shots: int = Field(..., description="Total shots taken by player")
    goals: int = Field(..., description="Total goals scored by player")
    shots_retained: int = Field(..., description="Non-goal shots retained by player's team")
    realized_shots_lost: int = Field(..., description="Actual non-goal shots lost")
    normalized_shots_lost: float = Field(..., description="Expected non-goal shots lost based on shot result baselines")
    realized_shot_possessions_used: int = Field(..., description="Actual possessions ended by player's shots")
    normalized_shot_possessions_used: float = Field(..., description="Expected possessions ended based on shot result baselines")
    realized_shooting_efficiency: float = Field(..., description="Player realized shooting efficiency (goals / realized possessions used)")
    normalized_shooting_efficiency: float = Field(..., description="Player normalized shooting efficiency based on average retention rates by shot result")
    shooting_pct: float = Field(..., description="Player shooting percentage (goals / total shots)")


class TeamShootingEfficiency(BaseSchema):
    team_id: int = Field(..., description="Team ID")
    team_name: str = Field(..., description="Team school name")
    team_name_full: Optional[str] = Field(None, description="Full team name")
    team_short_names: List[str] = Field(default_factory=list, description="List of all short team abbreviations used across games")
    games_played: int = Field(..., description="Total contests played")
    total_shots: int = Field(..., description="Total shots taken by team")
    goals: int = Field(..., description="Total goals scored by team")
    shots_retained: int = Field(..., description="Non-goal shots retained by team")
    realized_shots_lost: int = Field(..., description="Actual non-goal shots lost")
    normalized_shots_lost: float = Field(..., description="Expected non-goal shots lost based on shot result baselines")
    realized_shot_possessions_used: int = Field(..., description="Actual possessions ended by shots")
    normalized_shot_possessions_used: float = Field(..., description="Expected possessions ended based on shot result baselines")
    realized_shooting_efficiency: float = Field(..., description="Team realized shooting efficiency")
    normalized_shooting_efficiency: float = Field(..., description="Team normalized shooting efficiency based on average retention rates by shot result")
    shooting_pct: float = Field(..., description="Team shooting percentage (goals / total shots)")


class ShootingEfficiencySummaryResponse(BaseSchema):
    contest_id: Optional[int] = Field(None, description="Contest ID if filtered for a specific contest, null if overall")
    min_player_shots: int = Field(..., description="Minimum shots threshold applied for player leaderboard")
    order_by: str = Field("realized", description="Ordering metric applied ('realized' or 'normalized')")
    overall: OverallShootingEfficiency = Field(..., description="Overall aggregate shooting efficiency")
    retention_by_shot_result: List[ShotResultRetention] = Field(..., description="Retention by offensive team grouped by shot result")
    top_players: List[PlayerShootingEfficiency] = Field(..., description="Top most efficient shooting players meeting minimum shot threshold")
    top_teams: List[TeamShootingEfficiency] = Field(..., description="Top most efficient shooting teams")


# Aliases for convenience / naming parity
HealthCheckResponse = HealthResponse
TeamStats = TeamStatsResponse
Contest = ContestResponse
ContestSummary = ContestSummaryResponse
Play = PlayResponse
ShootingEfficiencySummary = ShootingEfficiencySummaryResponse



# --- Query Parameter & Filter Schemas ---

class PlayQueryParams(BaseSchema):
    event_type: Optional[str] = Field(None, description="Event type filter (e.g. TURNOVER, GOAL, SHOT)")
    period_number: Optional[int] = Field(None, ge=1, le=10, description="Period filter")
    min_period: Optional[int] = Field(None, ge=1, le=10, description="Minimum period number")
    max_period: Optional[int] = Field(None, ge=1, le=10, description="Maximum period number")
    player_name: Optional[str] = Field(None, description="Search player name across all roles")
    team: Optional[str] = Field(None, description="Team short code or name")
    team_id: Optional[int] = Field(None, description="Team ID")
    possession_team_id: Optional[int] = Field(None, description="Possession team ID")
    max_period_seconds_remaining: Optional[int] = Field(None, ge=0, description="Max seconds remaining in quarter")
    min_period_seconds_remaining: Optional[int] = Field(None, ge=0, description="Min seconds remaining in quarter")
    max_game_seconds_remaining: Optional[int] = Field(None, ge=0, description="Max seconds remaining in game")
    min_game_seconds_remaining: Optional[int] = Field(None, ge=0, description="Min seconds remaining in game")
    shot_result: Optional[str] = Field(None, description="Shot result (e.g. GOAL, SAVE, WIDE, BLOCKED)")
    shot_possession_retained: Optional[bool] = Field(None, description="Filter by whether shot possession was retained")
    is_extra_man_opportunity: Optional[bool] = Field(None, description="Filter for extra man opportunities (EMO)")
    event_team_margin_min: Optional[int] = Field(None, description="Min margin for event team")
    event_team_margin_max: Optional[int] = Field(None, description="Max margin for event team")
    order_by: str = Field("play_seq", description="Column to sort by")
    order_desc: bool = Field(False, description="Sort descending if true")
    limit: int = Field(100, ge=1, le=500, description="Max rows to return")
    offset: int = Field(0, ge=0, description="Row offset for pagination")


class PlayFilterSchema(PlayQueryParams):
    contest_id: Optional[int] = Field(None, description="Optional contest ID filter")
    event_types: Optional[List[str]] = Field(None, description="List of event types to filter by")
    home_score_margin_min: Optional[int] = Field(None, description="Min margin for home team")
    home_score_margin_max: Optional[int] = Field(None, description="Max margin for home team")
    # order_by/order_desc/limit/offset are inherited from PlayQueryParams on
    # purpose: re-declaring them as Optional here would let an explicit null
    # reach SQL as `LIMIT NULL` and disable pagination.


PlayFilterQueryParams = PlayFilterSchema

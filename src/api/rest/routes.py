from typing import Annotated, List, Optional

from fastapi import APIRouter, HTTPException, Query

from src.api.dependencies import RequestDB
from src.api.rest.schema import (
    ContestResponse,
    ContestSummaryResponse,
    HealthResponse,
    PlayFilterQueryParams,
    PlayQueryParams,
    PlayResponse,
    ShootingEfficiencySummaryResponse,
)

router = APIRouter(prefix="/api", tags=["REST Endpoints"])



@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok", engine="FastAPI + DuckDB")


@router.get("/contests", response_model=List[ContestResponse])
def list_contests(db: RequestDB) -> List[ContestResponse]:
    return [ContestResponse.model_validate(c) for c in db.get_contests()]


@router.get("/contests/{contest_id}/summary", response_model=ContestSummaryResponse)
def get_contest_summary(contest_id: int, db: RequestDB) -> ContestSummaryResponse:
    summary = db.get_contest_summary(contest_id)
    if not summary:
        raise HTTPException(status_code=404, detail=f"Contest {contest_id} not found")
    return ContestSummaryResponse.model_validate(summary)


@router.get("/contests/{contest_id}/plays", response_model=List[PlayResponse])
def get_contest_plays(
    contest_id: int,
    db: RequestDB,
    params: Annotated[PlayQueryParams, Query()] = PlayQueryParams(),
) -> List[PlayResponse]:
    plays = db.get_plays(contest_id=contest_id, **params.model_dump())
    return [PlayResponse.model_validate(p) for p in plays]


@router.get("/plays", response_model=List[PlayResponse])
def select_plays(
    db: RequestDB,
    params: Annotated[PlayFilterQueryParams, Query()] = PlayFilterQueryParams(),
) -> List[PlayResponse]:
    plays = db.get_plays(**params.model_dump())
    return [PlayResponse.model_validate(p) for p in plays]


@router.get("/shooting-efficiency", response_model=ShootingEfficiencySummaryResponse)
@router.get("/shooting-efficiency/summary", response_model=ShootingEfficiencySummaryResponse)
@router.get("/analytics/shooting-efficiency", response_model=ShootingEfficiencySummaryResponse)
def get_shooting_efficiency_summary(
    db: RequestDB,
    min_shots: int = Query(10, ge=1, description="Minimum number of shots taken by a player to qualify"),
    top_players_limit: int = Query(10, ge=1, le=100, description="Maximum number of top players to return"),
    top_teams_limit: int = Query(10, ge=1, le=100, description="Maximum number of top teams to return"),
    contest_id: Optional[int] = Query(None, description="Optional contest ID to filter summary"),
    order_by: str = Query("realized", pattern="^(realized|normalized)$", description="Ordering metric applied for top players and teams ('realized' or 'normalized')"),
) -> ShootingEfficiencySummaryResponse:
    if contest_id is not None:
        contests = db.execute_query("SELECT 1 FROM dim_contests WHERE contest_id = ?", [contest_id])
        if not contests:
            raise HTTPException(status_code=404, detail=f"Contest {contest_id} not found")
    summary = db.get_shooting_efficiency_summary(
        min_player_shots=min_shots,
        top_players_limit=top_players_limit,
        top_teams_limit=top_teams_limit,
        contest_id=contest_id,
        order_by=order_by,
    )
    return ShootingEfficiencySummaryResponse.model_validate(summary)


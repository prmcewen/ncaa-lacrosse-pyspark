"""REST API Pydantic schemas.

Re-exports shared schemas from `src.api.schemas` for backward compatibility.
"""
from src.api.schemas import (
    BaseSchema,
    Contest,
    ContestResponse,
    ContestSummary,
    ContestSummaryResponse,
    HealthCheckResponse,
    HealthResponse,
    OverallShootingEfficiency,
    Play,
    PlayerShootingEfficiency,
    PlayFilterQueryParams,
    PlayFilterSchema,
    PlayQueryParams,
    PlayResponse,
    ShootingEfficiencySummary,
    ShootingEfficiencySummaryResponse,
    ShotResultRetention,
    TeamShootingEfficiency,
    TeamStats,
    TeamStatsResponse,
)

__all__ = [
    "BaseSchema",
    "HealthResponse",
    "HealthCheckResponse",
    "TeamStatsResponse",
    "TeamStats",
    "ContestResponse",
    "Contest",
    "ContestSummaryResponse",
    "ContestSummary",
    "PlayResponse",
    "Play",
    "PlayQueryParams",
    "PlayFilterSchema",
    "PlayFilterQueryParams",
    "OverallShootingEfficiency",
    "ShotResultRetention",
    "PlayerShootingEfficiency",
    "TeamShootingEfficiency",
    "ShootingEfficiencySummaryResponse",
    "ShootingEfficiencySummary",
]


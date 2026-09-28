"""REST API Pydantic schemas.

Re-exports shared schemas from `src.api.schemas` for backward compatibility.
"""
from src.api.schemas import (
    BaseSchema,
    HealthResponse,
    HealthCheckResponse,
    TeamStatsResponse,
    TeamStats,
    ContestResponse,
    Contest,
    ContestSummaryResponse,
    ContestSummary,
    PlayResponse,
    Play,
    PlayQueryParams,
    PlayFilterSchema,
    PlayFilterQueryParams,
    OverallShootingEfficiency,
    ShotResultRetention,
    PlayerShootingEfficiency,
    TeamShootingEfficiency,
    ShootingEfficiencySummaryResponse,
    ShootingEfficiencySummary,
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


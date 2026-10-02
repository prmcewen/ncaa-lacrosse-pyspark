from typing import List, Optional
import strawberry
from pydantic import ValidationError
from strawberry.types import Info
from src.api.schemas import (
    TeamStatsResponse,
    ContestResponse,
    PlayResponse,
    PlayFilterSchema,
)


def _validation_message(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err["loc"]) or "input"
        parts.append(f"{loc}: {err['msg']}")
    return "; ".join(parts)


@strawberry.experimental.pydantic.type(model=TeamStatsResponse, all_fields=True)
class TeamStats:
    pass


@strawberry.experimental.pydantic.type(model=ContestResponse, all_fields=True)
class Contest:
    @strawberry.field
    def team_stats(self, info: Info) -> List[TeamStats]:
        stats = info.context["db"].execute_query(
            "SELECT * FROM agg_team_game_stats WHERE contest_id = ?",
            [self.contest_id]
        )
        return [TeamStats.from_pydantic(TeamStatsResponse.model_validate(s)) for s in stats]


@strawberry.experimental.pydantic.type(model=PlayResponse, all_fields=True)
class Play:
    pass


@strawberry.experimental.pydantic.input(model=PlayFilterSchema, all_fields=True)
class PlayFilter:
    pass


@strawberry.type
class Query:
    @strawberry.field
    def contests(self, info: Info) -> List[Contest]:
        raw = info.context["db"].get_contests()
        return [Contest.from_pydantic(ContestResponse.model_validate(c)) for c in raw]

    @strawberry.field
    def contest(self, info: Info, id: int) -> Optional[Contest]:
        res = info.context["db"].execute_query("SELECT * FROM dim_contests WHERE contest_id = ?", [id])
        if not res:
            return None
        return Contest.from_pydantic(ContestResponse.model_validate(res[0]))

    @strawberry.field
    def plays(
        self,
        info: Info,
        filter: Optional[PlayFilter] = None,
        contest_id: Optional[int] = None,
        event_type: Optional[str] = None,
        event_types: Optional[List[str]] = None,
        period_number: Optional[int] = None,
        min_period: Optional[int] = None,
        max_period: Optional[int] = None,
        player_name: Optional[str] = None,
        team: Optional[str] = None,
        team_id: Optional[int] = None,
        possession_team_id: Optional[int] = None,
        max_period_seconds_remaining: Optional[int] = None,
        min_period_seconds_remaining: Optional[int] = None,
        max_game_seconds_remaining: Optional[int] = None,
        min_game_seconds_remaining: Optional[int] = None,
        shot_result: Optional[str] = None,
        shot_possession_retained: Optional[bool] = None,
        is_extra_man_opportunity: Optional[bool] = None,
        event_team_margin_min: Optional[int] = None,
        event_team_margin_max: Optional[int] = None,
        home_score_margin_min: Optional[int] = None,
        home_score_margin_max: Optional[int] = None,
        order_by: Optional[str] = None,
        order_desc: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Play]:
        direct_args = {
            "contest_id": contest_id,
            "event_type": event_type,
            "event_types": event_types,
            "period_number": period_number,
            "min_period": min_period,
            "max_period": max_period,
            "player_name": player_name,
            "team": team,
            "team_id": team_id,
            "possession_team_id": possession_team_id,
            "max_period_seconds_remaining": max_period_seconds_remaining,
            "min_period_seconds_remaining": min_period_seconds_remaining,
            "max_game_seconds_remaining": max_game_seconds_remaining,
            "min_game_seconds_remaining": min_game_seconds_remaining,
            "shot_result": shot_result,
            "shot_possession_retained": shot_possession_retained,
            "is_extra_man_opportunity": is_extra_man_opportunity,
            "event_team_margin_min": event_team_margin_min,
            "event_team_margin_max": event_team_margin_max,
            "home_score_margin_min": home_score_margin_min,
            "home_score_margin_max": home_score_margin_max,
            "order_by": order_by,
            "order_desc": order_desc,
            "limit": limit,
            "offset": offset,
        }
        overrides = {k: v for k, v in direct_args.items() if v is not None}
        # model_copy(update=...) skips validation in Pydantic v2, so direct
        # arguments must be re-validated after merging or they bypass the
        # ge/le constraints that REST enforces on the same parameters.
        try:
            base_params = filter.to_pydantic() if filter else PlayFilterSchema()
            params = PlayFilterSchema.model_validate(
                {**base_params.model_dump(), **overrides}
            )
        except ValidationError as exc:
            raise ValueError(
                f"Invalid plays arguments: {_validation_message(exc)}"
            ) from exc

        raw_plays = info.context["db"].get_plays(**params.model_dump())
        return [Play.from_pydantic(PlayResponse.model_validate(p)) for p in raw_plays]



schema = strawberry.Schema(query=Query)

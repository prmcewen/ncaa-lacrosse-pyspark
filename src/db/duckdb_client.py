from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb
from src.db.gold_storage import resolve_gold_dir

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
SILVER_PARQUET = DATA_DIR / "silver" / "silver_plays.parquet"
GOLD_DIR = DATA_DIR / "gold"


def _scan_expression(path: Path, glob_pattern: Optional[str] = None) -> str:
    escaped = str(path).replace("'", "''")
    if (path / "_delta_log").exists():
        return f"delta_scan('{escaped}')"
    if glob_pattern:
        escaped_glob = str(glob_pattern).replace("'", "''")
        return f"read_parquet('{escaped_glob}')"
    return f"read_parquet('{escaped}')"


class DuckDBClient:
    def __init__(self, database: str = ":memory:", *, include_silver: bool = True):
        self.include_silver = include_silver
        self.conn = duckdb.connect(database)
        try:
            self._init_views()
        except Exception:
            self.conn.close()
            raise

    def close(self) -> None:
        self.conn.close()

    def _init_views(self) -> None:
        """Register views over Delta Lake or Parquet directories for clean, performant SQL querying."""
        try:
            self.conn.execute("LOAD delta;")
        except Exception:
            try:
                self.conn.execute("INSTALL delta; LOAD delta;")
            except Exception:
                pass

        # Pin once for all queries made by this client/request. Old generations
        # stay available while a new request can resolve a newer publication.
        self.gold_dir = resolve_gold_dir(GOLD_DIR)
        silver_glob = str(SILVER_PARQUET / "**" / "*.parquet")
        fact_plays_path = str(self.gold_dir / "fact_plays.parquet")
        dim_teams_path = str(self.gold_dir / "dim_teams.parquet")
        dim_contests_path = str(self.gold_dir / "dim_contests.parquet")
        agg_stats_path = str(self.gold_dir / "agg_team_game_stats.parquet")

        if self.include_silver and SILVER_PARQUET.exists():
            scan_expr = _scan_expression(SILVER_PARQUET, silver_glob)
            self.conn.execute(f"CREATE OR REPLACE VIEW silver_plays AS SELECT * FROM {scan_expr}")

        if Path(fact_plays_path).exists():
            scan_expr = _scan_expression(Path(fact_plays_path))
            self.conn.execute(f"CREATE OR REPLACE VIEW fact_plays AS SELECT * FROM {scan_expr}")
        elif self.include_silver and SILVER_PARQUET.exists():
            scan_expr = _scan_expression(SILVER_PARQUET, silver_glob)
            self.conn.execute(f"CREATE OR REPLACE VIEW fact_plays AS SELECT * FROM {scan_expr}")

        if Path(dim_teams_path).exists():
            scan_expr = _scan_expression(Path(dim_teams_path))
            self.conn.execute(f"CREATE OR REPLACE VIEW dim_teams AS SELECT * FROM {scan_expr}")

        if Path(dim_contests_path).exists():
            scan_expr = _scan_expression(Path(dim_contests_path))
            self.conn.execute(f"CREATE OR REPLACE VIEW dim_contests AS SELECT * FROM {scan_expr}")

        if Path(agg_stats_path).exists():
            scan_expr = _scan_expression(Path(agg_stats_path))
            self.conn.execute(f"CREATE OR REPLACE VIEW agg_team_game_stats AS SELECT * FROM {scan_expr} WHERE team_short IS NOT NULL")

    def execute_query(self, query: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute parameterized SQL query and return rows as dictionaries."""
        # Share the catalog, but isolate result state between concurrent requests.
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params or [])
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def get_contests(self) -> List[Dict[str, Any]]:
        return self.execute_query("SELECT * FROM dim_contests ORDER BY contest_id")

    def get_contest_summary(self, contest_id: int) -> Dict[str, Any]:
        contests = self.execute_query("SELECT * FROM dim_contests WHERE contest_id = ?", [contest_id])
        if not contests:
            return {}
        contest = contests[0]
        stats = self.execute_query("SELECT * FROM agg_team_game_stats WHERE contest_id = ?", [contest_id])
        contest["team_stats"] = stats
        return contest

    def get_plays(
        self,
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
        order_by: str = "play_seq",
        order_desc: bool = False,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        conditions = ["1=1"]
        params: List[Any] = []

        if contest_id is not None:
            conditions.append("contest_id = ?")
            params.append(contest_id)

        if event_type:
            conditions.append("event_type = ?")
            params.append(event_type.upper())

        if event_types:
            placeholders = ", ".join(["?"] * len(event_types))
            conditions.append(f"event_type IN ({placeholders})")
            params.extend([et.upper() for et in event_types])

        if period_number is not None:
            conditions.append("period_number = ?")
            params.append(period_number)

        if min_period is not None:
            conditions.append("period_number >= ?")
            params.append(min_period)

        if max_period is not None:
            conditions.append("period_number <= ?")
            params.append(max_period)

        if player_name:
            pattern = f"%{player_name}%"
            conditions.append(
                "(primary_player_name ILIKE ? OR secondary_player_name ILIKE ? OR "
                "caused_by_player_name ILIKE ? OR faceoff_winner_player ILIKE ? OR "
                "faceoff_loser_player ILIKE ?)"
            )
            params.extend([pattern] * 5)

        if team:
            team_clean = team.strip()
            team_pattern = f"%{team_clean}%"
            if len(team_clean) <= 4:
                conditions.append("""
                    (UPPER(event_team_short) = UPPER(?)
                     OR CAST(team_id AS VARCHAR) = ? 
                     OR CAST(caused_by_team_id AS VARCHAR) = ? 
                     OR team_id IN (SELECT team_id FROM dim_teams WHERE UPPER(name_short) = UPPER(?) OR UPPER(name_6char) = UPPER(?))
                     OR caused_by_team_id IN (SELECT team_id FROM dim_teams WHERE UPPER(name_short) = UPPER(?) OR UPPER(name_6char) = UPPER(?)))
                """)
                params.extend([team_clean, team_clean, team_clean, team_clean, team_clean, team_clean, team_clean])
            else:
                conditions.append("""
                    (event_team_short ILIKE ? 
                     OR CAST(team_id AS VARCHAR) = ? 
                     OR CAST(caused_by_team_id AS VARCHAR) = ? 
                     OR team_id IN (SELECT team_id FROM dim_teams WHERE name_short ILIKE ? OR name_full ILIKE ? OR name_6char ILIKE ?)
                     OR caused_by_team_id IN (SELECT team_id FROM dim_teams WHERE name_short ILIKE ? OR name_full ILIKE ? OR name_6char ILIKE ?))
                """)
                params.extend([team_pattern, team_clean, team_clean, team_pattern, team_pattern, team_pattern, team_pattern, team_pattern, team_pattern])

        if team_id is not None:
            conditions.append("(team_id = ? OR caused_by_team_id = ?)")
            params.extend([team_id, team_id])

        if possession_team_id is not None:
            conditions.append("possession_team_id = ?")
            params.append(possession_team_id)

        if max_period_seconds_remaining is not None:
            conditions.append("period_seconds_remaining <= ?")
            params.append(max_period_seconds_remaining)

        if min_period_seconds_remaining is not None:
            conditions.append("period_seconds_remaining >= ?")
            params.append(min_period_seconds_remaining)

        if max_game_seconds_remaining is not None:
            conditions.append("game_seconds_remaining_reg <= ?")
            params.append(max_game_seconds_remaining)

        if min_game_seconds_remaining is not None:
            conditions.append("game_seconds_remaining_reg >= ?")
            params.append(min_game_seconds_remaining)

        if shot_result:
            conditions.append("shot_result = ?")
            params.append(shot_result.upper())

        if shot_possession_retained is not None:
            conditions.append("shot_possession_retained = ?")
            params.append(shot_possession_retained)

        if is_extra_man_opportunity is not None:
            conditions.append("is_extra_man_opportunity = ?")
            params.append(is_extra_man_opportunity)

        if event_team_margin_min is not None:
            conditions.append("event_team_margin >= ?")
            params.append(event_team_margin_min)

        if event_team_margin_max is not None:
            conditions.append("event_team_margin <= ?")
            params.append(event_team_margin_max)

        if home_score_margin_min is not None:
            conditions.append("home_score_margin >= ?")
            params.append(home_score_margin_min)

        if home_score_margin_max is not None:
            conditions.append("home_score_margin <= ?")
            params.append(home_score_margin_max)

        allowed_order_by = {
            "play_seq", "game_seconds_elapsed", "game_seconds_remaining_reg", 
            "period_seconds_remaining", "period_number", "play_id"
        }
        order_col = order_by if order_by in allowed_order_by else "play_seq"
        direction = "DESC" if order_desc else "ASC"
        # Sequence/time values repeat across contests. Unique tie-breakers keep
        # LIMIT/OFFSET page boundaries stable regardless of Parquet scan order.
        tie_breakers = [col for col in ("contest_id", "play_seq", "play_id") if col != order_col]
        order_clause = ", ".join([f"{order_col} {direction}"] + [f"{col} ASC" for col in tie_breakers])

        params.extend([limit, offset])
        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT * FROM fact_plays 
            WHERE {where_clause} 
            ORDER BY {order_clause} 
            LIMIT ? OFFSET ?
        """
        return self.execute_query(sql, params)

    def get_caused_turnovers_situational(
        self,
        contest_id: Optional[int] = None,
        defending_team_down_by: Optional[int] = None,
        max_period_seconds: Optional[int] = None,
        min_period: Optional[int] = 4,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Complex analytical situational query:
        E.g., caused turnovers by a player on a team down by 1 in the last minute.
        Note: event_team_margin is from the offensive team's perspective.
        Therefore, defending_team_margin = -event_team_margin.
        If defending team is down by 1, defending_team_margin == -1, meaning event_team_margin == 1.
        """
        conditions = [
            "event_type = 'TURNOVER'",
            "caused_by_player_name IS NOT NULL"
        ]
        params: List[Any] = []

        if contest_id:
            conditions.append("contest_id = ?")
            params.append(contest_id)

        if min_period:
            conditions.append("period_number >= ?")
            params.append(min_period)

        if max_period_seconds is not None:
            conditions.append("period_seconds_remaining <= ?")
            params.append(max_period_seconds)

        if defending_team_down_by is not None:
            # If defending team is down by X, the turnover committing team is up by X:
            conditions.append("event_team_margin = ?")
            params.append(defending_team_down_by)

        params.append(limit)
        where_clause = " AND ".join(conditions)

        sql = f"""
            SELECT 
                play_id,
                contest_id,
                play_seq,
                period_number,
                clock_display,
                period_seconds_remaining,
                primary_player_name AS turnover_committer,
                event_team_short AS turnover_team,
                caused_by_player_name AS caused_by_player,
                caused_by_team_id,
                running_home_score,
                running_visitor_score,
                event_team_margin AS committer_team_margin,
                (-event_team_margin) AS defending_team_margin,
                play_text
            FROM fact_plays
            WHERE {where_clause}
            ORDER BY play_seq
            LIMIT ?
        """
        return self.execute_query(sql, params)

    def get_shooting_efficiency_summary(
        self,
        min_player_shots: int = 10,
        top_players_limit: int = 10,
        top_teams_limit: int = 10,
        contest_id: Optional[int] = None,
        order_by: str = "realized",
    ) -> Dict[str, Any]:
        """
        Generate aggregate shooting efficiency summary with realized and normalized metrics.
        
        Calculates:
        1. Overall average shooting efficiency and shooting percentage (realized and normalized).
        2. Retention by the offense by shot result type:
           SAVE, WIDE, HIGH, HIT POST, HIT CROSSBAR, BLOCKED.
        3. Top N most efficient shooting players with >= min_player_shots.
        4. Top N most efficient shooting teams.
        """
        normalized_order = order_by.lower() == "normalized"
        order_col = "normalized_shooting_efficiency" if normalized_order else "realized_shooting_efficiency"

        where_fact = "WHERE p.contest_id = ?" if contest_id is not None else ""
        where_fact_no_p = "WHERE contest_id = ?" if contest_id is not None else ""
        params_fact = [contest_id] if contest_id is not None else []

        # 1. Overall Shooting Efficiency
        overall_sql = f"""
            WITH baseline_rates AS (
                SELECT 
                    shot_result,
                    1.0 - (COUNT(CASE WHEN shot_possession_retained = true THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0)) AS expected_loss_rate
                FROM fact_plays
                WHERE event_type = 'SHOT'
                GROUP BY shot_result
            ),
            overall_default AS (
                SELECT 
                    1.0 - (COUNT(CASE WHEN shot_possession_retained = true THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0)) AS default_loss_rate
                FROM fact_plays
                WHERE event_type = 'SHOT'
            )
            SELECT 
                COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) AS goals,
                COUNT(CASE WHEN p.event_type IN ('SHOT', 'GOAL') THEN 1 END) AS total_shots,
                COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = true THEN 1 END) AS shots_retained,
                COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = false THEN 1 END) AS realized_shots_lost,
                ROUND(SUM(CASE 
                    WHEN p.event_type = 'SHOT' THEN COALESCE(b.expected_loss_rate, od.default_loss_rate)
                    ELSE 0.0 
                END), 4) AS normalized_shots_lost,
                COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = false THEN 1 END) AS realized_shot_possessions_used,
                ROUND(COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + SUM(CASE 
                    WHEN p.event_type = 'SHOT' THEN COALESCE(b.expected_loss_rate, od.default_loss_rate)
                    ELSE 0.0 
                END), 4) AS normalized_shot_possessions_used,
                ROUND(
                    COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) * 1.0 / 
                    NULLIF(COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = false THEN 1 END), 0),
                    4
                ) AS realized_shooting_efficiency,
                ROUND(
                    COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) * 1.0 / 
                    NULLIF(COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + SUM(CASE 
                        WHEN p.event_type = 'SHOT' THEN COALESCE(b.expected_loss_rate, od.default_loss_rate)
                        ELSE 0.0 
                    END), 0),
                    4
                ) AS normalized_shooting_efficiency,
                ROUND(
                    COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) * 1.0 / 
                    NULLIF(COUNT(CASE WHEN p.event_type IN ('SHOT', 'GOAL') THEN 1 END), 0),
                    4
                ) AS shooting_pct
            FROM fact_plays p
            CROSS JOIN overall_default od
            LEFT JOIN baseline_rates b ON p.shot_result = b.shot_result
            {where_fact}
        """
        overall_rows = self.execute_query(overall_sql, params_fact)
        overall = overall_rows[0] if overall_rows else {}
        overall["goals"] = overall.get("goals") or 0
        overall["total_shots"] = overall.get("total_shots") or 0
        overall["shots_retained"] = overall.get("shots_retained") or 0
        overall["realized_shots_lost"] = overall.get("realized_shots_lost") or 0
        overall["normalized_shots_lost"] = overall.get("normalized_shots_lost") or 0.0
        overall["realized_shot_possessions_used"] = overall.get("realized_shot_possessions_used") or 0
        overall["normalized_shot_possessions_used"] = overall.get("normalized_shot_possessions_used") or 0.0
        overall["realized_shooting_efficiency"] = overall.get("realized_shooting_efficiency") or 0.0
        overall["normalized_shooting_efficiency"] = overall.get("normalized_shooting_efficiency") or 0.0
        overall["shooting_pct"] = overall.get("shooting_pct") or 0.0

        avg_team_sql = f"""
            SELECT 
                ROUND(AVG(realized_shooting_efficiency), 4) as avg_realized_eff,
                ROUND(AVG(normalized_shooting_efficiency), 4) as avg_normalized_eff
            FROM agg_team_game_stats 
            {where_fact_no_p}
        """
        avg_team_rows = self.execute_query(avg_team_sql, params_fact)
        if avg_team_rows and avg_team_rows[0].get("avg_realized_eff") is not None:
            overall["average_team_game_realized_efficiency"] = avg_team_rows[0]["avg_realized_eff"]
            overall["average_team_game_normalized_efficiency"] = avg_team_rows[0]["avg_normalized_eff"]
        else:
            overall["average_team_game_realized_efficiency"] = None
            overall["average_team_game_normalized_efficiency"] = None

        # 2. Retention by Shot Result
        retention_cid_clause = "AND contest_id = ?" if contest_id is not None else ""
        retention_params = [contest_id] if contest_id is not None else []
        retention_sql = f"""
            SELECT 
                CASE 
                    WHEN shot_result = 'POST' THEN 'HIT POST'
                    WHEN shot_result = 'CROSSBAR' THEN 'HIT CROSSBAR'
                    ELSE shot_result 
                END AS shot_result,
                COUNT(*) AS total_shots,
                COUNT(CASE WHEN shot_possession_retained = true THEN 1 END) AS shots_retained,
                COUNT(CASE WHEN shot_possession_retained = false THEN 1 END) AS shots_lost,
                ROUND(
                    COUNT(CASE WHEN shot_possession_retained = true THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0),
                    4
                ) AS retention_rate
            FROM fact_plays
            WHERE event_type = 'SHOT' 
              AND shot_result IN ('SAVE', 'WIDE', 'HIGH', 'POST', 'CROSSBAR', 'BLOCKED')
              {retention_cid_clause}
            GROUP BY 1
        """
        retention_rows = self.execute_query(retention_sql, retention_params)
        retention_dict = {r["shot_result"]: r for r in retention_rows}
        desired_order = ["SAVE", "WIDE", "HIGH", "HIT POST", "HIT CROSSBAR", "BLOCKED"]
        ordered_retention = []
        for sr in desired_order:
            if sr in retention_dict:
                row = retention_dict[sr]
                row["retention_rate"] = row["retention_rate"] or 0.0
                ordered_retention.append(row)
            else:
                ordered_retention.append({
                    "shot_result": sr,
                    "total_shots": 0,
                    "shots_retained": 0,
                    "shots_lost": 0,
                    "retention_rate": 0.0,
                })

        # 3. Top Players
        player_cid_clause = "AND p.contest_id = ?" if contest_id is not None else ""
        player_params = (
            [contest_id, min_player_shots, top_players_limit]
            if contest_id is not None
            else [min_player_shots, top_players_limit]
        )
        players_sql = f"""
            WITH baseline_rates AS (
                SELECT 
                    shot_result,
                    1.0 - (COUNT(CASE WHEN shot_possession_retained = true THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0)) AS expected_loss_rate
                FROM fact_plays
                WHERE event_type = 'SHOT'
                GROUP BY shot_result
            ),
            overall_default AS (
                SELECT 
                    1.0 - (COUNT(CASE WHEN shot_possession_retained = true THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0)) AS default_loss_rate
                FROM fact_plays
                WHERE event_type = 'SHOT'
            )
            SELECT 
                p.primary_player_name AS player_name,
                COALESCE(d.name_short, MAX(p.event_team_short)) AS team_name,
                p.team_id,
                COUNT(CASE WHEN p.event_type IN ('SHOT', 'GOAL') THEN 1 END) AS total_shots,
                COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) AS goals,
                COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = true THEN 1 END) AS shots_retained,
                COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = false THEN 1 END) AS realized_shots_lost,
                ROUND(SUM(CASE 
                    WHEN p.event_type = 'SHOT' THEN COALESCE(b.expected_loss_rate, od.default_loss_rate)
                    ELSE 0.0 
                END), 4) AS normalized_shots_lost,
                COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = false THEN 1 END) AS realized_shot_possessions_used,
                ROUND(COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + SUM(CASE 
                    WHEN p.event_type = 'SHOT' THEN COALESCE(b.expected_loss_rate, od.default_loss_rate)
                    ELSE 0.0 
                END), 4) AS normalized_shot_possessions_used,
                ROUND(
                    COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) * 1.0 / 
                    NULLIF(COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = false THEN 1 END), 0),
                    4
                ) AS realized_shooting_efficiency,
                ROUND(
                    COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) * 1.0 / 
                    NULLIF(COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + SUM(CASE 
                        WHEN p.event_type = 'SHOT' THEN COALESCE(b.expected_loss_rate, od.default_loss_rate)
                        ELSE 0.0 
                    END), 0),
                    4
                ) AS normalized_shooting_efficiency,
                ROUND(
                    COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) * 1.0 / 
                    NULLIF(COUNT(CASE WHEN p.event_type IN ('SHOT', 'GOAL') THEN 1 END), 0),
                    4
                ) AS shooting_pct
            FROM fact_plays p
            CROSS JOIN overall_default od
            LEFT JOIN baseline_rates b ON p.shot_result = b.shot_result
            LEFT JOIN dim_teams d ON p.team_id = d.team_id
            WHERE p.event_type IN ('SHOT', 'GOAL') 
              AND p.primary_player_name IS NOT NULL
              AND p.primary_player_name NOT IN ('TEAM', 'TM')
              {player_cid_clause}
            GROUP BY p.primary_player_name, p.team_id, d.name_short
            HAVING total_shots >= ?
            ORDER BY {order_col} DESC, goals DESC, total_shots DESC, player_name ASC
            LIMIT ?
        """
        top_players = self.execute_query(players_sql, player_params)
        for p in top_players:
            p["realized_shooting_efficiency"] = p.get("realized_shooting_efficiency") or 0.0
            p["normalized_shooting_efficiency"] = p.get("normalized_shooting_efficiency") or 0.0
            p["shooting_pct"] = p.get("shooting_pct") or 0.0
            p["realized_shots_lost"] = p.get("realized_shots_lost") or 0
            p["normalized_shots_lost"] = p.get("normalized_shots_lost") or 0.0
            p["realized_shot_possessions_used"] = p.get("realized_shot_possessions_used") or 0
            p["normalized_shot_possessions_used"] = p.get("normalized_shot_possessions_used") or 0.0

        # 4. Top Teams
        team_cid_clause = "AND p.contest_id = ?" if contest_id is not None else ""
        team_params = [contest_id, top_teams_limit] if contest_id is not None else [top_teams_limit]
        teams_sql = f"""
            WITH baseline_rates AS (
                SELECT 
                    shot_result,
                    1.0 - (COUNT(CASE WHEN shot_possession_retained = true THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0)) AS expected_loss_rate
                FROM fact_plays
                WHERE event_type = 'SHOT'
                GROUP BY shot_result
            ),
            overall_default AS (
                SELECT 
                    1.0 - (COUNT(CASE WHEN shot_possession_retained = true THEN 1 END) * 1.0 / NULLIF(COUNT(*), 0)) AS default_loss_rate
                FROM fact_plays
                WHERE event_type = 'SHOT'
            )
            SELECT 
                p.team_id,
                COALESCE(d.name_short, MAX(p.event_team_short)) AS team_name,
                COALESCE(d.name_full, MAX(p.event_team_short)) AS team_name_full,
                list_sort(list(distinct p.event_team_short)) AS team_short_names,
                COUNT(DISTINCT p.contest_id) AS games_played,
                COUNT(CASE WHEN p.event_type IN ('SHOT', 'GOAL') THEN 1 END) AS total_shots,
                COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) AS goals,
                COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = true THEN 1 END) AS shots_retained,
                COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = false THEN 1 END) AS realized_shots_lost,
                ROUND(SUM(CASE 
                    WHEN p.event_type = 'SHOT' THEN COALESCE(b.expected_loss_rate, od.default_loss_rate)
                    ELSE 0.0 
                END), 4) AS normalized_shots_lost,
                COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = false THEN 1 END) AS realized_shot_possessions_used,
                ROUND(COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + SUM(CASE 
                    WHEN p.event_type = 'SHOT' THEN COALESCE(b.expected_loss_rate, od.default_loss_rate)
                    ELSE 0.0 
                END), 4) AS normalized_shot_possessions_used,
                ROUND(
                    COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) * 1.0 / 
                    NULLIF(COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + COUNT(CASE WHEN p.event_type = 'SHOT' AND p.shot_possession_retained = false THEN 1 END), 0),
                    4
                ) AS realized_shooting_efficiency,
                ROUND(
                    COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) * 1.0 / 
                    NULLIF(COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) + SUM(CASE 
                        WHEN p.event_type = 'SHOT' THEN COALESCE(b.expected_loss_rate, od.default_loss_rate)
                        ELSE 0.0 
                    END), 0),
                    4
                ) AS normalized_shooting_efficiency,
                ROUND(
                    COUNT(CASE WHEN p.event_type = 'GOAL' THEN 1 END) * 1.0 / 
                    NULLIF(COUNT(CASE WHEN p.event_type IN ('SHOT', 'GOAL') THEN 1 END), 0),
                    4
                ) AS shooting_pct
            FROM fact_plays p
            CROSS JOIN overall_default od
            LEFT JOIN baseline_rates b ON p.shot_result = b.shot_result
            LEFT JOIN dim_teams d ON p.team_id = d.team_id
            WHERE p.team_id IS NOT NULL
              {team_cid_clause}
            GROUP BY p.team_id, d.name_short, d.name_full
            ORDER BY {order_col} DESC, goals DESC, total_shots DESC, team_name ASC
            LIMIT ?
        """
        top_teams = self.execute_query(teams_sql, team_params)
        for t in top_teams:
            t["realized_shooting_efficiency"] = t.get("realized_shooting_efficiency") or 0.0
            t["normalized_shooting_efficiency"] = t.get("normalized_shooting_efficiency") or 0.0
            t["shooting_pct"] = t.get("shooting_pct") or 0.0
            t["realized_shots_lost"] = t.get("realized_shots_lost") or 0
            t["normalized_shots_lost"] = t.get("normalized_shots_lost") or 0.0
            t["realized_shot_possessions_used"] = t.get("realized_shot_possessions_used") or 0
            t["normalized_shot_possessions_used"] = t.get("normalized_shot_possessions_used") or 0.0
            t["team_short_names"] = [name for name in (t.get("team_short_names") or []) if name]

        return {
            "contest_id": contest_id,
            "min_player_shots": min_player_shots,
            "order_by": "normalized" if normalized_order else "realized",
            "overall": overall,
            "retention_by_shot_result": ordered_retention,
            "top_players": top_players,
            "top_teams": top_teams,
        }


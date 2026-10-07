"""Team labels must work for new IDs and remain local to their contest."""
import json

from src.etl.transform import (
    _embedded_ground_balls,
    extract_dimensions_from_silver_contests,
    process_bronze_to_silver,
    process_bronze_to_silver_contests,
)


def _snapshot(tmp_path, contest_id, plays):
    payload = {"data": {"playbyplay": {
        "contestId": contest_id,
        "teams": [
            {"teamId": "101", "nameShort": "Lake Valley", "name6Char": "LAKEV",
             "nameFull": "Lake Valley University", "isHome": True},
            {"teamId": "202", "nameShort": "Coastal Tech", "name6Char": "COAST",
             "nameFull": "Coastal Technical University", "isHome": False},
        ],
        "periods": [{"periodNumber": 1, "periodDisplay": "1st", "playbyplayStats": [
            {"teamId": team_id, "clock": "15:00", "plays": [{"playText": text}]}
            for team_id, text in plays
        ]}],
    }}}
    path = tmp_path / f"{contest_id}.json"
    path.write_text(json.dumps(payload))
    return contest_id, "20261006T120000Z", path


def test_metadata_infers_new_multiword_truncated_and_initial_labels(spark, tmp_path):
    snapshot = _snapshot(tmp_path, 1, [
        (None, "GOAL by Lake Valley Alex Adams."),
        (None, "Shot by LAKE VAL Ben Baker WIDE."),
        (None, "Ground ball pickup by LV Chris Cole."),
        (None, "Penalty on Coastal Tech Dan Davis (SLASHING/1:00)."),
        (None, "Ground ball pickup by CTU Evan Evans."),
    ])
    rows = process_bronze_to_silver(spark, snapshot).orderBy("play_seq").collect()
    assert [row.team_id for row in rows] == [101, 101, 101, 202, 202]
    assert [row.primary_player_name for row in rows] == [
        "Alex Adams", "Ben Baker", "Chris Cole", "Dan Davis", "Evan Evans",
    ]
    assert rows[1].event_team_short == "LAKE VAL"


def test_observed_aliases_apply_to_earlier_plays_without_crossing_contests(spark, tmp_path):
    snapshots = [
        _snapshot(tmp_path, 1, [
            (None, "Ground ball pickup by X99 Alex Adams."),
            (101, "Shot by X99 Alex Adams WIDE."),
        ]),
        _snapshot(tmp_path, 2, [
            (None, "Ground ball pickup by X99 Ben Baker."),
            (202, "Ben Baker at goalie for X99."),
        ]),
    ]
    rows = process_bronze_to_silver(spark, snapshots).orderBy("contest_id", "play_seq").collect()
    assert [row.team_id for row in rows] == [101, 101, 202, 202]


def test_conflicting_alias_without_source_team_stays_unresolved(spark, tmp_path):
    snapshot = _snapshot(tmp_path, 1, [
        (101, "Clear attempt by X99 good."),
        (202, "Clear attempt by X99 good."),
        (None, "Ground ball pickup by X99 Alex Adams."),
    ])
    rows = process_bronze_to_silver(spark, snapshot).orderBy("play_seq").collect()
    assert [row.team_id for row in rows] == [101, 202, None]


def test_observed_alias_takes_precedence_over_shared_initials(spark, tmp_path):
    snapshot = _snapshot(tmp_path, 1, [
        (None, "Ground ball pickup by LV Alex Adams."),
        (101, "Clear attempt by LV good."),
    ])
    payload = json.loads(snapshot[2].read_text())
    away = payload["data"]["playbyplay"]["teams"][1]
    away.update(nameShort="Long View", nameFull="Long View University")
    snapshot[2].write_text(json.dumps(payload))
    rows = process_bronze_to_silver(spark, snapshot).orderBy("play_seq").collect()
    assert [row.team_id for row in rows] == [101, 101]


def test_faceoff_and_gold_use_inferred_collector_alias(spark, tmp_path):
    snapshot = _snapshot(tmp_path, 1, [
        (None, "Faceoff Alex Adams vs Ben Baker won by X99, Ground ball pickup by Y88 Ben Baker."),
        (101, "Clear attempt by X99 good."),
        (202, "Turnover by Y88 Ben Baker."),
    ])
    silver = process_bronze_to_silver(spark, snapshot).cache()
    try:
        row = silver.filter("event_type = 'FACEOFF'").first()
        assert row.team_id == 101
        assert row.faceoff_winner_player == "Alex Adams"
        assert row.faceoff_loser_player == "Ben Baker"
        teams, _ = extract_dimensions_from_silver_contests(
            process_bronze_to_silver_contests(spark, [snapshot]),
        )
        ground_balls = _embedded_ground_balls(silver, teams).collect()
        assert len(ground_balls) == 1
        assert ground_balls[0].team_id == 202
        assert ground_balls[0].event_team_short == "Y88"
    finally:
        silver.unpersist()

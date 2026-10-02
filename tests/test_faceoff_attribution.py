import json

from src.etl.transform import (
    get_latest_valid_snapshots,
    process_bronze_to_silver,
)


def _snapshot(tmp_path, contest_id, texts, nd_home=True):
    teams = [
        {"teamId": "43861", "nameShort": "ND", "name6Char": "ND", "isHome": nd_home},
        {"teamId": "43731", "nameShort": "PU", "name6Char": "PU", "isHome": not nd_home},
    ]
    payload = {"data": {"playbyplay": {
        "contestId": contest_id, "title": "ND vs PU", "status": "final", "teams": teams,
        "periods": [{"periodNumber": 1, "periodDisplay": "1st", "playbyplayStats": [
            {"clock": "15:00", "teamId": None, "plays": [{"playText": text}]} for text in texts
        ]}],
    }}}
    path = tmp_path / f"{contest_id}.json"
    path.write_text(json.dumps(payload))
    return contest_id, "20260927T120000Z", path


def test_faceoff_uses_contest_evidence_for_either_order_and_winner(spark, tmp_path):
    snapshots, expected = [], {}
    for nd_home in (True, False):
        for reverse in (True, False):
            for winning_code in ("ND", "PU"):
                cid = len(snapshots) + 1
                participants = "Baker, Ben vs Adams, Alex" if reverse else "Adams, Alex vs Baker, Ben"
                # Evidence deliberately appears after the faceoff.
                texts = [
                    f"Faceoff {participants} won by {winning_code} (on faceoff violation).",
                    "Ground ball pickup by ND Alex Adams.",
                    "Turnover by PU Ben Baker.",
                ]
                snapshots.append(_snapshot(tmp_path, cid, texts, nd_home))
                expected[cid] = ("Alex Adams", "Ben Baker") if winning_code == "ND" else ("Ben Baker", "Alex Adams")
    # The same player names represent opposite teams in a different contest.
    snapshots.append(_snapshot(tmp_path, 9, [
        "Faceoff Alex Adams vs Ben Baker won by ND.",
        "Ground ball pickup by PU Alex Adams.",
        "Ground ball pickup by ND Ben Baker.",
    ]))
    expected[9] = ("Ben Baker", "Alex Adams")
    rows = process_bronze_to_silver(spark, snapshots).filter("event_type = 'FACEOFF'").collect()
    assert len(rows) == len(expected)
    for row in rows:
        assert (row.faceoff_winner_player, row.faceoff_loser_player) == expected[row.contest_id]


def test_faceoff_leaves_unresolved_and_conflicting_names_null(spark, tmp_path):
    texts = [
        "Faceoff Unknown One vs Unknown Two won by ND.",
        "Faceoff Unknown One vs Unknown Two won by ND, Ground ball pickup by ND Third Player.",
        "Faceoff Alex Adams vs Ben Baker won by ND.",
        "Ground ball pickup by ND Alex Adams.",
        "Ground ball pickup by PU Alex Adams.",
        "Ground ball pickup by PU Ben Baker.",
    ]
    df = process_bronze_to_silver(spark, _snapshot(tmp_path, 100, texts))
    rows = df.filter("event_type = 'FACEOFF'").collect()
    assert len(rows) == 3
    assert all(row.faceoff_winner_player is None and row.faceoff_loser_player is None for row in rows)


def test_faceoff_can_resolve_from_one_participant_and_embedded_evidence(spark, tmp_path):
    texts = [
        "Faceoff Unknown One vs Baker, Ben won by ND.",
        "Faceoff Alex Adams vs Unknown Two won by ND, Ground ball pickup by ND Alex Adams.",
        "Turnover by PU Ben Baker.",
    ]
    rows = (process_bronze_to_silver(spark, _snapshot(tmp_path, 101, texts))
            .filter("event_type = 'FACEOFF'").orderBy("play_seq").collect())
    assert (rows[0].faceoff_winner_player, rows[0].faceoff_loser_player) == ("Unknown One", "Ben Baker")
    assert (rows[1].faceoff_winner_player, rows[1].faceoff_loser_player) == ("Alex Adams", "Unknown Two")


def test_batch_parser_resolves_faceoff_participants_in_either_order(spark, tmp_path):
    texts = [
        "Faceoff Cal Girard vs McMeekin, Andrew won by PU, Ground ball pickup by PU McMeekin, Andrew.",
        "Faceoff McMeekin, Andrew vs Cal Girard won by PU, Ground ball pickup by PU McMeekin, Andrew.",
        "Faceoff Cal Girard vs McMeekin, Andrew won by PU (on faceoff violation).",
    ]
    snapshots = [
        _snapshot(tmp_path, 102, texts[:2]),
        _snapshot(tmp_path, 103, texts[2:]),
    ]
    rows = process_bronze_to_silver(spark, snapshots).filter("event_type = 'FACEOFF'").collect()
    assert len(rows) == 3
    for row in rows:
        expected = ("Andrew McMeekin", "Cal Girard") if row.contest_id == 102 else (None, None)
        assert (row.faceoff_winner_player, row.faceoff_loser_player) == expected

def test_real_contest_faceoff_regression(spark):
    snapshot = next(s for s in get_latest_valid_snapshots() if s[0] == 6599994)
    row = process_bronze_to_silver(spark, snapshot).filter("play_id = '6599994_3'").first()
    assert row.faceoff_winner_player == "Andrew McMeekin"
    assert row.faceoff_loser_player == "Cal Girard"

import random

import duckdb
import pytest

from src.db.duckdb_client import DuckDBClient


@pytest.mark.parametrize("order_by", [
    "play_seq", "game_seconds_elapsed", "game_seconds_remaining_reg",
    "period_seconds_remaining", "period_number", "play_id", "invalid_column",
])
@pytest.mark.parametrize("descending", [False, True])
def test_play_pages_have_stable_tie_breakers(order_by, descending):
    columns = ["contest_id", "play_id", "play_seq", "game_seconds_elapsed",
               "game_seconds_remaining_reg", "period_seconds_remaining", "period_number"]
    rows = [
        (contest, f"{contest}_{seq}", seq, (seq // 2) * 10, 3600 - (seq // 2) * 10, 900, 1)
        for contest in (10, 2, 30) for seq in (1, 2, 3, 4)
    ]
    random.Random(42).shuffle(rows)
    client = DuckDBClient.__new__(DuckDBClient)
    client.conn = duckdb.connect(":memory:")
    try:
        client.conn.execute("""CREATE TABLE fact_plays (
            contest_id BIGINT, play_id VARCHAR, play_seq INTEGER,
            game_seconds_elapsed INTEGER, game_seconds_remaining_reg INTEGER,
            period_seconds_remaining INTEGER, period_number INTEGER
        )""")
        actual_order = order_by if order_by in columns else "play_seq"
        # Stable primary sort preserves canonical ascending tie order.
        expected = sorted(rows, key=lambda row: (row[0], row[2], row[1]))
        expected.sort(key=lambda row: row[columns.index(actual_order)], reverse=descending)
        expected_ids = [row[1] for row in expected]
        for insertion_order in (rows, list(reversed(rows))):
            client.conn.execute("DELETE FROM fact_plays")
            client.conn.executemany("INSERT INTO fact_plays VALUES (?, ?, ?, ?, ?, ?, ?)", insertion_order)
            actual_ids = []
            for offset in range(0, len(rows), 2):
                page = client.get_plays(order_by=order_by, order_desc=descending, limit=2, offset=offset)
                actual_ids.extend(row["play_id"] for row in page)
            assert actual_ids == expected_ids
            assert len(set(actual_ids)) == len(rows)
    finally:
        client.conn.close()

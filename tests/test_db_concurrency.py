from concurrent.futures import ThreadPoolExecutor
import threading

import duckdb
import pytest

from src.db.duckdb_client import DuckDBClient


@pytest.fixture
def db_client():
    """Create a client without filesystem-backed views for concurrency tests."""
    client = DuckDBClient.__new__(DuckDBClient)
    client.conn = duckdb.connect(":memory:")
    client.conn.execute(
        "CREATE TABLE numbers AS SELECT range AS value FROM range(0, 1000)"
    )
    try:
        yield client
    finally:
        client.conn.close()


def test_execute_query_keeps_concurrent_results_isolated(db_client):
    worker_count = 24
    barrier = threading.Barrier(worker_count)

    def query(request_id: int):
        upper_bound = request_id + 10
        barrier.wait()
        return db_client.execute_query(
            """
            SELECT ?::INTEGER AS request_id, SUM(value) AS value_sum
            FROM numbers
            WHERE value < ?
            """,
            [request_id, upper_bound],
        )[0]

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        results = list(executor.map(query, range(worker_count)))

    assert {result["request_id"] for result in results} == set(range(worker_count))
    for result in results:
        upper_bound = result["request_id"] + 10
        assert result["value_sum"] == upper_bound * (upper_bound - 1) // 2


def test_failed_query_does_not_corrupt_concurrent_results(db_client):
    worker_count = 16
    barrier = threading.Barrier(worker_count)

    def successful_query(request_id: int):
        barrier.wait()
        return db_client.execute_query(
            "SELECT ?::INTEGER AS request_id, COUNT(*) AS count FROM numbers WHERE value < ?",
            [request_id, request_id + 1],
        )[0]

    def invalid_query():
        barrier.wait()
        with pytest.raises(duckdb.Error):
            db_client.execute_query("SELECT missing_column FROM numbers")

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(successful_query, request_id) for request_id in range(worker_count - 1)]
        futures.append(executor.submit(invalid_query))
        results = [future.result() for future in futures]

    assert results[-1] is None
    for result in results[:-1]:
        assert result["count"] == result["request_id"] + 1

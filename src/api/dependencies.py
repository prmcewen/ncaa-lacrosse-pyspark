"""One immutable Gold snapshot per REST or GraphQL request."""
from typing import Annotated, Iterator

from fastapi import Depends

from src.db.duckdb_client import DuckDBClient


def get_db() -> Iterator[DuckDBClient]:
    client = DuckDBClient(include_silver=False)
    try:
        yield client
    finally:
        client.close()


RequestDB = Annotated[DuckDBClient, Depends(get_db)]


def graphql_context(db: RequestDB) -> dict:
    return {"db": db}

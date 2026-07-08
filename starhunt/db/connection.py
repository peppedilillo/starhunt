"""Database connection helpers."""

import os

import psycopg
from psycopg import Connection


def init_db_conn() -> Connection:
    """Create a database connection from environment variables.

    Returns:
        A psycopg database connection.
    """
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

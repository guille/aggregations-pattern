from contextlib import contextmanager

import psycopg

from aggregations.config import SOURCE_DB_CONNECTION, TARGET_DB_CONNECTION


class DB:
    def __init__(self, conn_string: str):
        self._conn_string: str = conn_string

    @contextmanager
    def cursor(self):
        with psycopg.connect(self._conn_string, autocommit=True) as conn:
            with conn.cursor() as cur:
                yield cur

    @contextmanager
    def transaction(self):
        with psycopg.connect(self._conn_string, autocommit=True) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    yield conn, cur


source_db = DB(SOURCE_DB_CONNECTION)
target_db = DB(TARGET_DB_CONNECTION)

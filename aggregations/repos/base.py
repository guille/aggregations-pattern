from contextlib import contextmanager
from aggregations.db import DB
from psycopg import Cursor
from typing import Optional


class BaseRepo:
    _db: DB

    @classmethod
    @contextmanager
    def _with_cursor(cls, cur: Optional[Cursor] = None):
        if cur is not None:
            yield cur
        else:
            with cls._db.cursor() as c:
                yield c

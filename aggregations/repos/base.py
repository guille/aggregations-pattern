from abc import ABC
from collections.abc import Generator
from contextlib import contextmanager
from typing import ClassVar

from psycopg import Cursor

from aggregations.db import DB


class BaseRepo(ABC):
    _db: ClassVar[DB]

    @classmethod
    @contextmanager
    def _with_cursor(cls, cur: Cursor | None = None) -> Generator[Cursor, None, None]:
        if cur is not None:
            yield cur
        else:
            with cls._db.cursor() as c:
                yield c

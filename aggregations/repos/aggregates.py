from decimal import Decimal
from typing import cast, final

from psycopg import Cursor

from aggregations.db import target_db
from aggregations.repos.base import BaseRepo


@final
class AggregatesRepo(BaseRepo):
    _db = target_db

    @classmethod
    def get_total(cls, cursor: Cursor | None = None) -> Decimal:
        with cls._with_cursor(cursor) as cur:
            result = cur.execute("SELECT total FROM aggregates LIMIT 1").fetchone()
            if result:
                return cast(Decimal, result[0])
            return Decimal(0)

    @classmethod
    def update(cls, old_total: Decimal, total: Decimal, cursor: Cursor | None = None):
        with cls._with_cursor(cursor) as cur:
            _ = cur.execute(
                "UPDATE aggregates SET total = total - %s + %s",
                (
                    old_total,
                    total,
                ),
            )

from aggregations.db import target_db
from aggregations.repos.base import BaseRepo
from decimal import Decimal


class AggregatesRepo(BaseRepo):
    _db = target_db

    @classmethod
    def get_total(cls, cursor=None) -> Decimal:
        with cls._with_cursor(cursor) as cur:
            cur.execute("SELECT total FROM aggregates LIMIT 1")
            if result := cur.fetchone():
                return result[0]
            return Decimal(0)

    @classmethod
    def update(cls, old_total: Decimal, total: Decimal, cursor=None):
        with cls._with_cursor(cursor) as cur:
            cur.execute(
                "UPDATE aggregates SET total = total - %s + %s",
                (
                    old_total,
                    total,
                ),
            )

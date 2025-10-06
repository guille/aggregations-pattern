from decimal import Decimal
from typing import final

from psycopg import Cursor

from aggregations.db import target_db
from aggregations.repos.base import BaseRepo


@final
class CdcEntriesRepo(BaseRepo):
    _db = target_db

    @classmethod
    def get_for_update(cls, invoice_id: int, cursor: Cursor | None = None):
        with cls._with_cursor(cursor) as cur:
            return cur.execute(
                "SELECT lsn, total FROM cdc_entries WHERE invoice_id=%s FOR UPDATE",
                (invoice_id,),
            ).fetchone()

    @classmethod
    def create(
        cls, invoice_id: int, total: Decimal, lsn: int, cursor: Cursor | None = None
    ):
        with cls._with_cursor(cursor) as cur:
            _ = cur.execute(
                "INSERT INTO cdc_entries (invoice_id, total, lsn) VALUES (%s, %s, %s)",
                (invoice_id, total, lsn),
            )

    @classmethod
    def update(
        cls, invoice_id: int, total: Decimal, lsn: int, cursor: Cursor | None = None
    ):
        with cls._with_cursor(cursor) as cur:
            _ = cur.execute(
                "UPDATE cdc_entries SET total=%s, lsn=%s WHERE invoice_id=%s",
                (total, lsn, invoice_id),
            )

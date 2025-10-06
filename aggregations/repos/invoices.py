from typing import final

from psycopg import Cursor

from aggregations.db import source_db
from aggregations.repos.base import BaseRepo


@final
class InvoicesRepo(BaseRepo):
    _db = source_db

    @classmethod
    def get_all(cls, cursor: Cursor | None = None):
        with cls._with_cursor(cursor) as cur:
            return cur.execute(
                "SELECT id, name, total FROM invoices LIMIT 250"
            ).fetchall()

    @classmethod
    def get(cls, invoice_id: int, limit: int = 100, cursor: Cursor | None = None):
        with cls._with_cursor(cursor) as cur:
            return cur.execute(
                "SELECT id, name, total FROM invoices WHERE id=%s LIMIT %s",
                (invoice_id, limit),
            ).fetchone()

    @classmethod
    def create(cls, name: str, total: int, cursor: Cursor | None = None):
        with cls._with_cursor(cursor) as cur:
            return cur.execute(
                "INSERT INTO invoices (name, total) VALUES (%s, %s)",
                (name, total),
            )

    @classmethod
    def update(
        cls,
        invoice_id: int,
        new_name: str,
        new_total: int,
        cursor: Cursor | None = None,
    ):
        with cls._with_cursor(cursor) as cur:
            return cur.execute(
                "UPDATE invoices SET name=%s, total=%s WHERE id=%s",
                (new_name, new_total, invoice_id),
            )

    @classmethod
    def delete(cls, invoice_id: int, cursor: Cursor | None = None):
        with cls._with_cursor(cursor) as cur:
            return cur.execute(
                "DELETE FROM invoices WHERE id=%s",
                (invoice_id,),
            )

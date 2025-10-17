from decimal import Decimal
from enum import StrEnum

import msgspec


class Invoice(msgspec.Struct):
    id: int
    total: Decimal


class EventSource(msgspec.Struct):
    lsn: int


class OpTypes(StrEnum):
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"


class CreatePayload(msgspec.Struct, tag_field="op", tag=str(OpTypes.CREATE)):
    source: EventSource
    after: Invoice


class UpdatePayload(msgspec.Struct, tag_field="op", tag=str(OpTypes.UPDATE)):
    source: EventSource
    after: Invoice


class DeletePayload(msgspec.Struct, tag_field="op", tag=str(OpTypes.DELETE)):
    source: EventSource
    before: Invoice


EventPayload = CreatePayload | UpdatePayload | DeletePayload


class EventMessage(msgspec.Struct):
    payload: EventPayload

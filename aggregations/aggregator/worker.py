import json
import traceback
from collections.abc import Mapping
from contextlib import contextmanager
from decimal import Decimal
from typing import Any

import pulsar

from aggregations.config import PULSAR_URL
from aggregations.db import target_db
from aggregations.repos.aggregates import AggregatesRepo
from aggregations.repos.cdc_entries import CdcEntriesRepo

decimal_zero = Decimal(0)


@contextmanager
def pulsar_client(*args, **kwargs):
    client = pulsar.Client(*args, **kwargs)
    try:
        yield client
    except pulsar.Interrupted:
        print("Exiting...")
    finally:
        client.close()


def create(payload: Mapping[str, Any]):
    event_lsn = payload["source"]["lsn"]
    new_record = payload["after"]

    with target_db.transaction() as (conn, cur):
        existing_row = CdcEntriesRepo.get_for_update(new_record["id"], cursor=cur)

        if existing_row:
            existing_lsn = existing_row[0]
            if existing_lsn >= event_lsn:
                print("Outdated or duplicated event, not processing anything")
                return
            else:
                # If we get here, some event earlier than this (in the DB, not in receiver order)
                # has already created the row. Shouldn't happen
                # Basically: a deleted primary key is being reused!
                raise Exception("Conflict!! Something has gone terribly wrong!")

        CdcEntriesRepo.create(
            new_record["id"], new_record["total"], event_lsn, cursor=cur
        )
        AggregatesRepo.update(
            old_total=decimal_zero, total=new_record["total"], cursor=cur
        )


def update(payload: Mapping[str, Any]):
    event_lsn = payload["source"]["lsn"]
    new_record = payload["after"]

    with target_db.transaction() as (conn, cur):
        existing_row = CdcEntriesRepo.get_for_update(new_record["id"], cursor=cur)

        if existing_row:
            existing_lsn = existing_row[0]
            existing_total = existing_row[1]
            if existing_lsn >= event_lsn:
                print("Outdated or duplicated event, not processing anything")
                return
            else:
                CdcEntriesRepo.update(
                    new_record["id"], new_record["total"], event_lsn, cursor=cur
                )
        else:
            existing_total = decimal_zero
            CdcEntriesRepo.create(
                new_record["id"], new_record["total"], event_lsn, cursor=cur
            )
        AggregatesRepo.update(
            old_total=existing_total, total=new_record["total"], cursor=cur
        )


def delete(payload: Mapping[str, Any]):
    event_lsn = payload["source"]["lsn"]
    old_record = payload["before"]

    with target_db.transaction() as (conn, cur):
        existing_row = CdcEntriesRepo.get_for_update(old_record["id"], cursor=cur)

        if existing_row:
            existing_lsn = existing_row[0]
            existing_total = existing_row[1]
            if existing_lsn >= event_lsn:
                print("Outdated or duplicated event, not processing anything")
                return
            else:
                CdcEntriesRepo.update(
                    old_record["id"], decimal_zero, event_lsn, cursor=cur
                )
        else:
            existing_total = decimal_zero
            CdcEntriesRepo.create(old_record["id"], decimal_zero, event_lsn, cursor=cur)
        AggregatesRepo.update(old_total=existing_total, total=decimal_zero, cursor=cur)


def main():
    with pulsar_client(PULSAR_URL) as client:
        consumer = client.subscribe(
            "aggregations.public.invoices", "aggregations-cdc-subscription"
        )
        while True:
            msg = consumer.receive()
            try:
                if msg.data() == b"":
                    consumer.acknowledge(msg)
                    continue

                parsed = json.loads(msg.data())
                print("Received message payload '{}'".format(parsed["payload"]))

                match parsed["payload"]["op"]:
                    case "c":
                        create(parsed["payload"])
                    case "u":
                        update(parsed["payload"])
                    case "d":
                        delete(parsed["payload"])
                    case _:
                        print(f"Unrecognised operation: {parsed['payload']['op']}")
                        consumer.negative_acknowledge(msg)
                print("ACKed!")
                consumer.acknowledge(msg)
            except Exception as exc:
                print(exc)
                traceback.print_exc()
                print(msg.data())
                print("NACKed!")
                consumer.negative_acknowledge(msg)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Append rows to a local Apache Paimon filesystem catalog.

The warehouse directory is the durable store. A delivery id is a column, so
reopening the catalog and repeating the delivery does not write again.
"""
import pyarrow as pa
from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.schema.schema import Schema


class PaimonSink:
    name = "paimon"
    direction = "sink"
    semantic = "effectively_once"

    def __init__(self, warehouse="file:///tmp/m2s-paimon", database="m2s", table="rows"):
        self.warehouse = str(warehouse)
        self.database = str(database)
        self.table_name = str(table)

    def _catalog(self):
        return CatalogFactory.create({"warehouse": self.warehouse})

    def _identifier(self):
        return self.database + "." + self.table_name

    def ensure(self):
        catalog = self._catalog()
        catalog.create_database(self.database, True)
        catalog.drop_table(self._identifier(), True)
        schema = Schema.from_pyarrow_schema(
            pa.schema([
                pa.field("id", pa.int64(), nullable=False),
                pa.field("v", pa.int64(), nullable=False),
                pa.field("delivery_id", pa.string(), nullable=False),
            ]),
            primary_keys=["id"],
            options={"bucket": "1"},
        )
        catalog.create_table(self._identifier(), schema, False)

    def _table(self):
        return self._catalog().get_table(self._identifier())

    def _arrow(self):
        table = self._table()
        builder = table.new_read_builder()
        splits = builder.new_scan().plan().splits()
        if not splits:
            return pa.table({
                "id": pa.array([], type=pa.int64()),
                "v": pa.array([], type=pa.int64()),
                "delivery_id": pa.array([], type=pa.string()),
            })
        return builder.new_read().to_arrow(splits)

    def write(self, delivery_id, rows):
        delivery_id = str(delivery_id)
        seen = self._arrow().column("delivery_id").to_pylist()
        if delivery_id in seen:
            return False
        if not rows:
            return True
        frame = pa.table({
            "id": [int(row["id"]) for row in rows],
            "v": [int(row["v"]) for row in rows],
            "delivery_id": [delivery_id] * len(rows),
        }, schema=pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("v", pa.int64(), nullable=False),
            pa.field("delivery_id", pa.string(), nullable=False),
        ]))
        table = self._table()
        writer = table.new_batch_write_builder().new_write()
        writer.write_arrow(frame)
        table.new_batch_write_builder().new_commit().commit(writer.prepare_commit())
        return True

    def count(self):
        return int(self._arrow().num_rows)

    def snapshots(self):
        latest = self._table().snapshot_manager().get_latest_snapshot()
        if latest is None:
            return 0
        return int(latest.id)

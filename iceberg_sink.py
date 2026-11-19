#!/usr/bin/env python3
"""Append-only writes to an Iceberg table in a SQL catalog.

Each delivery id is stored in the table. A second write with the same id
does not append. The commit is one Iceberg snapshot, so a crash before the
commit leaves the previous snapshot in place.
"""
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


class IcebergSink:
    name = "iceberg"
    direction = "sink"
    semantic = "effectively_once"

    def __init__(self, catalog_uri="sqlite:////tmp/m2s-iceberg/catalog.db",
                 warehouse="s3://m2s-iceberg/warehouse",
                 endpoint="http://127.0.0.1:9100",
                 access_key="m2sadmin", secret_key="m2s-local-only",
                 namespace="m2s", table="rows"):
        self.catalog_uri = str(catalog_uri)
        self.warehouse = str(warehouse)
        self.endpoint = str(endpoint)
        self.access_key = str(access_key)
        self.secret_key = str(secret_key)
        self.namespace = str(namespace)
        self.table = str(table)

    def _catalog(self):
        return SqlCatalog(
            "m2s",
            **{
                "uri": self.catalog_uri,
                "warehouse": self.warehouse,
                "s3.endpoint": self.endpoint,
                "s3.access-key-id": self.access_key,
                "s3.secret-access-key": self.secret_key,
                "s3.region": "us-east-1",
                "s3.force-virtual-addressing": "false",
            })

    def _identifier(self):
        return self.namespace + "." + self.table

    def _schema(self):
        return Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="v", field_type=LongType(), required=True),
            NestedField(
                field_id=3, name="delivery_id", field_type=StringType(), required=True),
        )

    def ensure(self):
        catalog = self._catalog()
        try:
            catalog.create_namespace(self.namespace)
        except NamespaceAlreadyExistsError:
            pass
        try:
            catalog.drop_table(self._identifier())
        except NoSuchTableError:
            pass
        catalog.create_table(self._identifier(), schema=self._schema())

    def _table(self):
        return self._catalog().load_table(self._identifier())

    def _seen(self, delivery_id):
        column = self._table().scan().to_arrow().column("delivery_id").to_pylist()
        return str(delivery_id) in column

    def write(self, delivery_id, rows):
        delivery_id = str(delivery_id)
        if self._seen(delivery_id):
            return False
        if not rows:
            return True
        schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("v", pa.int64(), nullable=False),
            pa.field("delivery_id", pa.string(), nullable=False),
        ])
        frame = pa.table({
            "id": [int(row["id"]) for row in rows],
            "v": [int(row["v"]) for row in rows],
            "delivery_id": [delivery_id] * len(rows),
        }, schema=schema)
        self._table().append(frame)
        return True

    def count(self):
        return int(self._table().scan().to_arrow().num_rows)

    def snapshots(self):
        return len(self._table().snapshots())

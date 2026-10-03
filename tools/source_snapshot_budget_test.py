#!/usr/bin/env python3
"""Real fixed-W cursor coverage across byte-limited source snapshot pages."""
from pathlib import Path
import sys
import tempfile
import unittest

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import j4
import source_state


class SnapshotBudgetTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.con = j4.init_state(str(Path(self.temporary.name)/"state.sqlite3"))
        self.addCleanup(lambda: self.con.close())
        schema = pa.schema([pa.field("id",pa.int64()),pa.field("value",pa.string())])
        source_state.register_relation(self.con,"db.events","epoch",schema,["id"])
        table = pa.table({"id":[1,2,3],"value":["x"*100]*3},schema=schema)
        source_state.stage_snapshot_batch(self.con,"db.events",table,cursor=(3,),is_last=True)
        self.pin = source_state.acquire_pin(self.con,"budget-test",["db.events"])
        self.row_bytes = self.con.execute("""
            SELECT length(pk)+length(row_payload) FROM source_versions
            WHERE table_name='db.events' ORDER BY pk LIMIT 1
        """).fetchone()[0]

    def read(self, **kwargs):
        return source_state.read_snapshot_batch(self.con,self.pin["pin_id"],"db.events",**kwargs)

    def test_budget_limited_page_is_not_eof_and_cursor_has_full_coverage(self):
        table, cursor, info = self.read(limit=10,max_bytes=self.row_bytes*2,return_info=True)
        self.assertEqual(table.column("id").to_pylist(),[1,2])
        self.assertTrue(info["budget_limited"])
        self.assertLessEqual(info["payload_bytes"],self.row_bytes*2)
        # Reopen the real durable state/pin as a resumed worker would.
        path = self.con.execute("PRAGMA database_list").fetchone()[2]
        self.con.close()
        self.con = j4.open_state(path)
        rest, end, info = self.read(after_key=cursor,limit=10,max_bytes=self.row_bytes*2,return_info=True)
        self.assertEqual(rest.column("id").to_pylist(),[3])
        self.assertFalse(info["budget_limited"])
        empty, _, info = self.read(after_key=end,limit=10,max_bytes=self.row_bytes*2,return_info=True)
        self.assertEqual(empty.num_rows,0)
        self.assertFalse(info["budget_limited"])

    def test_singleton_over_budget_keeps_pin_and_rejects_without_progress(self):
        with self.assertRaises(RuntimeError):
            self.read(limit=10,max_bytes=self.row_bytes-1,return_info=True)
        self.assertEqual(source_state.pin_watermark(self.con,self.pin["pin_id"]),self.pin["watermark"])
        table, _ = self.read(limit=10)
        self.assertEqual(table.column("id").to_pylist(),[1,2,3])
        with self.assertRaises(ValueError):
            self.read(max_bytes=0)

    def test_row_limit_and_legacy_return_contract(self):
        table, cursor = self.read(limit=2)
        self.assertEqual(table.num_rows,2)
        table, _, info = self.read(after_key=cursor,limit=2,max_bytes=self.row_bytes*10,return_info=True)
        self.assertEqual(table.num_rows,1)
        self.assertFalse(info["budget_limited"])


if __name__ == "__main__":
    unittest.main()

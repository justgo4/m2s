#!/usr/bin/env python3
"""Merge Commit reconciliation: visible advance, aborted replay, else isolate."""
from pathlib import Path
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import j4


def runtime():
    return dict(
        stop=threading.Event(),
        control_lock=threading.Lock(),
        quarantined_tables={})


def seed(con, delivery, label, payload, table="starrocks.events"):
    con.execute(
        "INSERT INTO deliveries(id,table_name,lane) VALUES(?,?,0)",
        (delivery, table))
    con.execute(
        "INSERT INTO load_parts(delivery_id,part,label,payload,nrows) "
        "VALUES(?,?,?,?,1)",
        (delivery, 0, label, payload))
    j4.begin_merge_request(
        con,
        dict(src_table="events", sr_table="events", _catalog_sink=table),
        delivery, 0, label, payload)


def uncertain_label(con, delivery):
    return con.execute(
        "SELECT label,txn_id,visible FROM load_parts WHERE delivery_id=?",
        (delivery,)).fetchone(), con.execute(
        "SELECT COUNT(*) FROM merge_uncertain WHERE delivery_id=?",
        (delivery,)).fetchone()[0]


def test_journal_adopts_server_txn():
    with tempfile.TemporaryDirectory(prefix="m2s-merge-journal-") as directory:
        path = str(Path(directory) / "state.sqlite3")
        cfg = dict(state=path)
        con = j4.init_state(path)
        payload = b'{"id":1,"__op":0}\n'
        seed(con, "d1", "local-1", payload)
        j4.append_merge_accept_journal(
            cfg, "d1", 0, 77, "merge_commit_server", payload)
        assert j4.apply_merge_accept_journal(con, cfg, threading.Event()) == 1
        part, uncertain = uncertain_label(con, "d1")
        assert part == ("local-1", 77, 0), part
        assert uncertain == 0
        assert j4.quarantine_pending_merges(con, runtime()) == 0
        con.close()


def test_visible_match_advances_without_replay():
    with tempfile.TemporaryDirectory(prefix="m2s-merge-visible-") as directory:
        path = str(Path(directory) / "state.sqlite3")
        con = j4.init_state(path)
        payload = b'{"id":1,"__op":0}\n'
        seed(con, "d1", "local-1", payload)
        created = time.time()

        def loads(_cfg, _table):
            return [dict(
                label="merge_commit_server", txn_id=90,
                outcome="visible", created=created)]

        def confirm(txn):
            assert txn == 90
            return "visible", dict(TxnId=txn)

        j4.reconcile_merge_uncertain(
            con, dict(state=path), runtime(), loads=loads, confirm=confirm)
        part, uncertain = uncertain_label(con, "d1")
        assert part[0] == "local-1" and part[1] == 90 and part[2] == 1
        assert uncertain == 0
        assert j4.quarantine_pending_merges(con, runtime()) == 0
        con.close()


def test_aborted_txn_replays_delete_once():
    with tempfile.TemporaryDirectory(prefix="m2s-merge-abort-") as directory:
        path = str(Path(directory) / "state.sqlite3")
        con = j4.init_state(path)
        payload = b'{"id":1,"__op":1}\n'
        seed(con, "d1", "local-1", payload)
        created = time.time()

        def loads(_cfg, _table):
            return [dict(
                label="merge_commit_dead", txn_id=91,
                outcome="aborted", created=created)]

        j4.reconcile_merge_uncertain(
            con, dict(state=path), runtime(), loads=loads,
            confirm=lambda txn: ("aborted", dict(TxnId=txn)))
        part, uncertain = uncertain_label(con, "d1")
        assert uncertain == 0
        assert part[1] is None and part[2] == 0
        assert part[0] != "local-1" and part[0].startswith("local-1_r_")
        assert con.execute(
            "SELECT outcome FROM merge_fence WHERE txn_id=91").fetchone() == ("aborted",)
        label = part[0]
        j4.reconcile_merge_uncertain(
            con, dict(state=path), runtime(), loads=loads,
            confirm=lambda txn: ("aborted", dict(TxnId=txn)))
        part, uncertain = uncertain_label(con, "d1")
        assert part[0] == label and uncertain == 0
        con.close()


def test_unknown_delete_stays_quarantined():
    with tempfile.TemporaryDirectory(prefix="m2s-merge-unknown-") as directory:
        path = str(Path(directory) / "state.sqlite3")
        con = j4.init_state(path)
        payload = b'{"id":1,"__op":1}\n'
        seed(con, "d1", "local-1", payload)

        def loads(_cfg, _table):
            return []

        def confirm(_txn):
            raise AssertionError("unconfirmed delete must not be checked as a chosen txn")

        j4.reconcile_merge_uncertain(
            con, dict(state=path), runtime(), loads=loads, confirm=confirm)
        part, uncertain = uncertain_label(con, "d1")
        assert part == ("local-1", None, 0)
        assert uncertain == 1
        restored = runtime()
        assert j4.quarantine_pending_merges(con, restored) == 1
        assert j4.merge_table_quarantined(restored, "starrocks.events")
        con.close()


def test_ambiguous_or_pending_stays_closed():
    with tempfile.TemporaryDirectory(prefix="m2s-merge-ambiguous-") as directory:
        path = str(Path(directory) / "state.sqlite3")
        con = j4.init_state(path)
        payload = b'{"id":1,"__op":0}\n'
        seed(con, "d1", "local-1", payload)
        created = time.time()

        def loads(_cfg, _table):
            return [
                dict(label="a", txn_id=1, outcome="visible", created=created),
                dict(label="b", txn_id=2, outcome="pending", created=created),
            ]

        j4.reconcile_merge_uncertain(
            con, dict(state=path), runtime(), loads=loads,
            confirm=lambda txn: (_ for _ in ()).throw(AssertionError(txn)))
        part, uncertain = uncertain_label(con, "d1")
        assert part[0] == "local-1" and uncertain == 1
        con.close()


def main():
    test_journal_adopts_server_txn()
    test_visible_match_advances_without_replay()
    test_aborted_txn_replays_delete_once()
    test_unknown_delete_stays_quarantined()
    test_ambiguous_or_pending_stays_closed()
    print("merge_reconcile_test ok", flush=True)


if __name__ == "__main__":
    main()

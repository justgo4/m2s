#!/usr/bin/env python3
"""MySQL and PostgreSQL column add, type change, and drop, plus the MV image."""
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pg_cdc
import pymysql

import stream_connector


MV = [{"id": 3, "v": 1}]


def _expect(change, rows, propagate):
    decision, image = stream_connector.apply_mv(change, rows)
    if decision["drop_rows"]:
        raise AssertionError(decision)
    if decision["retained_rows"] != len(rows):
        raise AssertionError(decision)
    if propagate:
        if decision["mv"] != "propagate" or not decision["propagate"]:
            raise AssertionError(decision)
    else:
        if decision["mv"] != "rebuild" or decision["propagate"] or not decision["rebuild"]:
            raise AssertionError(decision)
    if len(image) != len(rows):
        raise AssertionError("mv row was dropped")
    return decision, image


def _mysql():
    user = os.environ.get("M2S_TEST_MYSQL_USER", "")
    password = os.environ.get("M2S_TEST_MYSQL_PASSWORD", "")
    if not user:
        raise SystemExit("M2S_TEST_MYSQL_USER is required")
    conn = pymysql.connect(
        host=os.environ.get("M2S_TEST_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("M2S_TEST_MYSQL_PORT", "3306")),
        user=user, password=password, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS m2s_ddl")
            cur.execute("USE m2s_ddl")
            cur.execute("DROP TABLE IF EXISTS evolve")
            cur.execute("CREATE TABLE evolve (id INT PRIMARY KEY, v INT)")
            cur.execute("INSERT INTO evolve (id, v) VALUES (3, 1)")
            cur.execute("ALTER TABLE evolve ADD COLUMN note VARCHAR(32) NULL")
            cur.execute(
                "SELECT IS_NULLABLE FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA='m2s_ddl' AND TABLE_NAME='evolve' AND COLUMN_NAME='note'")
            if cur.fetchone()[0] != "YES":
                raise AssertionError("mysql add was not nullable")
            _decision, image = _expect(
                dict(action="add", nullable=True, column="note"), MV, True)
            if image[0].get("note") is not None:
                raise AssertionError(image)
            cur.execute("ALTER TABLE evolve MODIFY v BIGINT")
            cur.execute(
                "SELECT DATA_TYPE FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA='m2s_ddl' AND TABLE_NAME='evolve' AND COLUMN_NAME='v'")
            if cur.fetchone()[0] != "bigint":
                raise AssertionError("mysql widen did not land")
            _expect(dict(action="widen", column="v", from_type="int", to_type="bigint"), image, True)
            cur.execute("ALTER TABLE evolve MODIFY note VARCHAR(8)")
            cur.execute(
                "SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA='m2s_ddl' AND TABLE_NAME='evolve' AND COLUMN_NAME='note'")
            if int(cur.fetchone()[0]) != 8:
                raise AssertionError("mysql narrow did not land")
            _expect(dict(action="narrow", column="note", from_type="varchar(32)", to_type="varchar(8)"), image, False)
            cur.execute("ALTER TABLE evolve DROP COLUMN note")
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA='m2s_ddl' AND TABLE_NAME='evolve' AND COLUMN_NAME='note'")
            if cur.fetchone()[0] != 0:
                raise AssertionError("mysql drop left the column")
            _expect(dict(action="drop", column="note"), image, False)
            cur.execute("SELECT COUNT(*) FROM evolve")
            if cur.fetchone()[0] != 1:
                raise AssertionError("mysql ddl removed a row")
            cur.execute("DROP TABLE evolve")
    finally:
        conn.close()


def _postgres():
    if not os.environ.get("PGPASSWORD"):
        raise SystemExit("PGPASSWORD is required")
    conn = pg_cdc.connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS ddl_evolve")
            cur.execute("CREATE TABLE ddl_evolve (id integer PRIMARY KEY, v integer)")
            cur.execute("INSERT INTO ddl_evolve (id, v) VALUES (3, 1)")
            cur.execute("ALTER TABLE ddl_evolve ADD COLUMN note varchar(32)")
            cur.execute(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_name='ddl_evolve' AND column_name='note'")
            if cur.fetchone()[0] != "YES":
                raise AssertionError("postgres add was not nullable")
            _decision, image = _expect(
                dict(action="add", nullable=True, column="note"), MV, True)
            cur.execute("ALTER TABLE ddl_evolve ALTER COLUMN v TYPE bigint")
            cur.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_name='ddl_evolve' AND column_name='v'")
            if cur.fetchone()[0] != "bigint":
                raise AssertionError("postgres widen did not land")
            _expect(dict(action="widen", column="v", from_type="integer", to_type="bigint"), image, True)
            cur.execute("ALTER TABLE ddl_evolve ALTER COLUMN note TYPE varchar(8)")
            cur.execute(
                "SELECT character_maximum_length FROM information_schema.columns "
                "WHERE table_name='ddl_evolve' AND column_name='note'")
            if int(cur.fetchone()[0]) != 8:
                raise AssertionError("postgres narrow did not land")
            _expect(
                dict(action="narrow", column="note", from_type="varchar(32)", to_type="varchar(8)"),
                image, False)
            cur.execute("ALTER TABLE ddl_evolve DROP COLUMN note")
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.columns "
                "WHERE table_name='ddl_evolve' AND column_name='note'")
            if cur.fetchone()[0] != 0:
                raise AssertionError("postgres drop left the column")
            _expect(dict(action="drop", column="note"), image, False)
            cur.execute("SELECT COUNT(*) FROM ddl_evolve")
            if cur.fetchone()[0] != 1:
                raise AssertionError("postgres ddl removed a row")
            cur.execute("DROP TABLE ddl_evolve")
    finally:
        conn.close()


def main():
    _mysql()
    _postgres()
    print(
        "ddl_mv mysql=add,widen,narrow,drop postgres=add,widen,narrow,drop "
        "mv_add=propagate mv_widen=propagate mv_narrow=rebuild mv_drop=rebuild "
        "retained_rows=1 drop_rows=0",
        flush=True)
    print("ddl_mv_test ok", flush=True)


if __name__ == "__main__":
    main()

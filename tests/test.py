#!/usr/local/bin/python3

import db
from db import db_session_fxt, cursor_fxt
import logging 

log = logging.getLogger(__name__)

def test_connect_mariadb(conn_params):
    mdb = db.MariaDB(conn_params = conn_params)

    mdb.connect()

    cursor = mdb.cursor()

    cursor.execute('show databases;')

    out = cursor.fetchall()

    log.info(out)


def test_create_db_session(conn_params):
    with db.DbSession(conn_params, isolate_db=True) as session:

        cursor = session.cursor()

        cursor.execute("CREATE TABLE t1 (col1 int);")

        cursor.execute("INSERT INTO t1 values (1);")

def test_check_session_fxt(db_session_fxt):

    with db_session_fxt.cursor() as cursor:

        cursor.execute("CREATE TABLE t1 (col1 int);")

        cursor.execute("INSERT INTO t1 values (1);")


def test_check_cursor_fxt(cursor_fxt):

    cursor_fxt.execute("CREATE TABLE t1 (col1 int);")

    cursor_fxt.execute("INSERT INTO t1 values (1);")

    cursor_fxt.execute("show tables")

    log.info("Cursor Result is {}".format(cursor_fxt.fetchall()))


#!/usr/local/bin/python3

from libs.db import db_session_fxt, cursor_fxt, MariaDB, DbSession
import logging 


def test_connect_mariadb(conn_params):
    mdb = MariaDB(conn_params = conn_params)
    mdb.connect()
    cursor = mdb.cursor()
    cursor.execute('show databases;')
    out = cursor.fetchall()
    logging.info(out)

def test_create_db_session(conn_params):
    with DbSession(conn_params, isolate_db=True) as session:
        cursor = session.cursor()
        cursor.execute("CREATE TABLE t1 (col1 int);")
        cursor.execute("INSERT INTO t1 values (1);")

def test_check_session_fxt(db_session_fxt):
    with db_session_fxt.cursor() as cursor:
        cursor.execute("CREATE TABLE t1 (col1 int);")
        cursor.execute("INSERT INTO t1 values (1);")
    logging.info("Done inserting tables")

def test_check_cursor_fxt(cursor_fxt):
    cursor_fxt.execute("CREATE TABLE t1 (col1 int);")
    cursor_fxt.execute("INSERT INTO t1 values (1);")
    cursor_fxt.execute("show tables")
    logging.info("Cursor Result is {}".format(cursor_fxt.fetchall()))

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

        cursor.execute("select * from t1;")

        var = cursor.fetchall()

        log.info(" Output is {}".format(var))


def test_check_cursor_fxt(cursor_fxt):

    cursor_fxt.execute("CREATE TABLE t1 (col1 int);")

    cursor_fxt.execute("INSERT INTO t1 values (1);")

    cursor_fxt.execute("show tables")


    log.info("Cursor Result is {}".format(cursor_fxt.fetchall()))


def test_check_query_output(db_session_fxt):

    with db_session_fxt.cursor() as cursor_fxt:

        cursor_fxt.execute("DROP TABLE IF EXISTS num_exp_add;")
        cursor_fxt.execute("DROP TABLE IF EXISTS num_exp_add2;")

        cursor_fxt.execute("CREATE TABLE num_exp_add (id1 int4, id2 int4, expected numeric(65,10));")
        cursor_fxt.execute("CREATE TABLE num_exp_add2 (id1 int4, id2 int4, expected numeric(65,10));")

        cursor_fxt.execute("INSERT INTO num_exp_add VALUES (0,0,'0');")
        cursor_fxt.execute("INSERT INTO num_exp_add VALUES (0,1,'0');")
        cursor_fxt.execute("INSERT INTO num_exp_add VALUES (1,3,'4.31');")
        cursor_fxt.execute("INSERT INTO num_exp_add VALUES (1,1,'0');")
        cursor_fxt.execute("INSERT INTO num_exp_add VALUES (1,0,'0');")
        #cursor_fxt.execute("INSERT INTO num_exp_add VALUES (1,0,'0');")


        cursor_fxt.execute("INSERT INTO num_exp_add2 VALUES (0,0,'0');")
        cursor_fxt.execute("INSERT INTO num_exp_add2 VALUES (0,1,'0');")
        cursor_fxt.execute("INSERT INTO num_exp_add2 VALUES (1,3,'4.31');")
        cursor_fxt.execute("INSERT INTO num_exp_add2 VALUES (1,1,'0');")
        cursor_fxt.execute("INSERT INTO num_exp_add2 VALUES (1,0,'0');")

        cursor_fxt.execute("select * from num_exp_add;")
        sql1=cursor_fxt.fetchall()
        cursor_fxt.execute("select * from num_exp_add2;")
        sql2=cursor_fxt.fetchall()

        log.info(" Lets check if the queries are equal")
        compare_sql_queries(sql1,sql2)


def test_query_multiple_db(db_session_fxt):
    with db_session_fxt.cursor() as cursor_fxt:

        cursor_fxt.execute("DROP TABLE IF EXISTS num_exp_add;")
        cursor_fxt.execute("CREATE TABLE num_exp_add (id1 int4, id2 int4, expected numeric(65,10));")
        cursor_fxt.execute("INSERT INTO num_exp_add VALUES (0,0,'0');")
        cursor_fxt.execute("INSERT INTO num_exp_add VALUES (0,1,'0');")
        cursor_fxt.execute("INSERT INTO num_exp_add VALUES (1,3,'4.31');")
        query_to_check= "select * from num_exp_add"
        switch_db_and_compare_query(cursor_fxt,query_to_check)



def compare_sql_queries(output1,output2):

    """ This method will compare 2 sql queries and verify if the contents are right
        First we will verify the count , if it is not equal we fail right away .
        Else we convert each into a set and check of they are do set operation
    """


    assert (len(output1) == len(output2))
    set1=set(output1)
    set2=set(output2)
    assert (set1 -set2 == set())

def switch_db_and_compare_query(cursor, sql1):
    """ This method take one query or a list of queries and
    run it against innodb and then against monetdb and the
    compare the results amd make sure they are correct
    The variable which will be used for this
    set mapi_monetdb_query_routing=AUTO;    # default; query_router will make decision
    set mapi_monetdb_query_routing=OFF;     # queries will be routed to MariaDB/InnoDB
    set mapi_monetdb_query_routing=ALWAYS;  # queries will be routed to MonetDB
    """

    if type(sql1) is not list :
        sql1 = [sql1]

    for each_query in sql1:
        cursor.execute("set mapi_monetdb_query_routing=OFF")
        cursor.execute(each_query)
        inno_output = cursor.fetchall()
        log.info("Inno Output is {}".format(inno_output))
        cursor.execute("set mapi_monetdb_query_routing=ALWAYS")
        cursor.execute(each_query)
        monet_output = cursor.fetchall()
        log.info("Monet Output is {}".format(monet_output))
        compare_sql_queries(inno_output, monet_output)
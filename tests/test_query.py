#!/usr/local/bin/python3

import pytest
from db import ExecuteQueries
import logging

log = logging.getLogger(__name__)

CREATE_TABLE = ["CREATE TABLE t1 (col1 int)"]
INSERT_LIST = [
    "INSERT INTO t1 values ({})".format(x)
    for x in range(100000)
]
SELECT_TABLE = ["SELECT * FROM t1"]
DELETE_TABLE = ["DROP TABLE t1"]


@pytest.fixture(scope="function")
def exec_query(conn_params):
    eq = ExecuteQueries(conn_params)
    eq.execute_queries(CREATE_TABLE)
    yield eq
    #out = eq.execute_queries(SELECT_TABLE, fetch_result=True)
    eq.execute_queries(DELETE_TABLE)


def test_execute_query(exec_query):
    exec_query.execute_queries(INSERT_LIST)

@pytest.mark.parametrize("procs", [16, 32, 48])
def test_execute_query_parallel(exec_query, procs):
    exec_query.execute_parallel_queries(INSERT_LIST, procs)



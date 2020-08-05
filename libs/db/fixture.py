#!/usr/local/bin/python3
import pytest
from .session import DbSession


@pytest.fixture(scope="function")
def db_session_fxt(conn_params):
    with DbSession(conn_params, isolate_db=True) as session:
        yield session


@pytest.fixture(scope="function")
def cursor_fxt(db_session_fxt):
    with db_session_fxt.cursor() as cursor:
        yield cursor

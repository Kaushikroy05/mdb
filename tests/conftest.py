#!/usr/local/bin/python3
import pytest
import logging
import time
import os
import sys
from datetime import datetime

lib_dir = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '../libs'
    )
)

sys.path.insert(0, lib_dir)

import infra
from connection import Connection

LOGLEVELS = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}


def pytest_addoption(parser):
    parser.addoption(
        "--host",
        action="store",
        default='localhost',
        help="mariadb hostname or IP"
    )

    parser.addoption(
        "--port",
        action="store",
        default=3306,
        help="db port"
    )

    parser.addoption(
        "--user",
        action="store",
        default=None,
        help="mariadb username"
    )

    parser.addoption(
        "--password",
        action="store",
        default=None,
        help="mariadb password"
    )

    parser.addoption(
        "--dbname",
        action="store",
        default=None,
        help="dbname"
    )


@pytest.fixture(scope="session", autouse=True)
def session_setup_teardown(request):

    infra.print_banner("Enter session setup")

    infra.print_banner("Exit session setup")

    yield

    infra.print_banner("Enter session teardown")

    infra.print_banner("Exit session teardown")


@pytest.fixture(scope="function", autouse=True)
def testcase_setup_teardown(request):
    infra.print_banner(
        "Enter testcase ({}) setup".format(request.node.name)
    )

    infra.print_banner(
        "Exit testcase ({}) setup".format(request.node.name)
    )

    yield

    infra.print_banner(
        "Enter testcase ({}) teardown".format(request.node.name)
    )

    infra.print_banner(
        "Exit testcase ({}) teardown".format(request.node.name)
    )

@pytest.fixture(scope="session")
def conn_params(request):
    dboptions = dict()
    dboptions['host'] = request.config.getoption("--host")
    dboptions['user'] = request.config.getoption("--user")
    dboptions['password'] = request.config.getoption("--password")
    dboptions['dbname'] = request.config.getoption("--dbname")
    dboptions['port'] = request.config.getoption("--port")

    yield dboptions

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

from infra import print_banner

log = logging.getLogger(__name__)

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
        default="usertest",
        help="mariadb username"
    )

    parser.addoption(
        "--password",
        action="store",
        default="TEST123",
        help="mariadb password"
    )

    parser.addoption(
        "--dbname",
        action="store",
        default="testDB",
        help="dbname"
    )


@pytest.fixture(scope="session", autouse=True)
def session_setup_teardown(request):

    print_banner("Enter session setup")

    print_banner("Exit session setup")

    yield

    print_banner("Enter session teardown")

    print_banner("Exit session teardown")


@pytest.fixture(scope="function", autouse=True)
def testcase_setup_teardown(request):
    print_banner(
        "Enter testcase ({}) setup".format(request.node.name)
    )

    print_banner(
        "Exit testcase ({}) setup".format(request.node.name)
    )

    start_time_ms = int(time.time() * 1000)

    yield

    end_time_ms = int(time.time() * 1000)

    log.info("Test execution time: {} ms".format(end_time_ms - start_time_ms))


    print_banner(
        "Enter testcase ({}) teardown".format(request.node.name)
    )

    print_banner(
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

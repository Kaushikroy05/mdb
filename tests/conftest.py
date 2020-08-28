#!/usr/local/bin/python3
import pytest
import time
import os
import sys
import logging
from datetime import datetime

def _create_log_path(filename: str, location: str) -> str:
    return os.path.join(f"{location}", f"pytest_{time.strftime('%Y%m%d%H%M%S')}", f"{filename}.log")

lib_dir = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '../'
    )
)

print(lib_dir)
sys.path.insert(0, lib_dir)

from libs.common.log import LogSetup
from libs.infra import print_banner


LOG_LEVEL = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
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

    parser.addoption(
        "--log_level",
        type=str,
        default="info",
        help="Log level for pytest. Options: info(default), debug, warning, error or critical"
    )

    parser.addoption(
        "--logdir",
        type=str,
        default=None,
        help="Log directory, if not given pytest will not generate logs"
    )

@pytest.fixture(scope="session")
def logdir(request):
    return request.config.getoption("--logdir").strip()

@pytest.fixture(scope="session")
def log_level(request):
    return request.config.getoption("--log_level").strip().lower()

@pytest.fixture(scope="function", autouse=True)
def logfile_setup(request, logdir, log_level):
    """pytest function must be wrapped in class
    """
    if request.cls or not request.config.getoption("logdir"):
        yield
        return
    assert request.function, f"{request.function}"
    current_log = LogSetup()

    current_log.logfile(_create_log_path(os.path.join(request.module.__name__.split('.')[-1], request.function.__name__), logdir), LOG_LEVEL[log_level])

    yield
    current_log.reset_logger()

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

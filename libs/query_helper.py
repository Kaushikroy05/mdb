#!/usr/local/bin/python3
# encoding=utf8

import collections
import functools
import logging
import math
import multiprocessing
import operator
import psycopg2
import random
import sqlparse
import time
from paramiko import SSHException
from raff.common import retry
from datetime import datetime
from prettytable import PrettyTable
from multiprocessing import Manager

class TableInfo(object):
    pass

class CorrectnessTestExecutor(object):
    pass

def execute_queries():
    pass

def execute_queries_parallel(db_instanceA, db_instanceB, randomize,
                             continue_on_fail, select_only,list_queries):
    pass

def run_queries(list_queries,db_instanceA, db_instanceB, random_execution, proc_sum, run_report):
    pass

def _execute_worker():
    pass
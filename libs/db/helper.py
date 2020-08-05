#!/usr/local/bin/python3
import uuid

MAX_IDENTIFIER_LEN = 20  # Consider using the full length of 127
MAX_USERNAME_LEN = 20  # Consider using the full length of 64

DEFAULT_CONNECT_TIMEOUT_SEC = 5

# Default settings for Maria DB connections.
DEFAULT_DB_SETTINGS = {
    'autocommit': True,
    'connect_timeout': DEFAULT_CONNECT_TIMEOUT_SEC,
}


def get_random_identifier(max_len=MAX_IDENTIFIER_LEN, prefix_str=''):
    """
    Helper function to generate a random string of a given max length. It also
    supports having that generated string be prefixed with a fixed substring.

    Args:
        max_len (int): Maximum length for the generated string
        prefix_str (str): Optional fixed prefix string

    Returns:
        rand_ident (str): Randomly generated identifier with optional prefix

    Raises:
        ValueError if prefix_str is greater than max_len
    """

    if len(prefix_str) > max_len:
        raise ValueError("The prefix_str must not be larger than max_len.")

    content_len = max_len - len(prefix_str)
    suffix = uuid.uuid4().hex[0:content_len]
    return '{}{}'.format(prefix_str, suffix)


def validate_conn_params(conn_params):
    """
    Helper to validate conn_params dictionary. These fields must match:
    http://initd.org/psycopg/docs/module.html

    conn_params = {
        'host': 'localhost',
        'dbname': 'dev',
        'port': 5439,
        'user': 'master',
        'password': 'Testing1234'
    }

    Args:
        conn_params (dict): Connection parameters dictionary

    Raises:
        ValueError for invalid connection parameters
    """

    if not isinstance(conn_params, dict):
        raise ValueError("Connection parameters must be a dictionary")

    required_keys = {'host', 'dbname', 'port', 'user', 'password'}
    conn_param_keys = conn_params.keys()
    if not required_keys.issubset(set(conn_param_keys)):
        missing_params = list(set(required_keys) - set(conn_param_keys))
        raise ValueError(
            "Required field(s) missing: {}".format(str(missing_params)))



class cached_property(object):
    """
    A property that is only computed once per instance and then replaces itself
    with an ordinary attribute. Deleting the attribute resets the property.

    The cached_property definition is cribbed from:
    https://github.com/pydanny/cached-property/blob/master/cached_property.py
    """

    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


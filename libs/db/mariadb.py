#!/usr/local/bin/python3
import logging
import mysql.connector

from .helper import DEFAULT_DB_SETTINGS
from copy import copy

log = logging.getLogger(__name__)


class MariaDB(object):
    """
    MariaDB class
    """

    def __init__(self, conn_params, settings=None):
        """
        Args:
            conn_params (dict): database connection parameters
            settings (dict): connection settings, like cursor factory,
                            autocommit
        """
        self.conn_params = conn_params
        self.settings = settings or DEFAULT_DB_SETTINGS.copy()
        self._autocommit = self.settings.get('autocommit', False)

    def connect(self, debug=False):
        """
        Establishes connection to the database.

        Returns:
            connection: New connection object
        """

        log.debug(
            "Establishing connection to {} database".format(self.dbname)
        )

        self.connection = mysql.connector.connect(
            host = self.conn_params['host'],
            user = self.conn_params['user'],
            password = self.conn_params['password'],
            port = self.conn_params['port'],
            database = self.conn_params['dbname'],
        )

        self.connection.autocommit = self.autocommit

    def _ensure_connection(self):
        """
        Helper function to guarantee that a connection to the database is
        established. If the connection was closed on the server side it will
        create a new connection.

        It is known that any connection/session settings will be lost as the
        server side lost the connection.
        """

        try:
            # When a connection is closed, self.connection will be None. In
            # which case try to establish a new connection.
            # If connection is present, confirm it is still active by running
            # a simple query.
            # If the connection not active we try to re-establish the
            # connection.
            if not self.connection:
                try:
                    self.connect()
                except Exception as exp:
                    log.info("[_ensure_connection] {}".format(exp))
                    log.info("[_ensure_connection] Reconnecting "
                             "with debug flag set to True")
                    self.connect(debug=True)
            else:
                with self.connection.cursor() as cursor:
                    cursor.execute('SELECT 1')

        except Exception as exp:
            log.debug("Connection None or Closed, {}".format(exp))
            self.connect()

    def _set_autocommit(self, value):
        """
        This function will set autocommit to True or False

        Args:
            value (boolean): True or False
        """
        self.connection.autocommit = value

    def cursor(self):
        """
        Returns:
            Cursor object
        """
        self._ensure_connection()
        cursor = self.connection.cursor()
        return cursor

    def clone(self):
        """
        Method to return a new database connections with the same
        initialization parameters. This avoids the isolation of tests and can
        be used to perform concurrent queries in the same schema.

        Returns:
          connection (mariadbBase): A connection of the same type.
        """
        return self.__class__(
            conn_params=self.conn_params,
            setting=self.settings
            #**self.driver_property
        )

    @property
    def dbname(self):
        """
        Property representing the name of the database.

        Returns:
            name (str): DB name
        """

        return self.conn_params['dbname']

    @property
    def autocommit(self):
        """
        Property storing the autocommit mode for the connection.

        Returns:
            autocommit (bool): Autocommit on or off
        """

        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        """
        Enable or disable autocommit on a connection.

        Args:
            value (bool): Enable or disable autocommit
        """

        self._ensure_connection()
        log.debug("Setting autocommit to {} for the connection "
                  "to {} database.".format(value, self.dbname))

        self._set_autocommit(value)
        self._autocommit = value


    def commit(self):
        """
        Commit a transaction.
        """

        if self.connection is not None:
            log.debug("Committing transaction.")
            self.connection.commit()

    def rollback(self):
        """
        Roll back a transaction.
        """

        if self.connection is not None:
            log.debug("Rolling back transaction.")
            self.connection.rollback()

    def close(self):
        """
        Closes the connection to the database.
        """

        if self.connection is not None:
            log.debug(
                "Closing connection to {} database".format(self.dbname))
            self.connection.close()
            self.connection = None

    def __enter__(self):
        """
        Enter meta function that allows this object to be used as a context
        manager.

        Returns:
            self (mariadb): Current mariadb instance
        """

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit meta function that allows this object to be used as a context
        manager.

        It invokes the close function to clean up resources.
        """

        self.close()

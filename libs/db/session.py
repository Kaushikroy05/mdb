import logging
import time

from .session_context import SessionContext
from .helper import DEFAULT_DB_SETTINGS
from .helper import get_random_identifier
from .helper import validate_conn_params
from .helper import MAX_IDENTIFIER_LEN
from .mariadb import MariaDB

from libs.cachedproperty import cached_property

from .db_exception import (
    Error,
    InternalError,
    ProgrammingError,
    DatabaseError
)

log = logging.getLogger(__name__)


def _try_execute(sql, cursor, log_str):
    """
    Helper function to execute SQL statements with a given cursor. It
    automatically retries under certain error conditions.

    Args:
        sql (str): SQL statement
        cursor (mariadb Cursor): Cursor instance
        log_str (str): Debug logging statement
    """

    max_attempts = 3
    delay_sec = 5

    for count in range(max_attempts, -1, -1):
        try:
            log.debug(log_str)
            cursor.execute(sql)
            break
        except (InternalError, ProgrammingError, DatabaseError) as e:
            warning_msgs = [
                "Failed to execute: {}.".format(sql),
                "Encountered: {}.".format(e.message.strip()),
                "Retry attempts remaining: {}".format(count),
                "Sleeping for {} seconds.".format(delay_sec)
            ]
            log.debug('\n'.join(warning_msgs))
            time.sleep(delay_sec)


class DbSession(object):
    """
    Class representing a MariaDB Db session. It has support for isolation of
    resources via automatically creating and destroying a session user
    and database.

    A session is a database connection with an automatically generated
    user and database. This is used to isolate queries from each other as much
    as possible. Queries executed on a session are limited to the generated
    unique database unless an explicit alternate database is referenced. However,
    sessions do not protect against operations that alter the database as a
    whole.

    User and database resources are represented via a session context which
    allows their properties to be modified. For example, the default behavior
    would be to create a regular user with a randomly generated name. However,
    this can be overridden to supply an explicit name and user type.

    By default a session db connection will have autocommit enabled for
    transactions.

    The autocommit and connection timeout setting can be controlled by
    altering the conn_settings dictionary as seen below.

        conn_settings = {
            'autocommit': False,
            'connect_timeout': 20,
        }

    This class has also implemented the __enter__ and __exit__ meta functions
    which allows it to be used as a context manager via the with keyword.
    Outside of the with block it will teardown the session resources and close
    the database connections.

    Automatic creation and dropping of a randomly named database is by
    default disabled unless isolate_db is turned on.

    Some special caveats:
    * If the database is named public we'll skip dropping.
    * If the db name is the same as what the cluster was created with, for
        example dev then we'll skip dropping it.

    This class is not thread safe. A new instance should be used in a thread
    rather than shared between threads.

    Example usage shown below:

    # Using it as a context manager which implicitly handles the close

        with DbSession(conn_params) as db_session:
            # do stuff

    # Explicitly handling close

        db_session = DbSession(conn_params)
        # do stuff
        db_session.close()

    # Customizing database name

        session_ctx = SessionContext(database='mydb')

        with DbSession(session_ctx=session_ctx) as session:
            cursor = session.cursor()

            cursor.execute('select 1;')
            result = cursor.fetchall()
            assert result == [(1,)]

    # Use a session with a transaction that does not autocommit. By default
    # DbSession has autocommit enabled.

        db_settings = {'autocommit': False}
        with DbSession(conn_params, settings=db_settings) as session:
            cursor = session.cursor()
            cursor.execute("CREATE TABLE t1 (col1 int);")
            cursor.execute("INSERT INTO t1 values (1);")
            session.connection.rollback()  # Or commit()
    """

    DB_PREFIX_NAME = 'db'

    def __init__(
            self,
            base_conn_params,
            conn_settings=None,
            session_ctx=None,
            isolate_db=False,
    ):
        """
        Initialize a new DbSession object with specified connection settings
        and session db resource customizations.

        Args:
            base_conn_params (dict): Base connection parameters for the MariaDB.

            conn_settings (dict): Connection settings such as autocommit for
                transactions.

            session_ctx (SessionContext): Session context object for creating
                a session user and database with custom properties.

            isolate_db (bool): Flag to enable creation of an isolated database

        Raises:
            ValueError if invalid connection parameters are supplied
        """

        validate_conn_params(base_conn_params)
        self._base_conn_params = base_conn_params

        self._base_conn_settings = DEFAULT_DB_SETTINGS.copy()
        self.conn_settings = conn_settings or DEFAULT_DB_SETTINGS.copy()

        if not session_ctx:
            self.session_ctx = SessionContext(
                username=self._base_conn_params['user'],
                password=self._base_conn_params['password'],
                dbname=self._base_conn_params['dbname'],
            )
        else:
            if not isinstance(session_ctx, SessionContext):
                raise ValueError(
                    "{} must be an instance of {}".format(
                        session_ctx, SessionContext))
            self.session_ctx = session_ctx

        self.isolate_db = isolate_db

        self._session_conn_params = {
            'host': self._base_conn_params['host'],
            'dbname': self.dbname,
            'port': self._base_conn_params['port'],
            'user': self.session_ctx.username,
            'password': self.session_ctx.password
        }

        self._session_db = None

        # This variable is used to track if session resources have been
        # created. We repeatedly access the session_db property and we don't
        # want to recreate session resources on every access.
        self._created_resources = False

        # This variable is used to track if the session database has been
        # created/dropped. There is a small timing window right after dropping
        # the database, but before dropping other resources in which it uses the
        # db_session property. This access attempts to leverage the database
        # even though it is already gone. With this variable we can skip that
        # unnecessary usage.
        self._database_exists = False

    @cached_property
    def base_db(self):
        """
        Cached property that establishes and stores a base connection to the
        database. This connection is used as a jumping off point to establish
        a subsequent session connection.

        In order to create the custom session database and user, we need to
        first connect to the database. This can be done via the base
        connection. These base connection parameters are commonly the master
        user.

        Returns:
            MariaDB instance for a base database connection
        """

        return MariaDB(self._base_conn_params, self._base_conn_settings)

    def _establish_session_resources(self):
        """
        Helper function to create the session user and database. As the
        session user requires permission to create database resources such as
        tables/views it also grants that user permission on the session
        database.

        It will skip creating the session user if the base connection was
        already using that user.

        Likewise it will skip creating the session database if the base
        connection was already using that database.
        """

        if not self._created_resources:
            session_username = self.session_ctx.username
            # Create the session user
            if session_username != self._base_conn_params['user']:
                log_str = "Creating user: {}".format(session_username)
                _try_execute(self.session_ctx.create_user_sql,
                             self.base_db.cursor(), log_str)

            # Create the session database
            if (self.isolate_db and
                    (self.dbname != self._base_conn_params['dbname'])):
                log_str = "Creating database: {}".format(self.dbname)
                _try_execute(self.create_db_sql, self.base_db.cursor(),
                             log_str)

            # Grant permission for the session user to use the session database
            log_str = "Granting permission to {} for database {}".format(
                session_username, self.dbname)
            _try_execute(self.grant_user_sql, self.base_db.cursor(),
                         log_str)
            self._created_resources = True

            # Close base_db conn as it is not needed until final cleanup later
            # in which it will auto-open it anyway if closed
            self._close_conn_attempt(self.base_db)

    def _set_session_search_path(self, db):
        """
        Helper function that sets the search_path for the session.

        search_path to be:
            database_name, "$user", public

        With that search_path pre-defined, any user queries leveraging this
        session with by default scope to that database first.

        Args:
            db (mariadb): Database connection instance
        """

        log_str = "Setting search_path to: {}".format(
            self.session_ctx.search_path)
        _try_execute(
            self.session_ctx.set_search_path_sql, db.cursor(), log_str)

    def _drop_session_resources(self):
        """
        Drops any automatically created session resources such as
        user and database.
        """

        # Drop the session database
        database_name = self.session_ctx.dbname

        # Have to disconnect from the session database to later drop it.
        # Purposefully using the private variable since we don't want to route
        # through resource re-creation that the public property does.
        self._close_conn_attempt(self._session_db)

        # Revoke permission for the user
        if self.session_ctx.username != self._base_conn_params['user']:
            log_str = "Revoking permission for user {} to database {}".format(
                self.session_ctx.username, self.dbname)
            _try_execute(
                self.revoke_user_sql, self.base_db.cursor(),
                log_str
            )

        # Drop the session database
        if (self.isolate_db and
                (self.dbname != self._base_conn_params['dbname'])):
            log_str = "Dropping database: {}".format(self.dbname)
            _try_execute(self.drop_db_sql, self.base_db.cursor(),
                         log_str)

        # Drop the session user
        if self.session_ctx.username != self._base_conn_params['user']:
            log_str = "Dropping user: {}".format(self.session_ctx.username)
            _try_execute(self.session_ctx.drop_user_sql,
                         self.base_db.cursor(), log_str)

        self._created_resources = False

    @property
    def session_db(self):
        """
        Property that establishes and stores a session connection to the
        database. This connection is the main session connection for consumers.

        When invoked it will auto-establish any session resources such as
        database, user with the intent being to isolate
        sessions from each other.

        Returns:
            instance for a session database connection
        """

        self._establish_session_resources()

        if not self._session_db:
            self._session_db = MariaDB(
                self._session_conn_params, self.conn_settings,
            )

        return self._session_db

    @property
    def connection(self):
        """
        Database connection for the current db session. When invoked it will
        auto-establish any session resources such as database, user
        with the intent being to isolate sessions from each other.

        Returns:
            connection: Connection object to the db
        """

        return self.session_db.connection

    @property
    def cursor(self):
        """
        Database cursor for the current db session. When invoked it will
        auto-establish any session resources such as database, user
        with the intent being to isolate sessions from each other.

        Returns:
            cursor (mariadb.cursor): Cursor object for the db connection
        """
        return self.session_db.cursor

    def _close_conn_attempt(self, db):
        """
        Attempt to close any outstanding connection.

        Args:
            db (mariadb): Instance of a db connection
        """

        try:
            if db:
                db.close()
        except Exception:
            # Connections will be closed whenever __del__ get's executed so
            # let's just move on
            pass

    def close(self):
        """
        Drop session resources that were previously created and attempt to
        close any outstanding connections.
        """

        try:
            self._drop_session_resources()
        except Error as e:
            log.warning("Failed to drop session resources! "
                        "Encountered: {}".format(e.message))
        finally:
            self._close_conn_attempt(self.base_db)

    def __enter__(self):
        """
        Enter meta function that allows this object to be used as a context
        manager.

        Returns:
            self (DbSession): Current DbSession instance
        """

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit meta function that allows this object to be used as a context
        manager.

        It invokes the close function to clean up resources.
        """

        self.close()

    # Database context information

    @cached_property
    def dbname(self):
        """
        This property represents the database name. It is either an
        explicitly provided name or a randomly generated name.

        Returns:
            dbname (str): Database name
        """

        if self.isolate_db:
            return get_random_identifier(
                max_len=MAX_IDENTIFIER_LEN,
                prefix_str=DbSession.DB_PREFIX_NAME
            )
        else:
            return self._base_conn_params['dbname']

    @cached_property
    def drop_db_sql(self):
        """
        This property stores the SQL statement to drop the database.

        Returns:
            drop_str (str): SQL drop statement
        """

        return 'DROP DATABASE {}'.format(self.dbname)

    @cached_property
    def create_db_sql(self):
        """
        This property stores the SQL statement to create the database.

        Returns:
            create_str (str): SQL create statement
        """

        # TODO: Look into WITH OWNER {} - might be able to skip grant
        return 'CREATE DATABASE {}'.format(self.dbname)

    @cached_property
    def grant_user_sql(self):
        """
        This property stores the SQL statement to grant permission to the
        database.

        Returns:
            grant_str (str): SQL grant statement
        """

        return "GRANT ALL PRIVILEGES ON {}.* TO '{}'@'localhost'".format(
            self.dbname, self.session_ctx.username)
        #return "GRANT ALL PRIVILEGES ON {}.* TO '{}'".format(
        #    self.dbname, self.session_ctx.username)

    @cached_property
    def revoke_user_sql(self):
        """
        This property stores the SQL statement to revoke permission to the
        database.

        Returns:
            grant_str (str): SQL grant statement
        """

        #return "REVOKE ALL PRIVILEGES ON {}.* FROM {}".format(
        #    self.dbname, self.session_ctx.username)
        return "REVOKE ALL PRIVILEGES ON {}.* FROM {}@'localhost'".format(
            self.dbname, self.session_ctx.username)

from libs.cachedproperty import cached_property
from .helper import get_random_identifier
from .helper import MAX_USERNAME_LEN, MAX_IDENTIFIER_LEN
from .db_defaults import DBDefaults


class SessionContext(object):
    """
    Class representing a session context. It stores optional randomly
    generated names and has setup/teardown SQL queries available.

    This class does not actually run the create/drop SQL statements, but
    instead  represents a data structure storing the information on how to
    perform the operation.
    """

    def __init__(
            self,
            username=None,
            password=DBDefaults.PASSWORD,
            dbname=None,
    ):
        """
        Initialize a new SessionContext instance that can auto-generate a
        username and database name. It also pre-computes SQL statements to
        create and drop these resources. Optionally, the caller can provide
        an explicit names rather than using the auto-generated ones.

        Args:
            username (str): Optionally set explicit user name
            password (str): Database user password
            dbname (str): Optionally set explicit database name
        """

        self._username = username
        self.password = password
        self._dbname = dbname

    # User context information

    def _generate_username(self):
        """
        Helper function to generate a random database username with a prefix
        based on the user's type.

        We do not generate a username for the bootstrap user as it is a
        pre-existing user and we cannot create additional ones.

        We do not generate a username for the master user since the master
        user is pre-existing on clusters. Note that a master user is just a
        special case of Super User in which the usesysid is 100.

        Returns:
            username (str): Database user's name
        """

        return get_random_identifier(MAX_USERNAME_LEN, prefix_str='usr')

    @cached_property
    def username(self):
        """
        This property represents the database user's name. It is either an
        explicitly provided name or a randomly generated name based on the
        user type.

        Returns:
            username (str): Database user's name
        """

        return self._username or self._generate_username()

    @cached_property
    def drop_user_sql(self):
        """
        This property stores the SQL statement to drop the database user.

        Returns:
            drop_str (str): SQL drop statement
        """

        #return 'DROP USER {}'.format(self.username)
        return 'DROP USER {}@localhost'.format(self.username)

    @cached_property
    def create_user_sql(self):
        """
        This property stores the SQL statement to create a database user
        of the expected type.

        Returns:
            create_str (str): SQL create statement
        """

        sql = "CREATE USER '{}'@localhost IDENTIFIED BY '{}'"
        #sql = "CREATE USER '{}' IDENTIFIED BY '{}'"

        create_str = sql.format(self.username, self.password)

        return create_str

    # Schema context information

    @cached_property
    def dbname(self):
        """
        This property represents the database name. It is either an explicitly
        provided name or a randomly generated name.

        Returns:
            database (str): database name
        """

        return self._dbname or get_random_identifier(
            max_len=MAX_IDENTIFIER_LEN,
            prefix_str=DBDefaults.DATABASE_NAME_PREFIX
        )

    @cached_property
    def drop_database(self):
        """
        This property stores the SQL statement to drop the database.

        Returns:
            drop_str (str): SQL drop statement
        """

        return 'DROP DATABASE {}'.format(self.dbname)

    @cached_property
    def create_database(self):
        """
        This property stores the SQL statement to create the database.

        Returns:
            create_str (str): SQL create statement
        """

        return 'CREATE DATABASE IF NOT EXISTS {}'.format(self.dbname)

    @cached_property
    def search_path(self):
        """
        Convenience property to fetch the search path which includes the
        database name in the path.

        This field does not represent the current contents of the database.
        Rather it is instead just a representation of what the search path
        string would be with the name from this instance. The database could
        very well have a different search path than the one below which is
        fine as it is not meant to be representative of the database's current
        state.

        Returns:
            search_path (str): Database search_path text
        """

        return '{}, "$user", public'.format(self.dbname)

    @cached_property
    def set_search_path_sql(self):
        """
        This property stores the SQL statement to set the search_path.

        Returns:
            search_path_str (str): SQL set search_path statement
        """

        return 'SET SEARCH_PATH TO {}'.format(self.search_path)

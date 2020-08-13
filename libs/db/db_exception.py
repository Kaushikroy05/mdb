class Error(Exception):
    """
    Error class derived from Base Exception
    Args:
        message (str): Error message
    """
    def __init__(self, message):
        super(Error, self).__init__()
        self.message = message

    _template = '{0.message}'

    def __str__(self):
        return self._template.format(self)


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass

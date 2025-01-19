class ColumnDoesNotExistError(Exception):
    pass


class InvalidPySparkDataTypeError(Exception):
    pass


class InvalidDataFrameNameError(Exception):
    pass


class PySparkVersionError(Exception):
    pass


class ValidationError(Exception):
    pass

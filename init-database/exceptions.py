class DatabaseConnectionError(Exception):
    """Can't connect to database for some reason"""


class ApiServiceError(Exception):
    """Can't send request to API for some reason"""


class SQLError(Exception):
    """Can't execute sql query fro some reason"""


class S3ServiceError(Exception):
    """Can't get response from s3 warehouse for some reason"""

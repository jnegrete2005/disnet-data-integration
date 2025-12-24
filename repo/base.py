from mysql.connector.cursor import MySQLCursor
from functools import wraps

from typing import TypeVar, Callable
from typing_extensions import ParamSpec, Concatenate

from repo.generic_repo import GenericRepo

P = ParamSpec("P")
R = TypeVar("R")


def sql_op(*, returns_bool: bool = True) -> Callable[
    [Callable[Concatenate[GenericRepo, MySQLCursor, P], R]],
    Callable[Concatenate[GenericRepo, P], R]
]:
    return_value = False if returns_bool else None

    def decorator(method: Callable[Concatenate[GenericRepo, MySQLCursor, P], R]) -> Callable[Concatenate[GenericRepo, P], R]:
        @wraps(method)
        def wrapper(self: GenericRepo, *args: P.args, **kwargs: P.kwargs) -> R:
            conn = self.db.conn
            cursor = self.db.get_cursor()
            if conn is None or cursor is None:
                return return_value
            try:
                result = method(self, cursor, *args, **kwargs)
                conn.commit()
                return result
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cursor.close()
        return wrapper
    return decorator

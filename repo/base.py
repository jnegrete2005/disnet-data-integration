from mysql.connector.cursor import MySQLCursor
from functools import wraps

from typing import TypeVar, Callable
from typing_extensions import ParamSpec, Concatenate

from repo.generic_repo import GenericRepo

from mysql.connector.errors import IntegrityError
from mysql.connector.errorcode import ER_DUP_ENTRY


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


def sql_insert_op(method):
    @wraps(method)
    def wrapper(self: GenericRepo, *args, **kwargs):
        conn = self.db.conn
        cursor = self.db.get_cursor()
        if conn is None or cursor is None:
            return False
        try:
            result = method(self, cursor, *args, **kwargs)
            conn.commit()
            return result
        except IntegrityError as ie:
            conn.rollback()
            if ie.errno == ER_DUP_ENTRY:
                # Duplicate entry, ignore
                return True
            raise
        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()

    return wrapper

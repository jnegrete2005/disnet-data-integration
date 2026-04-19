import functools
from typing import Any, Callable, Concatenate, ParamSpec, TypeVar

from mysql.connector.errorcode import ER_DUP_ENTRY
from mysql.connector.errors import IntegrityError

from repo.generic_repo import GenericRepo

# P captures the remaining arguments (e.g., `drug: Drug`)
P = ParamSpec("P")
# R captures the return type (e.g., `bool`)
R = TypeVar("R")


def sql_op(method: Callable[Concatenate[GenericRepo, Any, P], R]) -> Callable[Concatenate[GenericRepo, P], R]:
    @functools.wraps(method)
    def wrapper(self: GenericRepo, *args: P.args, **kwargs: P.kwargs) -> R:
        conn = self.db.conn
        cursor = self.db.get_cursor()
        if conn is None or cursor is None:
            raise RuntimeError("Database connection or cursor is None")
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


def sql_insert_op(method: Callable[Concatenate[GenericRepo, Any, P], R]) -> Callable[Concatenate[GenericRepo, P], R]:
    @functools.wraps(method)
    def wrapper(self: GenericRepo, *args: P.args, **kwargs: P.kwargs) -> R:
        conn = self.db.conn
        cursor = self.db.get_cursor()
        if conn is None or cursor is None:
            raise RuntimeError("Database connection or cursor is None")
        try:
            result = method(self, cursor, *args, **kwargs)
            conn.commit()
            return result
        except IntegrityError as ie:
            conn.rollback()
            if ie.errno == ER_DUP_ENTRY:
                # Duplicate entry, ignore
                return True  # type: ignore
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    return wrapper

from functools import wraps

from repo.generic_repo import GenericRepo

from mysql.connector.errors import IntegrityError
from mysql.connector.errorcode import ER_DUP_ENTRY


def sql_op(method):
    @wraps(method)
    def wrapper(self: GenericRepo, *args, **kwargs):
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


def sql_insert_op(method):
    @wraps(method)
    def wrapper(self: GenericRepo, *args, **kwargs):
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
                return True
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    return wrapper

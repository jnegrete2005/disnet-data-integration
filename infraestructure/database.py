import mysql.connector
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection

import os
from dotenv import load_dotenv

from pathlib import Path


class DisnetManager:
    def __init__(self, test=False):
        project_root = Path(__file__).resolve().parents[1]
        if not load_dotenv(project_root / ".env"):
            raise Error("ENV VARS NOT LOADED")
        self._conn = None
        self.__test = test

    def connect(self):
        if self._conn is not None:
            if self._conn.is_connected():
                return
            else:
                self._conn = None

        try:
            database = "drugslayer_test" if self.__test else "drugslayer"
            self._conn = mysql.connector.connect(
                host='127.0.0.1',
                port=3306,
                database=database,
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
        except Error as e:
            print(f"Error de conexión: {e}")

    def disconnect(self):
        if self.conn:
            self.conn.close()

    @property
    def conn(self) -> MySQLConnection | None:
        if self._conn is None or not self._conn.is_connected():
            return None
        return self._conn

    def get_cursor(self):
        return self.conn.cursor()

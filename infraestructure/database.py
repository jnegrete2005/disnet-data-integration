import os
from pathlib import Path

import mysql.connector
from dotenv import load_dotenv
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection


class DisnetManager:
    def __init__(self, test=False):
        project_root = Path(__file__).resolve().parents[1]
        if not load_dotenv(project_root / ".env"):
            raise Error("ENV VARS NOT LOADED")

        self._conn = None
        self.__test = test
        database = "drugslayer_test" if self.__test else "drugslayer"

        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "3306")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")

        self._db_config = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }

    def _create_connection(self):
        try:
            return mysql.connector.connect(**self._db_config)
        except Error as e:
            print(f"Connection error: {e}")
            raise e

    @property
    def conn(self) -> MySQLConnection | None:
        if self._conn is None:
            self._conn = self._create_connection()

        if self._conn.is_connected():
            return self._conn

        try:
            self._conn = self._create_connection()
            return self._conn
        except Error as e:
            print(f"Reconnection error: {e}")
            raise e

    def get_cursor(self):
        return self.conn.cursor()

    def disconnect(self):
        if self._conn and self._conn.is_connected():
            self._conn.close()

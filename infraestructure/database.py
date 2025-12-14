import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv


class DisnetManager:
    def __init__(self):
        load_dotenv("../.env")
        self._conn = None

        print(os.getenv('DB_USER'))

    def connect(self):
        if self._conn is not None:
            if self._conn.is_connected():
                return
            else:
                self._conn = None

        try:
            self._conn = mysql.connector.connect(
                host='127.0.0.1',
                port=3306,
                database='drugslayer',
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            print("Conexión establecida.")
        except Error as e:
            print(f"Error de conexión: {e}")

    def disconnect(self):
        if self.conn:
            self.conn.close()
            print("Conexión cerrada.")

    @property
    def conn(self):
        if self._conn is None or not self._conn.is_connected():
            return None
        return self._conn

    def get_cursor(self):
        return self.conn.cursor()

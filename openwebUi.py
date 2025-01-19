import mysql.connector
from mysql.connector import Error
import logging
from typing import List, Union, Generator, Iterator
import os
from pydantic import BaseModel

import aiohttp
import asyncio

logging.basicConfig(level=logging.DEBUG)

class Pipeline:
    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: str
        DB_USER: str
        DB_PASSWORD: str
        DB_DATABASE: str
        DB_TABLES: List[str]

    def __init__(self):
        self.name = "02 Database Query"
        self.conn = None
        self.nlsql_response = ""

        self.valves = self.Valves(
            **{
                "DB_HOST": os.getenv("MYSQL_HOST", "localhost"),
                "DB_PORT": os.getenv("MYSQL_PORT", '3306'),
                "DB_USER": os.getenv("MYSQL_USER", "root"),
                "DB_PASSWORD": os.getenv("MYSQL_PASSWORD", "Krishna@195"),
                "DB_DATABASE": os.getenv("MYSQL_DB", "chinook"),
                "DB_TABLES": ["albums"],
            }
        )

    def init_db_connection(self):
        connection_params = {
            'host': self.valves.DB_HOST,
            'port': int(self.valves.DB_PORT),
            'user': self.valves.DB_USER,
            'password': self.valves.DB_PASSWORD,
            'database': self.valves.DB_DATABASE
        }

        try:
            self.conn = mysql.connector.connect(**connection_params)
            if self.conn.is_connected():
                print("Connection to MySQL established successfully")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")

        # Create a cursor object
        self.cur = self.conn.cursor()

        # Query to get the list of tables
        self.cur.execute("SHOW TABLES;")

        # Fetch and print the table names
        tables = self.cur.fetchall()
        print("Tables in the database:")
        for table in tables:
            print(table[0])

    async def on_startup(self):
        self.init_db_connection()

    async def on_shutdown(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    async def make_request_with_retry(self, url, params, retries=3, timeout=10):
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=timeout) as response:
                        response.raise_for_status()
                        return await response.text()
            except (aiohttp.ClientResponseError, aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                logging.error(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt + 1 == retries:
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        try:
            conn = mysql.connector.connect(
                host=self.valves.DB_HOST,
                port=int(self.valves.DB_PORT),
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                database=self.valves.DB_DATABASE
            )

            conn.autocommit = True
            cursor = conn.cursor()
            sql = user_message

            cursor.execute(sql)
            result = cursor.fetchall()
            return str(result)

        except mysql.connector.Error as e:
            logging.error(f"MySQL Error: {e}")
            return f"MySQL Error: {e}"
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return f"Unexpected error: {e}"

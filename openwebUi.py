from typing import List, Union, Generator, Iterator
import logging
import os
from pydantic import BaseModel
import pymysql
from pymysql import OperationalError

class Pipeline:
    class Valves(BaseModel):
        DB_HOST: str
        DB_PORT: int
        DB_USER: str
        DB_PASSWORD: str
        DB_DATABASE: str
        DB_TABLES: List[str]

    def __init__(self):
        self.name = "MySQL Database Pipeline"
        self.conn = None

        self.valves = self.Valves(
            **{
                "DB_HOST": os.getenv("MYSQL_HOST", "localhost"),
                "DB_PORT": int(os.getenv("MYSQL_PORT", 3306)),
                "DB_USER": os.getenv("MYSQL_USER", "root"),
                "DB_PASSWORD": os.getenv("MYSQL_PASSWORD", "Krishna@195"),
                "DB_DATABASE": os.getenv("MYSQL_DB", "testdb"),  # Replace with your database name
                "DB_TABLES": ["movies"]  # Replace with your table names
            }
        )

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        self.init_db_connection()

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown:{__name__}")
        if self.conn:
            self.conn.close()

    def init_db_connection(self):
        try:
            self.conn = pymysql.connect(
                host=self.valves.DB_HOST,
                port=self.valves.DB_PORT,
                user=self.valves.DB_USER,
                password=self.valves.DB_PASSWORD,
                database=self.valves.DB_DATABASE
            )
            print("Connection to MySQL established successfully")

            # Create a cursor object
            self.cur = self.conn.cursor()

            # Query to get the list of tables
            self.cur.execute("SHOW TABLES;")

            # Fetch and print the table names
            tables = self.cur.fetchall()
            print("Tables in the database:")
            for table in tables:
                print(table[0])

            # Query to get the column names of a specific table
            for table in self.valves.DB_TABLES:
                print(f"Columns in the '{table}' table:")
                self.cur.execute(f"SHOW COLUMNS FROM {table};")
                columns = self.cur.fetchall()
                for column in columns:
                    print(column[0])
        except OperationalError as e:
            print(f"Error connecting to MySQL: {e}")

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        print(f"received message from user: {user_message}")  # Log user_message

        try:
            query = "SELECT * FROM movies LIMIT 5;"  # Example query

            # Execute query
            self.cur.execute(query)
            results = self.cur.fetchall()

            print("Query Results:")
            for row in results:
                print(row)

            return results
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return f"Unexpected error: {e}"

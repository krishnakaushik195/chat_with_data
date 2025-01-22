import logging
from typing import List, Union, Generator, Iterator
import os
from pydantic import BaseModel
import pymysql
from pymysql import OperationalError

logging.basicConfig(level=logging.DEBUG)

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
        except OperationalError as e:
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

        # Query to get the column names of a specific table
        for table in self.valves.DB_TABLES:
            print(f"Columns in the '{table}' table:")
            self.cur.execute(f"SHOW COLUMNS FROM {table};")
            columns = self.cur.fetchall()
            for column in columns:
                print(column[0])

    async def on_startup(self):
        self.init_db_connection()

    async def on_shutdown(self):
        self.cur.close()
        self.conn.close()

    def extract_sql_query(self, response_object):
        for key, value in response_object.items():
            if isinstance(value, dict) and 'sql_query' in value:
                return value['sql_query']
            elif key == 'sql_query':
                return value
        return None

    def handle_streaming_response(self, response_gen):
        final_response = ""
        for chunk in response_gen:
            final_response += chunk
        return final_response

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
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

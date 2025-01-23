from typing import List, Union, Generator, Iterator
from mysql.connector import connection, Error
import asyncio
from langchain_community.utilities import SQLDatabase

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        # Updated connection details (use password if needed)
        self.db_config = {
            'user': 'root',
            'host': 'host.docker.internal',  # assuming localhost as the host
            'database': 'chinook',  # your specified database
            'password': 'Krishna@195'
        }
        self.conn = None

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        # Connect to the database when the server starts
        self.connect_to_db()

    async def on_shutdown(self):
        # This function is called when the server is shut down.
        print(f"on_shutdown:{__name__}")
        # Close the database connection when the server shuts down
        self.disconnect_from_db()

    def connect_to_db(self):
        """Establish a connection to the MySQL database."""
        try:
            self.conn = connection.MySQLConnection(**self.db_config)
            print("Connected to MySQL server.")
        except Exception as e:
            print(f"Error connecting to MySQL: {e}")

    def disconnect_from_db(self):
        """Close the MySQL database connection."""
        if self.conn:
            self.conn.close()
            print("Disconnected from MySQL server.")

    def get_schema(self, db_name):
        """Retrieve and return the schema of the specified database."""
        try:
            return db_connections[db_name].get_table_info()
        except KeyError:
            print(f"Database {db_name} not found in connections.")
            return f"Database {db_name} not found."
        except Exception as e:
            print(f"Error retrieving schema: {e}")
            return f"Error retrieving schema: {e}"

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        print(f"received message from user: {user_message}")  # user_message to logs
        
        # Check if the connection is established and return the appropriate message
        if self.conn:
            schema = self.get_schema('chinook')
            return f"Connected to MySQL database: chinook. Schema: {schema}. Received message from user: {user_message}"
        else:
            return "Database connection not established. Please check the connection."

# Example usage if required
# pipeline = Pipeline()
# asyncio.run(pipeline.on_startup())

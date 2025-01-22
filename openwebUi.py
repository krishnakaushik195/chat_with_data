from typing import List, Union, Generator, Iterator
from mysql.connector import connection
import asyncio
from langchain.sql_database import SQLDatabase

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        
        # Database configurations
        self.db_configs = {
            "sys": {
                'user': 'root',
                'host': 'host.docker.internal',  # assuming localhost as the host
                'database': 'sys',
                'password': 'Krishna@195'
            },
            "chinook": {
                'user': 'root',
                'host': 'host.docker.internal',
                'database': 'chinook',
                'password': 'Krishna@195'
            },
            "sakila": {
                'user': 'root',
                'host': 'host.docker.internal',
                'database': 'sakila',
                'password': 'Krishna@195'
            }
        }
        self.connections = {}
        self.schemas = {}

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        # Connect to all databases when the server starts
        self.connect_to_dbs()
        self.fetch_schemas()

    async def on_shutdown(self):
        # This function is called when the server is shut down.
        print(f"on_shutdown:{__name__}")
        # Disconnect from all databases when the server shuts down
        self.disconnect_from_dbs()

    def connect_to_dbs(self):
        """Establish connections to all specified databases."""
        for db_name, config in self.db_configs.items():
            try:
                conn = connection.MySQLConnection(**config)
                print(f"Connected to MySQL database: {db_name}")
                self.connections[db_name] = conn
            except Exception as e:
                print(f"Error connecting to database {db_name}: {e}")

    def fetch_schemas(self):
        """Fetch and store database schemas using LangChain."""
        for db_name, config in self.db_configs.items():
            try:
                uri = f"mysql+mysqlconnector://{config['user']}:{config['password']}@{config['host']}/{config['database']}"
                db = SQLDatabase.from_uri(uri)
                self.schemas[db_name] = db.get_schema()
                print(f"Fetched schema for database {db_name}: {self.schemas[db_name]}")
            except Exception as e:
                print(f"Error fetching schema for database {db_name}: {e}")

    def disconnect_from_dbs(self):
        """Close all database connections."""
        for db_name, conn in self.connections.items():
            try:
                conn.close()
                print(f"Disconnected from MySQL database: {db_name}")
            except Exception as e:
                print(f"Error disconnecting from database {db_name}: {e}")

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        print(f"received message from user: {user_message}")  # user_message to logs
        
        # Check if connections are established and return the appropriate message
        connected_dbs = [db_name for db_name, conn in self.connections.items() if conn.is_connected()]
        if connected_dbs:
            return f"Connected to databases: {', '.join(connected_dbs)}. Received message from user: {user_message}"
        else:
            return "No database connections established. Please check the configurations."

# Example usage if required
# pipeline = Pipeline()
# asyncio.run(pipeline.on_startup())

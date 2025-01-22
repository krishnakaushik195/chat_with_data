from typing import List, Union, Generator, Iterator
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import asyncio

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        
        # Database URIs
        self.db_uris = {
            "sys": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys',
            "chinook": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/chinook',
            "sakila": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sakila'
        }
        self.engines = {}

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        # Connect to all databases when the server starts
        self.connect_to_dbs()

    async def on_shutdown(self):
        # This function is called when the server is shut down.
        print(f"on_shutdown:{__name__}")
        # Dispose all database connections when the server shuts down
        self.disconnect_from_dbs()

    def connect_to_dbs(self):
        """Establish connections to all specified databases."""
        for db_name, uri in self.db_uris.items():
            try:
                engine = create_engine(uri)
                # Test connection
                with engine.connect() as conn:
                    print(f"Connected to MySQL database: {db_name}")
                self.engines[db_name] = engine
            except Exception as e:
                print(f"Error connecting to database {db_name}: {e}")

    def disconnect_from_dbs(self):
        """Dispose all database connections."""
        for db_name, engine in self.engines.items():
            try:
                engine.dispose()
                print(f"Disconnected from MySQL database: {db_name}")
            except Exception as e:
                print(f"Error disconnecting from database {db_name}: {e}")

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        print(f"received message from user: {user_message}")  # user_message to logs
        
        # Check if connections are established and return the appropriate message
        connected_dbs = [db_name for db_name, engine in self.engines.items() if engine]
        if connected_dbs:
            return f"Connected to databases: {', '.join(connected_dbs)}. Received message from user: {user_message}"
        else:
            return "No database connections established. Please check the configurations."

# Example usage if required
# pipeline = Pipeline()
# asyncio.run(pipeline.on_startup())

from langchain_community.utilities import SQLDatabase
from typing import List, Union, Generator, Iterator
import asyncio

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        # Database URI: replace `password`, `host`, and `database` as needed
        self.db_uri = "mysql+mysqlconnector://root:Krishna@195@host.docker.internal/chinook"
        self.db = None

    async def on_startup(self):
        """Initialize resources during server startup."""
        print(f"on_startup: {__name__}")
        # Create SQLDatabase instance
        try:
            self.db = SQLDatabase.from_uri(self.db_uri)
            print("Connected to MySQL database using LangChain.")
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    async def on_shutdown(self):
        """Clean up resources during server shutdown."""
        print(f"on_shutdown: {__name__}")
        # SQLDatabase doesn't need explicit disconnection

    def get_schema(self):
        """Retrieve and return the schema of the connected database."""
        if not self.db:
            return "Database connection not established."
        try:
            # Use LangChain's `get_table_info` to fetch the schema
            schema = self.db.get_table_info()
            return schema
        except Exception as e:
            return f"Error retrieving schema: {e}"

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Handle incoming user messages."""
        print(f"Received message from user: {user_message}")
        
        if self.db:
            schema = self.get_schema()
            return f"Connected to MySQL database: chinook.\nSchema: {schema}\nReceived message from user: {user_message}"
        else:
            return "Database connection not established. Please check the connection."


# Example usage if required
# pipeline = Pipeline()
# asyncio.run(pipeline.on_startup())

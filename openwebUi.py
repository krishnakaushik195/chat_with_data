from langchain.sql_database import SQLDatabase
from typing import List, Optional
from pydantic import BaseModel, Field
import time
import logging

logging.basicConfig(level=logging.DEBUG)


class Pipeline:
    """
    A pipeline class for managing database interactions using LangChain's SQLDatabase.
    """
    class Config(BaseModel):
        db_uri: str = Field(
            default="mysql+mysqlconnector://root:Krishna%40195@localhost:3306/chinook",
            description="The URI for connecting to the database."
        )
        emit_interval: float = Field(
            default=2.0,
            description="Interval in seconds between status emissions."
        )

    def __init__(self):
        """
        Initialize the Pipeline with default configuration.
        """
        self.type = "pipe"
        self.id = "sql_pipeline"
        self.name = "SQL Database Pipeline"
        self.config = self.Config()
        self.db = None
        self.last_emit_time = 0

    def initialize_database(self):
        """
        Initialize the database connection using the URI from the configuration.
        """
        try:
            self.db = SQLDatabase.from_uri(self.config.db_uri)
            logging.info("Database connection initialized.")
        except Exception as e:
            logging.error(f"Failed to initialize database connection: {e}")

    def get_schema(self) -> Optional[str]:
        """
        Retrieve the database schema.

        Returns:
            Optional[str]: A string representation of the database schema, or None if an error occurs.
        """
        if self.db is None:
            logging.error("Database connection is not initialized. Call initialize_database() first.")
            return None

        try:
            schema = self.db.get_table_info()
            return schema
        except Exception as e:
            logging.error(f"Failed to retrieve schema: {e}")
            return None

    def execute_query(self, query: str) -> Optional[List[tuple]]:
        """
        Execute a raw SQL query and return the result.

        Args:
            query (str): The SQL query to execute.

        Returns:
            Optional[List[tuple]]: The result of the query as a list of tuples.
        """
        if self.db is None:
            logging.error("Database connection is not initialized. Call initialize_database() first.")
            return None

        try:
            result = self.db.run(query)
            logging.info(f"Query executed successfully: {query}")
            return result
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            return None

    async def emit_status(self, level: str, message: str, done: bool):
        """
        Emit a status message at intervals defined in the configuration.

        Args:
            level (str): The level of the status (e.g., 'info', 'error').
            message (str): The status message.
            done (bool): Whether the operation is complete.
        """
        current_time = time.time()
        if current_time - self.last_emit_time >= self.config.emit_interval or done:
            logging.info({
                "status": "complete" if done else "in_progress",
                "level": level,
                "description": message,
                "done": done
            })
            self.last_emit_time = current_time


# Example usage
if __name__ == "__main__":
    pipeline = Pipeline()

    # Initialize database connection
    pipeline.initialize_database()

    # Retrieve and display database schema
    schema_info = pipeline.get_schema()
    if schema_info:
        logging.info("Database Schema:")
        logging.info(schema_info)

    # Example SQL query execution
    query = "SELECT * FROM Artist LIMIT 5;"
    query_result = pipeline.execute_query(query)
    if query_result:
        logging.info("Query Result:")
        for row in query_result:
            logging.info(row)

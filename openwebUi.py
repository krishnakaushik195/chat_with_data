from typing import List, Union, Generator, Iterator
from langchain.sql_database import SQLDatabase  # Ensure correct library
import subprocess


class Pipeline:
    def __init__(self):
        # Optionally, you can set the id and name of the pipeline.
        # Best practice is to not specify the id so that it can be automatically inferred from the filename, so that users can install multiple versions of the same pipeline.
        # The identifier must be unique across all pipelines.
        # The identifier must be an alphanumeric string that can include underscores or hyphens. It cannot contain spaces, special characters, slashes, or backslashes.
        # self.id = "database_tools_pipeline"
        self.name = "Database Tools Pipeline"
        self.db = None
        pass

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is stopped.
        print(f"on_shutdown:{__name__}")
        pass

    def connect_to_database(self, db_uri: str) -> str:
        """
        Connect to the SQL database using the provided URI.

        :param db_uri: The URI for the database connection.
        :return: Connection status message.
        """
        try:
            self.db = SQLDatabase.from_uri(db_uri)
            return "Successfully connected to the database."
        except Exception as e:
            return f"Failed to connect to the database: {str(e)}"

    def get_database_schema(self) -> str:
        """
        Retrieve and display the database schema.

        :return: Database schema or error message.
        """
        if not self.db:
            return "Database is not connected. Use the connect_to_database method first."

        try:
            schema_info = self.db.get_table_info()
            return f"Database Schema:\n{schema_info}"
        except Exception as e:
            return f"Failed to retrieve database schema: {str(e)}"

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        """
        Custom pipeline execution method.

        :param user_message: User-provided input, e.g., database URI or commands.
        :param model_id: Model identifier (unused in this pipeline).
        :param messages: List of messages (unused in this pipeline).
        :param body: Additional data.
        :return: Schema information or error messages.
        """
        print(f"pipe:{__name__}")

        print(messages)
        print(user_message)

        if body.get("title", False):
            print("Title Generation")
            return "Database Tools Pipeline"
        else:
            if not self.db:
                connect_message = self.connect_to_database(user_message)
                return connect_message

            schema = self.get_database_schema()
            return schema



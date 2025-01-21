from typing import List, Union, Generator, Iterator
from langchain.sql_database import SQLDatabase  # Ensure correct library
import subprocess


def pipe(
    user_message: str, model_id: str, messages: List[dict], body: dict
) -> Union[str, Generator, Iterator]:
    """
    Custom pipeline execution function.

    :param user_message: User-provided input, e.g., database URI or commands.
    :param model_id: Model identifier (unused in this pipeline).
    :param messages: List of messages (unused in this pipeline).
    :param body: Additional data.
    :return: Schema information or error messages.
    """
    print("pipe function called")

    print(messages)
    print(user_message)

    db = None

    def connect_to_database(db_uri: str) -> str:
        """
        Connect to the SQL database using the provided URI.

        :param db_uri: The URI for the database connection.
        :return: Connection status message.
        """
        nonlocal db
        try:
            db = SQLDatabase.from_uri(db_uri)
            return "Successfully connected to the database."
        except Exception as e:
            return f"Failed to connect to the database: {str(e)}"

    def get_database_schema() -> str:
        """
        Retrieve and display the database schema.

        :return: Database schema or error message.
        """
        if not db:
            return "Database is not connected. Use the connect_to_database method first."

        try:
            schema_info = db.get_table_info()
            return f"Database Schema:\n{schema_info}"
        except Exception as e:
            return f"Failed to retrieve database schema: {str(e)}"

    def execute_python_code(code: str) -> str:
        """
        Execute arbitrary Python code provided as a string.

        :param code: The Python code to execute.
        :return: Execution output or error message.
        """
        try:
            result = subprocess.run(
                ["python", "-c", code], capture_output=True, text=True, check=True
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            return e.output.strip()

    if body.get("title", False):
        print("Title Generation")
        return "Database Tools Pipeline"
    else:
        if not db:
            connect_message = connect_to_database(user_message)
            if "Failed" in connect_message:
                return connect_message

        schema = get_database_schema()
        return schema

from typing import List, Union, Generator, Iterator
from langchain.sql_database import SQLDatabase  # Ensure this is the correct library for SQLDatabase

class Pipeline:
    def __init__(self):
        self.name = "Schema Retriever"
        self.db_uri = 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/chinook'
        self.db = SQLDatabase.from_uri(self.db_uri)
        pass

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup: {__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shut down.
        print(f"on_shutdown: {__name__}")
        pass

    def get_schema(self) -> str:
        """
        Retrieve schema information as a string.
        """
        schema = self.db.get_table_info()
        return schema

    def schema_character_count(self, schema: str) -> int:
        """
        Calculate the character count of the schema.
        """
        schema_str = schema if isinstance(schema, str) else str(schema)
        return len(schema_str)

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """
        This function is called when a new user_message is received.
        """
        print(f"received message from user: {user_message}")

        if user_message.lower() == "get schema":
            schema_info = self.get_schema()
            char_count = self.schema_character_count(schema_info)

            response = (
                f"Schema Information:\n{schema_info}\n\n"
                f"Character Count of the Schema: {char_count} characters"
            )
        else:
            response = "Unrecognized command. Please send 'get schema' to retrieve schema information."

        return response

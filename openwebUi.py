from langchain_community.utilities import SQLDatabase
from typing import List, Union, Generator, Iterator

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        pass

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown:{__name__}")
        pass
    def get_schema(self):
        # Define the MySQL URI
        mysql_uri = 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys'
        # Create a SQLDatabase object using the URI
        db = SQLDatabase.from_uri(mysql_uri)
        # Fetch schema information
        schema = db.get_table_info()

        return schema

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        
        print(f"received message from user: {user_message}")  # user_message to logs

        # Call the get_schema function to get the database schema
        schema = self.get_schema()
        
        # Return the schema and user message as part of the response
        return (f"received message from user: {user_message}\nSchema: {schema}")
        



from typing import List, Union, Generator, Iterator
import sqlalchemy
from sqlalchemy import create_engine


# Define your database URIs
db_uris = {
    "sys": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/sys',
    "chinook": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/chinook',
    "sakila" : 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/sakila'
}


class Pipeline:
    def __init__(self, db_name: str):
        """
        Initialize the pipeline with a database name from the db_uris dictionary.
        
        Args:
        db_name (str): The name of the database (e.g., 'sys', 'chinook', 'sakila').
        """
        self.name = "00 Repeater Example"
        
        # Select the database URI based on the db_name provided
        self.db_uri = db_uris.get(db_name)
        if not self.db_uri:
            raise ValueError(f"Database {db_name} not found in db_uris.")
        print(f"Using database: {db_name}")

    async def on_startup(self):
        """
        This function is called when the server is started.
        """
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        """
        This function is called when the server is shutdown.
        """
        print(f"on_shutdown:{__name__}")
        pass

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """
        This function is called when a new user_message is received.
        
        It will print the database schema whenever a message is received.

        Args:
        user_message (str): The message from the user.
        model_id (str): The model identifier.
        messages (List[dict]): List of message objects.
        body (dict): Additional data or context related to the message.

        Returns:
        Union[str, Generator, Iterator]: A string response to be sent back, or a generator/iterator if needed.
        """
        print(f"received message from user: {user_message}")  # user_message to logs
        
        # Fetch the schema whenever a message is received
        schema_info = self.get_schema(self.db_uri)
        
        # Convert schema information to a string for displaying
        schema_display = "\n".join(
            [f"Table: {table_name}, Columns: {', '.join(columns)}" for table_name, columns in schema_info.items()]
        )
        
        return f"received message from user: {user_message}\n\nDatabase Schema:\n{schema_display}"

    def get_schema(self, db_uri: str) -> dict:
        """
        Fetches schema information from a given database.

        Args:
        db_uri (str): The database URI for connecting to the database.

        Returns:
        dict: A dictionary containing the schema information (table names and columns).
        """
        engine = create_engine(db_uri)
        with engine.connect() as connection:
            query = "SHOW TABLES;"
            tables = connection.execute(query).fetchall()

            schema_info = {}
            for (table_name,) in tables:
                schema_query = f"SHOW COLUMNS FROM {table_name};"
                columns = connection.execute(schema_query).fetchall()
                schema_info[table_name] = [column[0] for column in columns]

            return schema_info

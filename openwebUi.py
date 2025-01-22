from typing import List, Union, Generator, Iterator
from langchain_community.utilities import SQLDatabase

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        # MySQL connection URI
        self.mysql_uri = 'mysql+mysqlconnector://root:Krishna@195@localhost:3306/Chinook'
        self.db = None

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        # Connect to the database when server starts
        self.connect_to_db()

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown:{__name__}")
        # Close the database connection when server shuts down
        self.disconnect_from_db()

    def connect_to_db(self):
        """Establish a connection to the MySQL database using Langchain."""
        try:
            self.db = SQLDatabase.from_uri(self.mysql_uri)
            print("Connected to MySQL database via Langchain.")
        except Exception as e:
            print(f"Error connecting to MySQL: {e}")

    def disconnect_from_db(self):
        """Close the database connection."""
        if self.db:
            print("Disconnected from MySQL database.")
            self.db = None
        else:
            print("No active database connection to close.")

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        print(f"received message from user: {user_message}") # user_message to logs
        
        # Check if the connection is established and return the appropriate message
        if self.db:
            return f"Connected to MySQL database. Received message from user: {user_message}"
        else:
            return "Database connection not established. Please check the connection."

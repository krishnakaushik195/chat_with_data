from typing import List, Union, Generator, Iterator
from mysql.connector import connection

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        self.db_config = {
            'user': 'root',
            'host': 'localhost',
            'database': 'College',
            'password': 'Krishna@195'
        }
        self.conn = None

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
        """Establish a connection to the MySQL database."""
        try:
            self.conn = connection.MySQLConnection(**self.db_config)
            print("Connected to MySQL server.")
        except Exception as e:
            print(f"Error connecting to MySQL: {e}")

    def disconnect_from_db(self):
        """Close the MySQL database connection."""
        if self.conn:
            self.conn.close()
            print("Disconnected from MySQL server.")

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        print(f"received message from user: {user_message}") # user_message to logs
        
        # Check if the connection is established and return the appropriate message
        if self.conn:
            return f"Connected to MySQL database. Received message from user: {user_message}"
        else:
            return "Database connection not established. Please check the connection."

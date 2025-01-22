import logging
from typing import List, Union, Generator, Iterator
from mysql.connector import connection
import asyncio

# Set up logging
logging.basicConfig(level=logging.DEBUG)  # You can change this to INFO or WARNING in production
logger = logging.getLogger(__name__)

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        # Updated connection details (use password if needed)
        self.db_config = {
            'user': 'root',
            'host': 'localhost',  # assuming localhost as the host
            'database': 'chinook',  # your specified database
            'password': 'Krishna@195'
        }
        self.conn = None

    async def on_startup(self):
        # This function is called when the server is started.
        logger.info(f"on_startup:{__name__}")
        # Connect to the database when the server starts
        self.connect_to_db()

    async def on_shutdown(self):
        # This function is called when the server is shut down.
        logger.info(f"on_shutdown:{__name__}")
        # Close the database connection when the server shuts down
        self.disconnect_from_db()

    def connect_to_db(self):
        """Establish a connection to the MySQL database."""
        try:
            self.conn = connection.MySQLConnection(**self.db_config)
            logger.info("Connected to MySQL server.")
        except Exception as e:
            logger.error(f"Error connecting to MySQL: {e}")

    def disconnect_from_db(self):
        """Close the MySQL database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from MySQL server.")

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        logger.info(f"received message from user: {user_message}")  # user_message to logs
        
        # Check if the connection is established and return the appropriate message
        if self.conn:
            return f"Connected to MySQL database: chinook. Received message from user: {user_message}"
        else:
            return "Database connection not established. Please check the connection."

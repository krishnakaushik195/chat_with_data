from typing import List, Union, Generator, Iterator
from mysql.connector import connection
import asyncio

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        # Updated connection details (use password if needed)
        self.db_config = {
            'user': 'root',
            'host': 'host.docker.internal',  # assuming localhost as the host
            'database': 'chinook',  # your specified database
            'password': 'Krishna@195'
        }
        self.conn = None

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        # Connect to the database when the server starts
        self.connect_to_db()

    async def on_shutdown(self):
        # This function is called when the server is shut down.
        print(f"on_shutdown:{__name__}")
        # Close the database connection when the server shuts down
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
        print(f"received message from user: {user_message}")  # user_message to logs
        
        # Check if the connection is established and return the appropriate message
        if self.conn:
            return f"Connected to MySQL database: chinook. Received message from user: {user_message}"
        else:
            return "Database connection not established. Please check the connection."

    def get_schema(self):
        """Retrieve and display the schema of the database."""
        if not self.conn:
            print("Database connection is not established.")
            return
        
        cursor = self.conn.cursor()
        
        try:
            # Get all the tables in the database
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            schema = {}
            
            for (table,) in tables:
                # Describe each table to get the columns and their types
                cursor.execute(f"DESCRIBE {table}")
                columns = cursor.fetchall()
                
                schema[table] = [{'Field': field, 'Type': col_type, 'Null': null} for field, col_type, _, null, _, _ in columns]
            
            return schema
        except Exception as e:
            print(f"Error fetching schema: {e}")
        finally:
            cursor.close()

# Example usage:
pipeline = Pipeline()
pipeline.connect_to_db()
schema = pipeline.get_schema()
if schema:
    print("Database Schema:")
    for table, columns in schema.items():
        print(f"Table: {table}")
        for column in columns:
            print(f"  Field: {column['Field']}, Type: {column['Type']}, Null: {column['Null']}")

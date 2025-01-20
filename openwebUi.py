import os
from typing import Optional
from langchain.sql_database import SQLDatabase  # Ensure correct library
from blueprints.function_calling_blueprint import Pipeline as FunctionCallingBlueprint


class Pipeline(FunctionCallingBlueprint):
    class Valves(FunctionCallingBlueprint.Valves):
        # Add your custom parameters here
        DATABASE_URI: str = ""

    class Tools:
        def __init__(self, pipeline) -> None:
            self.pipeline = pipeline
            self.db: Optional[SQLDatabase] = None

        def connect_to_database(self) -> str:
            """
            Connect to the SQL database using the URI defined in the valves.

            :return: Connection status message.
            """
            if not self.pipeline.valves.DATABASE_URI:
                return "Database URI is not set. Please set the DATABASE_URI in the valves."
            
            try:
                self.db = SQLDatabase.from_uri(self.pipeline.valves.DATABASE_URI)
                return "Successfully connected to the database."
            except Exception as e:
                return f"Failed to connect to the database: {str(e)}"

        def get_database_schema(self) -> str:
            """
            Retrieve and display the database schema.

            :return: Database schema or error message.
            """
            if not self.db:
                return "Database is not connected. Use the connect_to_database tool first."
            
            try:
                schema_info = self.db.get_table_info()
                return f"Database Schema:\n{schema_info}"
            except Exception as e:
                return f"Failed to retrieve database schema: {str(e)}"

    def __init__(self):
        super().__init__()
        self.name = "Database Tools Pipeline"
        self.valves = self.Valves(
            **{
                **self.valves.model_dump(),
                "pipelines": ["*"],  # Connect to all pipelines
                "DATABASE_URI": os.getenv(
                    "DATABASE_URI", "mysql+mysqlconnector://root:Krishna%40195@localhost:3306/chinook"
                ),
            },
        )
        self.tools = self.Tools(self)

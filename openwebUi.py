"""
title: Langflow Pipe Function
author: moss
version: 0.7.0

This module defines a Pipe class that integrates Open WebUI with Langflow.
"""

from typing import Optional, Callable, Awaitable
from pydantic import BaseModel, Field
import requests
import time


class Pipeline:
    """
    A pipeline class for managing database interactions using LangChain's SQLDatabase.
    """
    class Config(BaseModel):
        db_uri: str = Field(
            default="mysql+mysqlconnector://root:Krishna%40195@localhost:3306/chinook",
            description="The URI for connecting to the database."
        )
        emit_interval: float = Field(
            default=2.0,
            description="Interval in seconds between status emissions."
        )

    def __init__(self):
        """
        Initialize the Pipeline with default configuration.
        """
        self.type = "pipe"
        self.id = "sql_pipeline"
        self.name = "SQL Database Pipeline"
        self.config = self.Config()
        self.db = None
        self.last_emit_time = 0

    def initialize_database(self):
        """
        Initialize the database connection using the URI from the configuration.
        """
        try:
            self.db = SQLDatabase.from_uri(self.config.db_uri)
            print("Database connection initialized.")
        except Exception as e:
            print(f"Failed to initialize database connection: {e}")

    def get_schema(self) -> Optional[str]:
        """
        Retrieve the database schema.

        Returns:
            Optional[str]: A string representation of the database schema, or None if an error occurs.
        """
        if self.db is None:
            print("Database connection is not initialized. Call initialize_database() first.")
            return None

        try:
            schema = self.db.get_table_info()
            return schema
        except Exception as e:
            print(f"Failed to retrieve schema: {e}")
            return None

    async def emit_status(
        self,
        __event_emitter__: Callable[[dict], Awaitable[None]],
        level: str,
        message: str,
        done: bool,
    ):
        """
        Emit a status message at intervals defined in the configuration.

        Args:
            __event_emitter__ (Callable[[dict], Awaitable[None]]): Event emitter function.
            level (str): The level of the status (e.g., 'info', 'error').
            message (str): The status message.
            done (bool): Whether the operation is complete.
        """
        current_time = time.time()
        if __event_emitter__ and (
            current_time - self.last_emit_time >= self.config.emit_interval or done
        ):
            await __event_emitter__(
                {
                    "type": "status",
                    "data": {
                        "status": "complete" if done else "in_progress",
                        "level": level,
                        "description": message,
                        "done": done,
                    },
                }
            )
            self.last_emit_time = current_time

    async def pipe(
        self,
        body: dict,
        __user__: Optional[dict] = None,
        __event_emitter__: Callable[[dict], Awaitable[None]] = None,
        __event_call__: Callable[[dict], Awaitable[dict]] = None,
    ) -> Optional[dict]:
        """
        Execute the pipeline with the provided body and optional user and event hooks.

        Args:
            body (dict): The input data for the pipeline.
            __user__ (Optional[dict]): User information (default: None).
            __event_emitter__ (Callable[[dict], Awaitable[None]]): Event emitter (default: None).
            __event_call__ (Callable[[dict], Awaitable[dict]]): Event call function (default: None).

        Returns:
            Optional[dict]: The updated body after processing.
        """
        await self.emit_status(
            __event_emitter__, "info", "Calling SQL Database...
", False
        )

        messages = body.get("messages", [])

        if messages:
            user_message = messages[-1]["content"]
            try:
                result = self.db.run(user_message)
                body["messages"].append(
                    {"role": "assistant", "content": str(result)}
                )
            except Exception as e:
                await self.emit_status(
                    __event_emitter__, "error", f"Error during query execution: {str(e)}", True
                )
                return {"error": str(e)}
        else:
            await self.emit_status(
                __event_emitter__, "error", "No messages found in the request body", True
            )
            body["messages"].append(
                {
                    "role": "assistant",
                    "content": "No messages found in the request body",
                }
            )

        await self.emit_status(__event_emitter__, "info", "Complete", True)
        return body


# Example usage
pipeline = Pipeline()

# Initialize database connection
pipeline.initialize_database()

# Retrieve and display database schema
schema_info = pipeline.get_schema()
if schema_info:
    print("Database Schema:")
    print(schema_info)

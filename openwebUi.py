import os
import logging
from typing import List, Union, Generator, Iterator
from pydantic import BaseModel
import aiohttp
import asyncio
from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq

logging.basicConfig(level=logging.DEBUG)

class Pipeline:
    class Config(BaseModel):
        DB_URIS: dict
        GROQ_API_KEY: str

    def __init__(self):
        self.name = "Database Pipeline"
        self.db_connections = {}
        self.llm = None

        # Load configuration
        self.config = self.Config(
            DB_URIS={
                "sys": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/sys',
                "chinook": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/chinook',
                "sakila": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/sakila'
            },
            GROQ_API_KEY=os.getenv("GROQ_API_KEY", "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")
        )

    def init_db_connections(self):
        """Initialize database connections."""
        try:
            self.db_connections = {
                name: SQLDatabase.from_uri(uri) for name, uri in self.config.DB_URIS.items()
            }
            logging.info("Database connections initialized successfully.")
        except Exception as e:
            logging.error(f"Error initializing database connections: {e}")

    def get_schema(self, db_name):
        """Retrieve schema for the specified database."""
        return self.db_connections[db_name].get_table_info()

    async def on_startup(self):
        """Startup routine to initialize resources."""
        self.init_db_connections()
        os.environ['GROQ_API_KEY'] = self.config.GROQ_API_KEY
        self.llm = ChatGroq()
        logging.info("Pipeline started successfully.")

    async def on_shutdown(self):
        """Shutdown routine to clean up resources."""
        for conn in self.db_connections.values():
            conn.engine.dispose()
        logging.info("Pipeline shut down successfully.")

    def determine_relevant_database(self, question):
        """Determine the relevant database for a given question."""
        schema_matching_prompt_template = ChatPromptTemplate.from_template(
            """You are an intelligent assistant. Based on the following database schema, check where this user's question belongs or is related to this schema. Respond strictly with yes or no without any explanations or additional details.
Schema:{schema}
Question: {question}
Match:"""
        )

        for db_name in self.db_connections.keys():
            schema = self.get_schema(db_name)
            formatted_prompt = schema_matching_prompt_template.format(schema=schema, question=question)
            response = self.llm.invoke(formatted_prompt).content.strip().lower()
            normalized_response = response.replace("'", "").rstrip('.').strip()
            if normalized_response == "yes":
                return db_name
        return None

    def run_query(self, database, query):
        """Execute a SQL query on the specified database."""
        try:
            return self.db_connections[database].run(query)
        except Exception as e:
            logging.error(f"Error executing query: {e}")
            return str(e)

    async def pipe(self, question: str) -> Union[str, Generator, Iterator]:
        """Main pipeline function for handling user questions."""
        if not question:
            return "No question provided."

        # Step 1: Determine the relevant database
        selected_database = self.determine_relevant_database(question)

        if not selected_database:
            return "No matching database found for the question."

        # Step 2: Generate the SQL query
        schema = self.get_schema(selected_database)
        generate_sql_prompt = ChatPromptTemplate.from_template(
            """Generate only the SQL query to answer the user's question. Do not include any explanations, natural language responses, or other text:
                {schema}
                Question: {question}
                SQL Query:"""
        )
        formatted_sql_prompt = generate_sql_prompt.format(schema=schema, question=question)
        generated_sql = self.llm.invoke(formatted_sql_prompt).content.strip()

        # Step 3: Execute the SQL query
        query_result = self.run_query(selected_database, generated_sql)

        if isinstance(query_result, str) and "Error" in query_result:
            return f"Error executing query: {query_result}"

        # Step 4: Display results
        visualization_prompt = ChatPromptTemplate.from_template(
            """Output the following data directly as a clean table without any introductory text, explanations, or additional information:
{query_result}"""
        )
        formatted_visualization_prompt = visualization_prompt.format(query_result=query_result)
        formatted_output = self.llm.invoke(formatted_visualization_prompt).content

        return formatted_output

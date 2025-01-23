import os
from typing import List, Union, Generator, Iterator
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq

class DatabasePipeline:
    def __init__(self):
        self.name = "Database Query Pipeline"
        self.db_connections = {}
        self.llm = ChatGroq()

        # Prompts
        self.schema_matching_prompt = ChatPromptTemplate.from_template(
            """You are an intelligent assistant. Based on the following database schema, check whether this user's question belongs or relates to this schema. Respond strictly with yes or no without any explanations or additional details.
            Schema: {schema}
            Question: {question}
            Match:"""
        )

        self.generate_sql_prompt = ChatPromptTemplate.from_template(
            """Generate only the SQL query to answer the user's question. Do not include any explanations, natural language responses, or other text:
            Schema: {schema}
            Question: {question}
            SQL Query:"""
        )

        self.visualization_prompt = ChatPromptTemplate.from_template(
            """Output the following data directly as a clean table without any introductory text, explanations, or additional information:
            {query_result}"""
        )

    def init_db_connection(self):
        """Initialize database connections."""
        os.environ['GROQ_API_KEY'] = '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'
        db_uris = {
            "sys": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys',
            "chinook": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/chinook',
            "sakila": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sakila'
        }
        try:
            self.db_connections = {name: SQLDatabase.from_uri(uri) for name, uri in db_uris.items()}
            print("Database connections initialized successfully.")
        except Exception as e:
            print(f"Error initializing database connections: {e}")

    async def on_startup(self):
        """Startup tasks for the pipeline."""
        self.init_db_connection()

    async def on_shutdown(self):
        """Shutdown tasks for the pipeline."""
        for db_name, db in self.db_connections.items():
            if hasattr(db, 'engine'):
                db.engine.dispose()
        self.db_connections.clear()
        print("Database connections closed.")

    def get_schema(self, db_name: str):
        """Retrieve the schema for the specified database."""
        return self.db_connections[db_name].get_table_info()

    def determine_relevant_database(self, question: str) -> Union[str, None]:
        """Determine the relevant database for a given question."""
        for db_name in self.db_connections.keys():
            schema = self.get_schema(db_name)
            formatted_prompt = self.schema_matching_prompt.format(schema=schema, question=question)
            response = self.llm.invoke(formatted_prompt).content.strip().lower()
            if response == "yes":
                return db_name
        return None

    def run_query(self, database: str, query: str):
        """Execute a SQL query on the specified database."""
        try:
            return self.db_connections[database].run(query)
        except Exception as e:
            return f"Error executing query: {e}"

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Process user input through the pipeline."""
        print(f"Received user message: {user_message}")

        # Step 1: Determine the relevant database
        selected_database = self.determine_relevant_database(user_message)
        if not selected_database:
            return "No matching database found for the question."

        # Step 2: Generate SQL query
        schema = self.get_schema(selected_database)
        formatted_sql_prompt = self.generate_sql_prompt.format(schema=schema, question=user_message)
        generated_sql = self.llm.invoke(formatted_sql_prompt).content.strip()

        # Step 3: Execute the query
        query_result = self.run_query(selected_database, generated_sql)
        if isinstance(query_result, str) and query_result.startswith("Error"):
            return query_result

        # Step 4: Visualize the result
        visualization_prompt = self.visualization_prompt.format(query_result=query_result)
        formatted_output = self.llm.invoke(visualization_prompt).content
        return formatted_output

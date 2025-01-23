from typing import List, Union, Generator, Iterator
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
# import os

class Pipeline:
    def __init__(self):
        self.name = "Database Agent"
        # Set the environment variable for the API key
        os.environ['GROQ_API_KEY'] = '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'

        # Database URIs
        self.db_uris = {
            "sys": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/sys',
            "chinook": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/chinook',
            "sakila": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/sakila'
        }
        # Initialize connections
        self.db_connections = {name: SQLDatabase.from_uri(uri) for name, uri in self.db_uris.items()}

        # Prompts
        self.schema_matching_prompt = ChatPromptTemplate.from_template(
            """You are an intelligent assistant. Based on the following database schema, 
            check where this user's question belong or related to this schema. 
            Respond strictly with yes or no without any explanations or additional details.
            Schema:{schema}
            Question: {question}
            Match:"""
        )

        self.generate_sql_prompt = ChatPromptTemplate.from_template(
            """Generate only the SQL query to answer the user's question. 
            Do not include any explanations, natural language responses, or other text:
            {schema}
            Question: {question}
            SQL Query:"""
        )

        self.visualization_prompt = ChatPromptTemplate.from_template(
            """Output the following data directly as a clean table without any introductory text, 
            explanations, or additional information:
            {query_result}"""
        )

        # Initialize the Groq LLM
        self.llm = ChatGroq()

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown:{__name__}")

    def get_schema(self, db_name):
        # Retrieve schema for a specific database
        return self.db_connections[db_name].get_table_info()

    def determine_relevant_database(self, question):
        # Determine which database is relevant for the user's question
        for db_name in self.db_connections.keys():
            schema = self.get_schema(db_name)
            formatted_prompt = self.schema_matching_prompt.format(schema=schema, question=question)
            response = self.llm.invoke(formatted_prompt).content.strip().lower()
            normalized_response = response.replace("'", "").rstrip('.').strip()
            if normalized_response == "yes":
                return db_name
        return None

    def run_query(self, database, query):
        # Execute a SQL query
        try:
            return self.db_connections[database].run(query)
        except Exception as e:
            return str(e)

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # Main pipeline logic to handle user input and generate results
        print(f"received message from user: {user_message}")

        # Step 1: Determine the relevant database
        selected_database = self.determine_relevant_database(user_message)

        if not selected_database:
            return "No matching database found for the question."

        # Step 2: Generate the SQL query
        schema = self.get_schema(selected_database)
        formatted_sql_prompt = self.generate_sql_prompt.format(schema=schema, question=user_message)
        generated_sql = self.llm.invoke(formatted_sql_prompt).content.strip()

        # Step 3: Execute the SQL query
        query_result = self.run_query(selected_database, generated_sql)

        if isinstance(query_result, str) and "Error" in query_result:
            return f"Error executing query: {query_result}"

        # Step 4: Display the results
        formatted_visualization_prompt = self.visualization_prompt.format(query_result=query_result)
        formatted_output = self.llm.invoke(formatted_visualization_prompt).content

        return f"Generated SQL Query:\n{generated_sql}\n\nQuery Results:\n{formatted_output}"

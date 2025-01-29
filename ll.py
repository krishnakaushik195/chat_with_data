import logging
import time
from urllib.parse import urlparse
from typing import List, Union, Generator, Iterator
from langchain_community.utilities import SQLDatabase
from langchain.prompts import ChatPromptTemplate
from groq import Groq  # Ensure you have the Groq library installed

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Define database URIs
db_uris = {
    "sys": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys',
    "chinook": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/chinook',
    "sakila": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sakila',
    "world": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/world',
    "krishna": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/krishna'
}

# Dynamically extract database names
dynamic_db_names = {uri.split("/")[-1]: uri for uri in db_uris.values()}

# Initialize database connections
db_connections = {
    db_name: SQLDatabase.from_uri(uri)
    for db_name, uri in dynamic_db_names.items()
}

def run_query(database, query):
    """Run the SQL query against the specified database."""
    try:
        return db_connections[database].run(query)
    except Exception as e:
        return str(e)

class Pipeline:
    def __init__(self):
        self.name = "Database_Pipeline"
        # Initialize the Groq client with API key
        self.client = Groq(api_key="your_groq_api_key")  # Replace with your API key

    async def on_startup(self):
        print(f"on_startup: {__name__}")

    async def on_shutdown(self):
        print(f"on_shutdown: {__name__}")

    def get_schema(self, db_name):
        """Get the schema of the specified database."""
        return db_connections[db_name].get_table_info()

    def call_groq_api(self, prompt: str, model: str = "mixtral-8x7b-32768") -> str:
        """Send a request to the Groq API and return its response."""
        try:
            chat_completion = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=model
            )
            response = chat_completion.choices[0].message.content.strip()
            logging.debug(f"Groq API Response: {response}")
            return response
        except Exception as e:
            logging.error(f"Groq API Error: {e}")
            return f"Error while communicating with Groq API: {e}"

    def extract_database_and_question(self, user_question):
        """Determine the relevant database and extract the user’s question."""
        db_list = ', '.join(dynamic_db_names.keys())
        db_extraction_prompt = f"""
        Here is the user's question: "{user_question}"
        Identify the database from this list: {db_list}.
        If the database is unclear, return "None". Also, extract the actual question the user asked.
        Respond in the format: Database, Question
        """

        response = self.call_groq_api(db_extraction_prompt)
        
        if "," in response:
            db_name, extracted_question = response.split(",", 1)
            return db_name.strip(), extracted_question.strip()
        else:
            return "None", "None"

    def determine_relevant_database(self, question):
        """Determine the database related to the user’s question."""
        schema_matching_prompt_template = """You are an intelligent assistant. Based on the following database schema, determine whether this user's question is related to the schema. Respond with 'yes' or 'no' only.
        Schema:{schema}
        Question: {question}
        Match:"""
        schema_matching_prompt = ChatPromptTemplate.from_template(schema_matching_prompt_template)

        for db_name in db_connections.keys():
            schema = self.get_schema(db_name)
            formatted_prompt = schema_matching_prompt.format(schema=schema, question=question)
            response = self.call_groq_api(formatted_prompt).strip().lower()
            if response == "yes":
                return db_name
        return None

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Pipeline for processing the user's message."""
        if user_message.lower() in ["hi", "hello", "how are you", "hi, how are you?"]:
            available_databases = ', '.join(dynamic_db_names.keys())
            return f"Hi! 😊 Here’s the list of available databases:\n{available_databases}"

        print(f"Received message: {user_message}")

        # Step 1: Extract database and question
        db_name, extracted_question = self.extract_database_and_question(user_message)

        # Step 2: Handle different scenarios based on the extraction
        if db_name == "None" and extracted_question == "None":
            return "You didn't select any database. Can you specify the database, or should I answer your question separately?"
        elif db_name == "None":
            return "I detected a question but no database. Can you please specify the database?"
        elif extracted_question == "None":
            return "You selected a database but didn't ask a question. Can you please provide a question?"

        print(f"Database: {db_name}, Question: {extracted_question}")

        # Step 3: Fetch schema of the selected database
        schema = self.get_schema(db_name)

        # Step 4: Generate SQL query
        generate_sql_prompt_template = """Generate only the SQL query to answer the user's question. Do not include any explanations:
        {schema}
        Question: {question}
        SQL Query:"""
        generate_sql_prompt = ChatPromptTemplate.from_template(generate_sql_prompt_template)
        sql_query = self.call_groq_api(generate_sql_prompt.format(schema=schema, question=extracted_question))

        # Step 5: Execute query and get result
        if sql_query.strip().lower() != "no":
            db_response, execution_time = run_query_with_timing(db_name, sql_query)

            # Step 6: Format output using Groq
            visualization_prompt_template = """Format the following data as a clean table:
            {query_result}
            """
            visualization_prompt = ChatPromptTemplate.from_template(visualization_prompt_template)
            formatted_result = self.call_groq_api(visualization_prompt.format(query_result=db_response))

            return f"Database: {db_name}\nExecution Time: {execution_time:.2f} seconds\n{formatted_result}"
        else:
            return f"Database: {db_name}\nNo valid SQL query generated."

def run_query_with_timing(database, query):
    """Run the SQL query and return result with execution time."""
    start_time = time.time()
    result = run_query(database, query)
    execution_time = time.time() - start_time
    return result, execution_time

from urllib.parse import urlparse
from langchain_community.utilities import SQLDatabase
from typing import List, Union, Generator, Iterator
from groq import Groq  # Ensure the Groq library is installed
from langchain.prompts import ChatPromptTemplate
import time

# Define database URIs
db_uris = {
    "sys": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys',
    "chinook": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/chinook',
    "sakila": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sakila',
    "world": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/world',
    "krishna": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/krishna'
}

# Extract database names dynamically
dynamic_db_names = {uri.split("/")[-1]: uri for uri in db_uris.values()}

# Initialize database connections dynamically
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
        self.name = "Ur DataBase_Pipeline"
        self.client = Groq(api_key="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")

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
            return chat_completion.choices[0].message.content.strip()
        except Exception as e:
            return f"Error while communicating with Groq API: {e}"

    def extract_database(self, user_question):
        """Determine which database the user is referring to."""
        db_list = ', '.join(dynamic_db_names.keys())
        db_extraction_prompt = f"""
        Here is the user's question: "{user_question}"
        Based on this question, determine which database it belongs to from the following list: {db_list}
        Respond with only the database name without any additional text.
        """
        return self.call_groq_api(db_extraction_prompt)

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Pipeline for processing the user's message."""
        # Greeting with database list
        if user_message.lower() in ["hi", "hello", "how are you", "hi, how are you?"]:
            available_databases = ', '.join(dynamic_db_names.keys())
            return f"Hi, how are you Admin? 😊\nHere’s the list of available databases:\n{available_databases}\nPlease specify your query, including the database name."
        
        # Extract database name from the user query
        selected_db = self.extract_database(user_message)
        if selected_db not in dynamic_db_names:
            return f"Could not determine the database from your query. Please specify one from: {', '.join(dynamic_db_names.keys())}"

        print(f"Selected Database: {selected_db}")

        # Fetch the schema of the selected database
        schema = self.get_schema(selected_db)

        # Generate the SQL query
        generate_sql_prompt_template = """Generate only the SQL query to answer the user's question. Do not include any explanations, natural language responses, or other text:
        {schema}
        Question: {question}
        SQL Query:"""
        generate_sql_prompt = ChatPromptTemplate.from_template(generate_sql_prompt_template)
        combined_prompt = generate_sql_prompt.format(schema=schema, question=user_message)
        sql_query = self.call_groq_api(combined_prompt)

        # Run the SQL query and get the result
        if sql_query.strip().lower() != "no":  # Ensure it's a valid query
            db_response, execution_time = run_query_with_timing(selected_db, sql_query)

            # Format the result
            visualization_prompt_template = """Output the following data directly as a clean table without any introductory text, explanations, or additional information:
            {query_result}
            """
            visualization_prompt = ChatPromptTemplate.from_template(visualization_prompt_template)
            combined_visualization_prompt = visualization_prompt.format(query_result=db_response)
            formatted_result = self.call_groq_api(combined_visualization_prompt)

            # Return the formatted result
            return f"Selected Database: {selected_db}\nExecution Time: {execution_time:.2f} seconds\nFormatted Table:\n{formatted_result}"
        else:
            return f"Selected Database: {selected_db}\nNo valid query result generated."

def run_query_with_timing(database, query):
    """Run the query and return the result along with execution time."""
    start_time = time.time()
    result = run_query(database, query)
    execution_time = time.time() - start_time
    return result, execution_time

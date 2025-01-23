import os
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
from typing import List, Union, Generator, Iterator

# Set the environment variable for the API key
os.environ['GROQ_API_KEY'] = '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'

# Database URIs
db_uris = {
    "sys": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/sys',
    "chinook": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/chinook',
    "sakila" : 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/sakila'
}

# Initialize connections
db_connections = {name: SQLDatabase.from_uri(uri) for name, uri in db_uris.items()}

# Retrieve schemas for all databases
def get_schema(db_name):
    return db_connections[db_name].get_table_info()

# Prompts
schema_matching_prompt_template = """You are an intelligent assistant. Based on the following database schema, check where this user's question belong or related to this schema. Respond strictly with yes or no without any explanations or additional details.
Schema:{schema}
Question: {question}
Match:"""
schema_matching_prompt = ChatPromptTemplate.from_template(schema_matching_prompt_template)

generate_sql_prompt_template = """Generate only the SQL query to answer the user's question. Do not include any explanations, natural language responses, or other text:
{schema}
Question: {question}
SQL Query:"""
generate_sql_prompt = ChatPromptTemplate.from_template(generate_sql_prompt_template)

visualization_prompt_template = """Output the following data directly as a clean table without any introductory text, explanations, or additional information:
{query_result}
"""
visualization_prompt = ChatPromptTemplate.from_template(visualization_prompt_template)

# Initialize the Groq LLM
llm = ChatGroq()

# Pipeline Class to manage the workflow
class Pipeline:
    def __init__(self):
        self.name = "Groq API Example for SQL Query Generation, Execution, and Visualization"
        # Initialize the Groq client with the API key
        self.client = ChatGroq(api_key=os.getenv('GROQ_API_KEY'))

    async def on_startup(self):
        # This function is called when the server is started.
        pass

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        pass

    def determine_relevant_database(self, question: str):
        # Step 1: Determine the relevant database
        for db_name in db_connections.keys():
            schema = get_schema(db_name)
            formatted_prompt = schema_matching_prompt.format(schema=schema, question=question)
            response = llm.invoke(formatted_prompt).content.strip().lower()
            normalized_response = response.replace("'", "").rstrip('.').strip()
            if normalized_response == "yes":
                return db_name
        return None

    def run_query(self, database: str, query: str):
        # Step 2: Execute the SQL query
        try:
            return db_connections[database].run(query)
        except Exception as e:
            return str(e)

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # Step 1: Determine the relevant database
        selected_database = self.determine_relevant_database(user_message)

        if not selected_database:
            return "No matching database found for the question."

        # Step 2: Generate the SQL query
        schema = get_schema(selected_database)
        formatted_sql_prompt = generate_sql_prompt.format(schema=schema, question=user_message)
        generated_sql = llm.invoke(formatted_sql_prompt).content.strip()

        # Step 3: Execute the SQL query
        query_result = self.run_query(selected_database, generated_sql)

        if isinstance(query_result, str) and "Error" in query_result:
            return f"Error executing query: {query_result}"

        # Step 4: Format the database result into a clean table
        formatted_visualization_prompt = visualization_prompt.format(query_result=query_result)
        formatted_output = llm.invoke(formatted_visualization_prompt).content

        # Return the structured response with both SQL and formatted output
        return {
            "SQL Query": generated_sql,
            "Formatted Database Response": formatted_output
        }

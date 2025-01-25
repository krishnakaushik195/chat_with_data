from langchain_community.utilities import SQLDatabase
from typing import List, Union, Generator, Iterator
from groq import Groq  # Ensure the Groq library is installed
from langchain.prompts import ChatPromptTemplate

# Simulated db_connections object for database access
db_uris = {
    "sys": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys',
    "chinook": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/chinook',
    "sakila": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sakila'
}

db_connections = {db_name: SQLDatabase.from_uri(uri) for db_name, uri in db_uris.items()}

schema_matching_prompt_template = """You are an intelligent assistant. Based on the following database schema, check where this user's question belong or related to this schema. Respond strictly with yes or no without any explanations or additional details.
Schema:{schema}
Question: {question}
Match:"""
schema_matching_prompt = ChatPromptTemplate.from_template(schema_matching_prompt_template)

def run_query(database: str, query: str):
    """Execute the query against the specified database and return the result."""
    try:
        return db_connections[database].run(query)
    except Exception as e:
        return str(e)

class Pipeline:
    def __init__(self):
        self.name = "Database agent"
        # Initialize the Groq client with the hardcoded API key
        self.client = Groq(api_key="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")
        self.selected_database = None

    async def on_startup(self):
        """This function is called when the server starts."""
        print(f"on_startup: {__name__}")

    async def on_shutdown(self):
        """This function is called when the server shuts down."""
        print(f"on_shutdown: {__name__}")

    def get_schema(self, database: str) -> str:
        """Fetch schema information from the specified database."""
        try:
            db = db_connections[database]
            schema = db.get_table_info()
            return schema
        except Exception as e:
            return f"Error fetching schema: {e}"

    def call_groq_api(self, prompt: str, model: str = "mixtral-8x7b-32768") -> str:
        """Send a request to the Groq API and return its response."""
        try:
            chat_completion = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=model
            )
            return chat_completion.choices[0].message.content
        except Exception as e:
            return f"Error while communicating with Groq API: {e}"

    def determine_relevant_database(self, question: str) -> Union[str, None]:
        """Determine which database the question is relevant to."""
        for db_name in db_connections.keys():
            schema = self.get_schema(db_name)
            if "Error" in schema:
                continue
            formatted_prompt = schema_matching_prompt.format(schema=schema, question=question)
            response = self.call_groq_api(formatted_prompt).strip().lower()
            normalized_response = response.replace("'", "").rstrip('.').strip()
            if normalized_response == "yes":
                return db_name
        return None

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Handle incoming user messages and process them."""
        try:
            print(f"Received message from user: {user_message}")  # Log user message

            # Prompt user to select a database if not already selected
            if not self.selected_database:
                relevant_db = self.determine_relevant_database(user_message)
                if relevant_db:
                    self.selected_database = relevant_db
                else:
                    return "Please specify which database you want to use: sys, chinook, or sakila."

            # Fetch the schema for the selected database
            schema = self.get_schema(self.selected_database)
            if "Error" in schema:
                return schema

            # Define the SQL prompt template
            generate_sql_prompt_template = """Generate only the SQL query to answer the user's question. Do not include any explanations, natural language responses, or other text:
            {schema}
            Question: {question}
            SQL Query:"""

            # Create the prompt using the schema and user message
            generate_sql_prompt = ChatPromptTemplate.from_template(generate_sql_prompt_template)

            # Combine the schema and user message to generate the final prompt
            combined_prompt = generate_sql_prompt.format(schema=schema, question=user_message)

            # Call the Groq API with the combined prompt
            groq_response = self.call_groq_api(combined_prompt)
            if "Error" in groq_response:
                return groq_response

            # Execute the SQL query if valid
            if groq_response.strip().lower() != "no":
                db_response = run_query(self.selected_database, groq_response)

                # Define the visualization prompt template
                visualization_prompt_template = """Output the following data directly as a clean table without any introductory text, explanations, or additional information:
                {query_result}
                """

                # Create the prompt using the query result
                visualization_prompt = ChatPromptTemplate.from_template(visualization_prompt_template)

                # Combine the query result with the visualization prompt
                combined_visualization_prompt = visualization_prompt.format(query_result=db_response)

                # Get the clean table formatted output from Groq
                formatted_result = self.call_groq_api(combined_visualization_prompt)
                if "Error" in formatted_result:
                    return formatted_result

                # Return both the SQL query and the formatted table result
                return f"Formatted Table: {formatted_result}"
            else:
                return "No valid SQL query generated."
        except Exception as e:
            return f"An error occurred: {e}"

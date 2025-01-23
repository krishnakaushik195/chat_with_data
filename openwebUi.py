from langchain_community.utilities import SQLDatabase
from typing import List, Union, Generator, Iterator
from groq import Groq  # Ensure the Groq library is installed
from langchain.prompts import ChatPromptTemplate

# Simulated db_connections object for database access
db_connections = {
    'your_database': SQLDatabase.from_uri('mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys')
}

def run_query(database, query):
    try:
        # Execute the query against the specified database and return the result
        return db_connections[database].run(query)
    except Exception as e:
        return str(e)

class Pipeline:
    def __init__(self):
        self.name = "Groq API Example for SQL Query Generation, Execution, and Visualization"
        # Initialize the Groq client with the new API key
        self.client = Groq(api_key="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")

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup: {__name__}")

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown: {__name__}")

    def get_schema(self):
        # Define the MySQL URI
        mysql_uri = 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys'
        # Create a SQLDatabase object using the URI
        db = SQLDatabase.from_uri(mysql_uri)
        # Fetch schema information
        schema = db.get_table_info()
        return schema

    def call_groq_api(self, prompt: str, model: str = "mixtral-8x7b-32768") -> str:
        """Send a request to the Groq API and return its response."""
        try:
            # Call the Groq API for a chat completion
            chat_completion = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=model
            )
            # Extract and return the content of the first choice (SQL query)
            return chat_completion.choices[0].message.content
        except Exception as e:
            return f"Error while communicating with Groq API: {e}"

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        
        print(f"received message from user: {user_message}")  # Log user_message

        # Fetch the schema
        schema = self.get_schema()

        # Define the generate SQL prompt template
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
        
        # Now, execute the SQL query using the run_query function
        if groq_response.strip().lower() != "no":  # If the response is not "No", it's a valid query
            db_response = run_query('your_database', groq_response)
            
            # Directly format the database response as a structured table
            visualization_prompt_template = """Output the following data directly as a clean table without any introductory text, explanations, or additional information:
            {query_result}
            """
            visualization_prompt = ChatPromptTemplate.from_template(visualization_prompt_template)
            
            # Format the database response using the visualization prompt
            formatted_response = visualization_prompt.format(query_result=db_response)
            
            # Return both the SQL query and the formatted database response
            return f"Generated SQL Query: {groq_response}\nFormatted Database Response:\n{formatted_response}"
        else:
            return "No valid SQL query generated."

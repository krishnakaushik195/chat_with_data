from langchain_community.utilities import SQLDatabase
from typing import List, Union, Generator, Iterator
from groq import Groq  # Ensure the Groq library is installed
from langchain.prompts import ChatPromptTemplate

class Pipeline:
    def __init__(self):
        self.name = "Groq API Example with Schema Matching for SQL"
        # Initialize the Groq client with the hardcoded API key
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

        # Define the schema matching prompt template
        schema_matching_prompt_template = """You are an intelligent assistant. Based on the following database schema, check where this user's question belongs or relates to this schema. Respond strictly with yes or no without any explanations or additional details.
        Schema: {schema}
        Question: {question}
        Match:"""
        
        # Create the schema matching prompt
        schema_matching_prompt = ChatPromptTemplate.from_template(schema_matching_prompt_template)

        # Combine the schema and user message to generate the final prompt
        combined_prompt = schema_matching_prompt.format(schema=schema, question=user_message)

        # Call the Groq API with the combined prompt
        groq_response = self.call_groq_api(combined_prompt)
        
        # Return the Groq response (SQL query)
        return f"Generated SQL Query: {groq_response}"


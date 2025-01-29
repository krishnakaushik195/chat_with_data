from typing import List, Union
from langchain_community.utilities import SQLDatabase
from groq import Groq  # Ensure the Groq library is installed
from langchain.prompts import ChatPromptTemplate
import time

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        
        # Define database URIs
        self.db_uris = {
            "sys": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys',
            "chinook": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/chinook',
            "sakila": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sakila',
            "world": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/world',
            "krishna": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/krishna',
            "db_info1": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/db_info1'
        }
        
        # Dynamically extract the database names
        self.dynamic_db_names = {uri.split("/")[-1]: uri for uri in self.db_uris.values()}
        
        # Initialize database connections
        self.db_connections = {
            db_name: SQLDatabase.from_uri(uri)
            for db_name, uri in self.db_uris.items()
        }

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown:{__name__}")
        pass

    def call_groq_api(self, prompt: str, api_key: str = "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", model: str = "mixtral-8x7b-32768") -> str:
        """Send a request to the Groq API and return its response."""
        try:
            groq_client = Groq(api_key=api_key)  # Groq client initialized with the provided API key
            # Send the prompt to the Groq API (this assumes the client setup is correct)
            chat_completion = groq_client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=model
            )
            return chat_completion.choices[0].message.content.strip()
        except Exception as e:
            return f"Error while communicating with Groq API: {e}"

    def extract_database_and_question(self, user_question, api_key: str = "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"):
        """Determine which database the user is referring to and extract the question."""
        db_list = ', '.join(self.dynamic_db_names.keys())
        
        # Construct the prompt
        db_extraction_prompt = f"""
        Here is the user's question: "{user_question}"
        Based on this question, determine which database it belongs to from the following list: {db_list}.
        If you cannot determine the database, return "None". Also, provide the question the user asked without modification.
        Respond with the database name and the user's question, separated by a comma. If no database is determined, return "None" for both.
        """
        
        # Send the prompt to Groq API and get the response
        response = self.call_groq_api(db_extraction_prompt, api_key)
        
        print(f"Raw Groq response: {response}")  # This is the point where we get the first LLM output
        return response

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict, api_key: str = "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") -> Union[str, List[dict]]:
        """Pipeline for processing the user's message."""
        
        print(f"received message from user: {user_message}")  # user_message to logs
        
        # Extract database and question
        response = self.extract_database_and_question(user_message, api_key)
        
        # Parse the response
        if response:
            # Split the response into database and question
            db_name, question = response.split(",", 1) if "," in response else ("None", "None")
            return f"Database: {db_name}\nQuestion: {question.strip()}"
        
        return "Could not determine the database or question."

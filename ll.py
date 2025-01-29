from langchain_community.utilities import SQLDatabase
from typing import List, Union
from groq import Groq  # Ensure the Groq library is installed
from langchain.prompts import ChatPromptTemplate
import time

class Pipeline:
    def __init__(self):
        # Define database URIs
        self.db_uris = {
            "sys": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys',
            "chinook": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/chinook',
            "sakila": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sakila',
            "world": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/world',
            "krishna": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/krishna',
            "db_info1": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/db_info1'
        }

        # Extract database names dynamically
        self.dynamic_db_names = {uri.split("/")[-1]: uri for uri in self.db_uris.values()}

    def call_groq_api(self, prompt: str, model: str = "mixtral-8x7b-32768") -> str:
        """Send a request to the Groq API and return its response."""
        try:
            # Simulate sending to Groq API
            print(f"Sending to Groq: {prompt}")
            # Example mock response from Groq API (you will replace this with actual API call)
            return "chinook, how many columns in that database?"
        except Exception as e:
            return f"Error while communicating with Groq API: {e}"

    def extract_database_and_question(self, user_question):
        """Determine which database the user is referring to and extract the question."""
        db_list = ', '.join(self.dynamic_db_names.keys())
        
        # Construct the prompt
        db_extraction_prompt = f"""
        Here is the user's question: "{user_question}"
        Based on this question, determine which database it belongs to from the following list: {db_list}.
        If you cannot determine the database, return "None". Also, provide the question the user asked without modification.
        Respond with the database name and the user's question, separated by a comma. If no database is determined, return "None" for both.
        """
        
        # Send the prompt to Groq API and get the response (for now, we mock the response)
        response = self.call_groq_api(db_extraction_prompt)
        
        print(f"Raw Groq response: {response}")  # This is the point where we get the first LLM output
        return response

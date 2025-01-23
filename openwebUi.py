import os
import requests
from typing import List, Union, Generator, Iterator

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        self.groq_api_key = os.getenv("GROQ_API_KEY", "default_key")
        self.groq_api_url = "https://api.groq.com/v1/execute"  # Replace with the correct endpoint if needed

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup: {__name__}")

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown: {__name__}")

    def call_groq_api(self, user_message: str) -> str:
        """Send a request to the Groq API and return its response."""
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "query": user_message  # Assuming Groq API expects a "query" field
        }
        try:
            response = requests.post(self.groq_api_url, json=payload, headers=headers)
            response.raise_for_status()  # Raise an error for bad HTTP status codes
            return response.json().get("response", "No response from Groq API.")  # Adjust based on Groq API's actual response structure
        except requests.exceptions.RequestException as e:
            return f"Error while communicating with Groq API: {e}"

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        print(f"received message from user: {user_message}")  # Log user_message

        # Call the Groq API and return its response
        groq_response = self.call_groq_api(user_message)
        return f"Groq API response: {groq_response}"  # Response to the UI

from typing import List, Union, Generator, Iterator
from groq import Groq

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        self.client = Groq(api_key="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")
    
    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup: {__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown: {__name__}")
        pass

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user message is received.
        print(f"Received message from user: {user_message}")  # Log user message
        
        # Here you can interact with the Groq API to send the user message and get a response
        response = self.get_groq_response(user_message)
        
        # Return the Groq model's response back to the UI
        return f"Response from Groq: {response}"
    
    def get_groq_response(self, user_message: str) -> str:
        # Send user message to Groq API and get a response
        try:
            response = self.client.query(user_message)
            return response.get("choices", [{}])[0].get("message", {}).get("content", "No response")
        except Exception as e:
            return f"Error querying Groq: {e}"

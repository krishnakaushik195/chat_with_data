from typing import List, Union, Generator, Iterator
from groq import Groq

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        # Set up the Groq client with your API key
        self.groq_client = Groq("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")
    
    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown:{__name__}")
        pass

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        
        print(f"received message from user: {user_message}")  # Log the user message
        
        # Generate a response based on the user's message
        # For simplicity, we're sending the user message directly to Groq API
        response = self.groq_client.query(user_message)  # Using the message directly
        
        print(f"Groq response: {response}")
        
        # Return the Groq response as the result
        return f"Groq response: {response}"


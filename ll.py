from groq import Groq
from typing import List, Union, Generator, Iterator

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        self.client = Groq(
            api_key="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",  # Direct API key
        )
        
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
        
        # Create chat completion
        chat_completion = self.client.chat.completions.create(
            messages=messages,
            model=model_id,
        )
        
        # Return the response message
        response_message = chat_completion.choices[0].message.content
        return response_message  # Send the response to the UI

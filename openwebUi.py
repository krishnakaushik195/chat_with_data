import os
from typing import List, Union
from pydantic import BaseModel
from groq import Groq

class Pipeline:
    class Valves(BaseModel):
        GROQ_API_URL: str
        GROQ_API_KEY: str

    def __init__(self):
        self.name = "Groq API Pipeline"
        self.valves = self.Valves(
            **{
                "GROQ_API_URL": os.getenv("GROQ_API_URL", "https://api.groq.com/openai/v1"),
                "GROQ_API_KEY": os.getenv("GROQ_API_KEY", "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"),
            }
        )

        # Initialize Groq client
        self.client = Groq(api_key=self.valves.GROQ_API_KEY)

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown:{__name__}")

    async def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, None]:
        # This function is called when a new user_message is received.
        print(f"received message from user: {user_message}")  # Log user_message

        # Create chat completion using Groq client
        chat_completion = self.client.chat.completions.create(
            messages=messages,
            model=model_id,
        )

        # Return the content of the response or an error message
        if chat_completion.choices:
            return chat_completion.choices[0].message.content
        else:
            return "Error: No choices received in the response."

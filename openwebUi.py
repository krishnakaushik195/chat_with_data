from typing import List, Union
import aiohttp
from pydantic import BaseModel

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

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown:{__name__}")

    async def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        print(f"received message from user: {user_message}")  # Log user_message

        headers = {
            "Authorization": f"Bearer {self.valves.GROQ_API_KEY}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": model_id,
            "messages": messages
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(self.valves.GROQ_API_URL, json=payload, headers=headers) as response:
                if response.status == 200:
                    result = await response.json()
                    return result.get("choices", [{}])[0].get("message", {}).get("content", "")
                else:
                    return f"Error: Received status code {response.status}"

from typing import List, Union, Generator, Iterator
from langchain.chat_models import ChatGroq
from langchain.schema import AIMessage, HumanMessage

class Pipeline:
    def __init__(self):
        self.name = "Groq Conversational Chain"
        self.model = ChatGroq(model_name="mixtral-8x7b-32768", api_key="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")
        
    async def on_startup(self):
        print(f"on_startup: {__name__}")
        
    async def on_shutdown(self):
        print(f"on_shutdown: {__name__}")
    
    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        print(f"Received message from user: {user_message}")
        
        # Prepare conversation history
        chat_history = [HumanMessage(content=msg["content"]) if msg["role"] == "user" else AIMessage(content=msg["content"]) for msg in messages]
        
        # Append new user message
        chat_history.append(HumanMessage(content=user_message))
        
        # Get response from Groq model
        response = self.model(chat_history)
        
        return response.content

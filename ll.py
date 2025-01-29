from typing import List, Union, Generator, Iterator
from groq import Groq
from langchain.chains import ConversationChain
from langchain.chains.conversation.memory import ConversationBufferWindowMemory
from langchain_groq import ChatGroq

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        # Initialize the Groq client with the provided API key
        self.groq_client = Groq(api_key="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")

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
        
        print(f"received message from user: {user_message}")  # user_message to logs
        
        # Set up conversational memory with the specified length
        conversational_memory_length = body.get("conversational_memory_length", 5)
        memory = ConversationBufferWindowMemory(k=conversational_memory_length)

        # Initialize Groq LangChain chat object
        groq_chat = ChatGroq(
            groq_api_key="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",
            model_name=model_id  # Use the model_id passed in the parameters
        )

        # Set up the conversation chain with the memory and Groq model
        conversation = ConversationChain(
            llm=groq_chat,
            memory=memory
        )

        # Get the response from the Groq API based on the user message
        response = conversation(user_message)

        # Return the AI's response
        return f"Groq API response: {response['response']}"

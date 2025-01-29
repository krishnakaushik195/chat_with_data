from typing import List, Union, Generator, Iterator
from groq import Groq  # Import the Groq package

class Pipeline:
    def __init__(self):
        self.name = "Conversational ChatBot with Memory"
        # Initialize the Groq client
        self.client = Groq(api_key="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")
        self.memory = []  # List to store conversation history

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup: {__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown: {__name__}")
        pass

    def query_groq_database(self, query: str) -> str:
        """Query the Groq database and return results."""
        try:
            response = self.client.query(query)
            if response['status'] == 'success':
                return f"Query result: {response['data']}"
            else:
                return "Error: Query failed, check the database structure or query."
        except Exception as e:
            return f"Error: {str(e)}"

    def generate_sql_query(self, question: str) -> str:
        """Generate SQL query based on the user's question."""
        if "total sales" in question.lower():
            return "SELECT SUM(amount) FROM sales"
        else:
            return f"SELECT * FROM database WHERE question='{question}'"

    def update_memory(self, user_message: str, bot_response: str):
        """Update memory with the latest user and bot messages."""
        self.memory.append({"user": user_message, "bot": bot_response})

    def get_context(self) -> str:
        """Get the memory context for conversation continuity."""
        context = ""
        for interaction in self.memory[-5:]:  # Limit to the last 5 exchanges
            context += f"User: {interaction['user']}\nBot: {interaction['bot']}\n"
        return context

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Process the user message and generate a bot response."""
        print(f"Received message: {user_message}")  # Log the incoming message

        # Get conversation context from memory
        context = self.get_context()

        # Generate SQL query if needed
        if "query" in user_message.lower() or "sales" in user_message.lower():
            query = self.generate_sql_query(user_message)
            db_response = self.query_groq_database(query)
            bot_response = f"Here are the results for your query: {db_response}"
        else:
            bot_response = "I'm here to help! How can I assist you today?"

        # Update memory with the latest user and bot messages
        self.update_memory(user_message, bot_response)

        # Return response along with context (optional)
        return f"{bot_response}\n\n(Conversation context: \n{context})"


from groq import Groq

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        # Set up the Groq client with your API key
        self.groq_client = Groq("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")
    
    def pipe(self, user_message: str) -> str:
        # Send the user message to Groq API and get a response
        print(f"Received message from user: {user_message}")
        
        # Send message to Groq API and get a response
        response = self.groq_client.query(user_message)
        
        # Print and return the response
        print(f"Groq API response: {response}")
        
        return f"Groq API response: {response}"

import requests
from typing import List, Union, Generator, Iterator

class Pipeline:
    def __init__(self):
        self.name = "Groq Question Pipeline"
        self.api_key = "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"
        self.url = "https://api.groq.com/openai/v1/completions"

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
        
        print(f"Received message from user: {user_message}")  # user_message to logs
        
        # Define the headers and body for the POST request
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # Prepare the data for the POST request
        data = {
            "model": model_id,  # You can customize the model here
            "messages": [{"role": "user", "content": user_message}]
        }

        # Send the request to the Groq API
        response = requests.post(self.url, json=data, headers=headers)

        if response.status_code == 200:
            response_json = response.json()
            # Extract the reply from the response
            ai_response = response_json["choices"][0]["message"]["content"]
            print(f"AI response: {ai_response}")  # AI response to logs
            return ai_response  # Return the AI's response
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return f"Error: {response.status_code}, {response.text}"  # Return error message if there's an issue

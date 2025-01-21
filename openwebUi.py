import requests
from typing import List, Union

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"

    async def on_startup(self):
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        print(f"on_shutdown:{__name__}")
        pass

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str]:
        # This function is called when a new user_message is received.
        
        print(f"received message from user: {user_message}")  # user_message to logs
        
        # Now, send the user message to the GOUQ API and get the response
        response = self.query_gouq_api(user_message)
        
        return response

    def query_gouq_api(self, user_message: str) -> str:
        # Set the GOUQ API URL and API key
        url = "https://api.gouq.com/v1/query"  # Replace with the actual GOUQ API URL
        api_key = "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"
        
        # Prepare the payload with the user message
        payload = {
            "query": user_message
        }
        
        # Set up the headers, including the API key for authorization
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        # Send a POST request to the GOUQ API
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()  # Check for HTTP errors
            
            # Extract the result from the API response
            result = response.json()
            
            # You can adjust this depending on how the response is structured
            return f"GOUQ API Response: {result.get('data', 'No data found')}"
        
        except requests.exceptions.RequestException as e:
            return f"Error while connecting to GOUQ API: {str(e)}"

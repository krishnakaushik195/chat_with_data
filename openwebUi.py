from typing import List, Union, Generator, Iterator

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        self.initial_greeting = True  # Track if it's the first interaction

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup: {__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown: {__name__}")
        pass

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # Check if this is the first interaction
        if self.initial_greeting:
            self.initial_greeting = False  # Reset after the first message
            return "Hi, how are you?"

        # After the greeting, process user input normally
        print(f"received message from user: {user_message}")  # Log user_message
        if "select database" in user_message.lower():
            return "Which database would you like to use? (e.g., Chinook)"
        
        return f"Received message from user: {user_message}"  # Respond to the UI

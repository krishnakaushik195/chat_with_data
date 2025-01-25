from typing import List, Union, Generator, Iterator

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"
        self.initial_greeting = True  # Tracks if this is the first interaction

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup: {__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown: {__name__}")
        pass

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # if self.initial_greeting:
        #     self.initial_greeting = False  # Disable the greeting after the first interaction
        return "Hi, how are you?"

        # Echo back the user message after the greeting
        print(f"received message from user: {user_message}")  # Log user_message
        return f"You said: {user_message}"

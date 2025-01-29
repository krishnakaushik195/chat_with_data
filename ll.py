from typing import List, Union, Generator, Iterator
from groq import Groq

class Pipeline:
    def __init__(self):
        self.name = "Ur DataBase_Pipeline"
        # Initialize the Groq client
        self.client = Groq(api_key="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")
        self.db_list = ["database_1", "database_2", "database_3", "database_4"]
        self.selected_db = None  # Store the last detected database if needed
        self.conversation_history = []  # Store conversation history
    
    async def on_startup(self):
        print(f"on_startup: {__name__}")
    
    async def on_shutdown(self):
        print(f"on_shutdown: {__name__}")
    
    def determine_db_and_question(self, user_message: str):
        prompt = (f"Here is the user message: \"{user_message}\"\n"
                  f"Based on this question, determine which database it belongs to from the following list: {self.db_list}.\n"
                  f"Respond with only the database name without any additional text.\n"
                  f"Also, extract the question from the message if it exists; otherwise, return 'None'.")
        
        response = self.client.chat.completions.create(
            model="gpt-4",  # Ensure you specify the correct model
            messages=[{"role": "system", "content": prompt}]
        )
        reply = response.choices[0].message.content.strip()
        
        db_name, question = reply.split("\n") if "\n" in reply else (reply, "None")
        db_name, question = db_name.strip(), question.strip()
        
        return db_name if db_name in self.db_list else "None", question
    
    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        db_name, question = self.determine_db_and_question(user_message)
        
        self.conversation_history.append({"user": user_message, "db": db_name, "question": question})
        
        return f"Database: {db_name}, Question: {question}"

from langchain_community.utilities import SQLDatabase
from typing import List, Union
from groq import Groq  # Ensure the Groq library is installed
from langchain.prompts import ChatPromptTemplate

# Define database URIs
db_uris = {
    "sys": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys',
    "chinook": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/chinook',
    "sakila": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sakila'
}

# Initialize database connections
db_connections = {
    db_name: SQLDatabase.from_uri(uri)
    for db_name, uri in db_uris.items()
}

# Function to execute SQL query
def run_query(database, query):
    try:
        return db_connections[database].run(query)
    except Exception as e:
        return str(e)

class ChatDatabaseAgent:
    def __init__(self):
        self.name = "Conversational Chatbot & Database Agent"
        self.client = Groq(api_key="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")
        self.available_databases = list(db_uris.keys())
        self.selected_database = None  # For manual selection

    async def on_startup(self):
        print(f"Welcome to the Database Agent!")
        print(f"Available Databases: {', '.join(self.available_databases)}")
        print("You can either:")
        print("1. Select a database manually (e.g., 'Select sakila').")
        print("2. Let me automatically determine the best database for your question.")

    async def on_shutdown(self):
        print("Shutting down the agent. See you next time!")

    def respond_to_greeting(self, user_message: str) -> str:
        """Handle basic conversational queries."""
        greetings = ["hi", "hello", "hey", "how are you", "what's up"]
        if any(greet in user_message.lower() for greet in greetings):
            return "Hello! How can I assist you today?"
        elif "thank you" in user_message.lower():
            return "You're welcome! 😊"
        elif "bye" in user_message.lower():
            return "Goodbye! Have a great day! 👋"
        return None  # Not a greeting

    def get_schema(self, db_name):
        """Fetch the schema of the selected database."""
        return db_connections[db_name].get_table_info()

    def call_groq_api(self, prompt: str, model: str = "mixtral-8x7b-32768") -> str:
        """Send a request to the Groq API and return its response."""
        try:
            chat_completion = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model=model
            )
            return chat_completion.choices[0].message.content
        except Exception as e:
            return f"Error while communicating with Groq API: {e}"

    def determine_relevant_database(self, question):
        """Determine which database is relevant to the user's question."""
        schema_matching_prompt_template = """You are an intelligent assistant. Based on the following database schema, check where this user's question belong or related to this schema. Respond strictly with yes or no without any explanations or additional details.
        Schema:{schema}
        Question: {question}
        Match:"""
        schema_matching_prompt = ChatPromptTemplate.from_template(schema_matching_prompt_template)

        for db_name in db_connections.keys():
            schema = self.get_schema(db_name)
            formatted_prompt = schema_matching_prompt.format(schema=schema, question=question)
            response = self.call_groq_api(formatted_prompt).strip().lower()
            if response == "yes":
                return db_name
        return None

    def handle_database_query(self, user_message: str) -> str:
        """Handle database-related queries."""
        if self.selected_database:
            db_name = self.selected_database
        else:
            db_name = self.determine_relevant_database(user_message)

        if not db_name:
            return "I'm unable to determine the relevant database for your question."

        # Notify user about database selection
        print(f"Selected Database: {db_name}")

        # Fetch the schema and generate SQL
        schema = self.get_schema(db_name)
        generate_sql_prompt_template = """Generate only the SQL query to answer the user's question. Do not include any explanations, natural language responses, or other text:
        {schema}
        Question: {question}
        SQL Query:"""
        generate_sql_prompt = ChatPromptTemplate.from_template(generate_sql_prompt_template)
        combined_prompt = generate_sql_prompt.format(schema=schema, question=user_message)
        sql_query = self.call_groq_api(combined_prompt)

        # Run SQL query
        if sql_query.strip().lower() != "no":
            db_response = run_query(db_name, sql_query)

            # Visualize result
            visualization_prompt_template = """Output the following data directly as a clean table without any introductory text, explanations, or additional information:
            {query_result}
            """
            visualization_prompt = ChatPromptTemplate.from_template(visualization_prompt_template)
            formatted_visualization = self.call_groq_api(
                visualization_prompt.format(query_result=db_response)
            )

            return f"Selected Database: {db_name}\n{formatted_visualization}"
        else:
            return f"Selected Database: {db_name}\nNo valid query result generated."

    def pipe(self, user_message: str):
        """Main pipeline to process user messages."""
        greeting_response = self.respond_to_greeting(user_message)
        if greeting_response:
            return greeting_response

        if user_message.lower().startswith("select"):
            # Handle manual database selection
            db_name = user_message.split(" ", 1)[1].strip().lower()
            if db_name in self.available_databases:
                self.selected_database = db_name
                return f"Database '{db_name}' selected! You can now ask queries specific to this database."
            else:
                return f"Invalid database name. Available databases: {', '.join(self.available_databases)}."

        # Handle database-related queries
        return self.handle_database_query(user_message)

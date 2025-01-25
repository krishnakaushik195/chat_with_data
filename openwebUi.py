from langchain_community.utilities import SQLDatabase
from typing import List, Union, Generator, Iterator
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

def run_query(database, query):
    """Run the SQL query against the specified database."""
    try:
        return db_connections[database].run(query)
    except Exception as e:
        return str(e)

class Pipeline:
    def __init__(self):
        self.name = "Multi-Database Groq API Pipeline"
        # Initialize the Groq client
        self.client = Groq(api_key="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")

    async def on_startup(self):
        print(f"on_startup: {__name__}")

    async def on_shutdown(self):
        print(f"on_shutdown: {__name__}")

    def get_schema(self, db_name):
        """Get the schema of the specified database."""
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

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Pipeline for processing the user's message."""
        print(f"received message from user: {user_message}")

        # Step 1: Determine the relevant database
        relevant_database = self.determine_relevant_database(user_message)
        if not relevant_database:
            return "Unable to determine a relevant database for the user's question."

        # Notify about the selected database
        print(f"Selected Database: {relevant_database}")

        # Step 2: Fetch the schema of the selected database
        schema = self.get_schema(relevant_database)

        # Step 3: Generate the SQL query
        generate_sql_prompt_template = """Generate only the SQL query to answer the user's question. Do not include any explanations, natural language responses, or other text:
        {schema}
        Question: {question}
        SQL Query:"""
        generate_sql_prompt = ChatPromptTemplate.from_template(generate_sql_prompt_template)
        combined_prompt = generate_sql_prompt.format(schema=schema, question=user_message)
        sql_query = self.call_groq_api(combined_prompt)

        # Step 4: Run the SQL query and get the result
        if sql_query.strip().lower() != "no":  # Ensure it's a valid query
            db_response = run_query(relevant_database, sql_query)

            # Step 5: Visualize the query result
            visualization_prompt_template = """Output the following data directly as a clean table without any introductory text, explanations, or additional information:
            {query_result}
            """
            visualization_prompt = ChatPromptTemplate.from_template(visualization_prompt_template)
            combined_visualization_prompt = visualization_prompt.format(query_result=db_response)
            formatted_result = self.call_groq_api(combined_visualization_prompt)

            # Step 6: Return the database name, SQL query, and formatted result
            return f"Selected Database: {relevant_database}\nGenerated SQL Query: {sql_query}\nFormatted Table:\n{formatted_result}"
        else:
            return f"Selected Database: {relevant_database}\nNo valid SQL query generated."

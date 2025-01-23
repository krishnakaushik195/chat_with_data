#from typing import List, Union, Generator, Iterator
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
import os

class Pipeline:
    def __init__(self):
        self.name = "00 Database Agent"
        self.db_uris = {
            "sys": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/sys',
            "chinook": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/chinook',
            "sakila": 'mysql+mysqlconnector://root:Krishna%40195@localhost:3306/sakila'
        }
        self.db_connections = {name: SQLDatabase.from_uri(uri) for name, uri in self.db_uris.items()}
        os.environ['GROQ_API_KEY'] = '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'
        self.llm = ChatGroq()
        
        # Prompts
        self.schema_matching_prompt = ChatPromptTemplate.from_template("""You are an intelligent assistant. Based on the following database schema, check where this user's question belongs or relates to this schema. Respond strictly with yes or no without any explanations or additional details.
        Schema:{schema}
        Question: {question}
        Match:""")
        
        self.generate_sql_prompt = ChatPromptTemplate.from_template("""Generate only the SQL query to answer the user's question. Do not include any explanations, natural language responses, or other text:
        {schema}
        Question: {question}
        SQL Query:""")
        
        self.visualization_prompt = ChatPromptTemplate.from_template("""Output the following data directly as a clean table without any introductory text, explanations, or additional information:
        {query_result}
        """)

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shut down.
        print(f"on_shutdown:{__name__}")
        pass

    def get_schema(self, db_name: str):
        """Retrieve the schema for the specified database."""
        return self.db_connections[db_name].get_table_info()

    def determine_relevant_database(self, question: str) -> Union[str, None]:
        """Identify the database schema related to the user's question."""
        for db_name in self.db_connections.keys():
            schema = self.get_schema(db_name)
            formatted_prompt = self.schema_matching_prompt.format(schema=schema, question=question)
            response = self.llm.invoke(formatted_prompt).content.strip().lower()
            normalized_response = response.replace("'", "").rstrip('.').strip()
            if normalized_response == "yes":
                return db_name
        return None

    def run_query(self, database: str, query: str):
        """Run the generated SQL query on the selected database."""
        try:
            return self.db_connections[database].run(query)
        except Exception as e:
            return str(e)

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        print(f"received message from user: {user_message}")  # user_message to logs

        # Step 1: Determine the relevant database
        selected_database = self.determine_relevant_database(user_message)
        if not selected_database:
            return "No matching database found for the question."

        # Step 2: Generate the SQL query
        schema = self.get_schema(selected_database)
        formatted_sql_prompt = self.generate_sql_prompt.format(schema=schema, question=user_message)
        generated_sql = self.llm.invoke(formatted_sql_prompt).content.strip()

        # Step 3: Execute the SQL query
        query_result = self.run_query(selected_database, generated_sql)
        if isinstance(query_result, str) and "Error" in query_result:
            return f"Error executing query: {query_result}"

        # Step 4: Format the query results
        formatted_visualization_prompt = self.visualization_prompt.format(query_result=query_result)
        formatted_output = self.llm.invoke(formatted_visualization_prompt).content

        return formatted_output

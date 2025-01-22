import os
from typing import Optional, Any, Dict, List, Union, Generator, Iterator
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq

class Pipeline:
    def __init__(self):
        self.name = "Database Query Pipeline"
        # Set the environment variable for the API key
        os.environ['GROQ_API_KEY'] = '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'

        # Database URIs
        self.db_uris = {
            "sys": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sys',
            "chinook": 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/chinook',
            "sakila" : 'mysql+mysqlconnector://root:Krishna%40195@host.docker.internal:3306/sakila'
        }

        # Initialize connections
        self.db_connections = {name: SQLDatabase.from_uri(uri) for name, uri in self.db_uris.items()}

        # Prompts
        self.schema_matching_prompt = ChatPromptTemplate.from_template("""You are an intelligent assistant. Based on the following database schema, check where this user's question belong or related to this schema. Respond strictly with yes or no without any explanations or additional details.
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

        # Initialize the Groq LLM
        self.llm = ChatGroq()

    def get_schema(self, db_name: str) -> str:
        return self.db_connections[db_name].get_table_info()

    def determine_relevant_database(self, question: str) -> Optional[str]:
        """Determines the relevant database for a given question."""
        for db_name in self.db_connections.keys():
            schema = self.get_schema(db_name)
            formatted_prompt = self.schema_matching_prompt.format(schema=schema, question=question)
            response = self.llm.invoke(formatted_prompt).content.strip().lower()
            normalized_response = response.replace("'", "").rstrip('.').strip()
            if normalized_response == "yes":
                return db_name
        return None

    def generate_sql_query(self, schema: str, question: str) -> str:
        """Generates an SQL query based on the provided schema and question."""
        formatted_sql_prompt = self.generate_sql_prompt.format(schema=schema, question=question)
        return self.llm.invoke(formatted_sql_prompt).content.strip()

    def execute_query(self, database: str, query: str) -> Any:
        """Executes the SQL query on the specified database."""
        try:
            return self.db_connections[database].run(query)
        except Exception as e:
            return str(e)

    def format_query_result(self, query_result: Any) -> str:
        """Formats the query result into a clean table format."""
        formatted_visualization_prompt = self.visualization_prompt.format(query_result=query_result)
        return self.llm.invoke(formatted_visualization_prompt).content

    def pipe(self, user_question: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Processes the user question and returns the selected database, generated query, and query result."""
        print(f"Received message from user: {user_question}")

        # Step 1: Determine the relevant database
        selected_database = self.determine_relevant_database(user_question)

        if not selected_database:
            return {"error": "No matching database found for the question."}

        # Step 2: Generate the SQL query
        schema = self.get_schema(selected_database)
        generated_sql = self.generate_sql_query(schema, user_question)

        # Step 3: Execute the SQL query
        query_result = self.execute_query(selected_database, generated_sql)

        if isinstance(query_result, str) and "Error" in query_result:
            return {"error": f"Error executing query: {query_result}"}

        # Step 4: Format the query result
        formatted_result = self.format_query_result(query_result)

        return {
            "selected_database": selected_database,
            "generated_sql": generated_sql,
            "query_result": formatted_result
        }

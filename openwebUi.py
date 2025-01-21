import sqlalchemy
from sqlalchemy import create_engine

class Pipeline:
    def __init__(self):
        self.name = "00 Repeater Example"

    async def on_startup(self):
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        print(f"on_shutdown:{__name__}")
        pass

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        print(f"received message from user: {user_message}")
        
        # If the user asks for the schema
        if user_message.lower() == "get database schema":
            db_uri = "mysql+pymysql://user:password@localhost/dbname"
            schema = self.get_schema(db_uri)
            return schema
        
        return f"received message from user: {user_message}"

    # Function to get schema information
    def get_schema(self, db_uri: str):
        engine = create_engine(db_uri)
        with engine.connect() as connection:
            query = "SHOW TABLES;"
            tables = connection.execute(query).fetchall()

            schema_info = {}
            for (table_name,) in tables:
                schema_query = f"SHOW COLUMNS FROM {table_name};"
                columns = connection.execute(schema_query).fetchall()
                schema_info[table_name] = [column[0] for column in columns]

            return schema_info

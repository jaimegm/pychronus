from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from jinja2 import Template
import pandas as pd

class DBManager(BaseHook):
    def __init__(self, df: pd.DataFrame, tablename: str, schema: str = "public", conn_id="postgres"):        
        self.tablename = tablename
        self.conn_id = conn_id
        self.schema = schema
        self._engine = None
        self.df = df
        
    @property
    def engine(self):
        conn = self.get_conn()
        if self._engine is None:
            db_url = f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:5432/cryptos'
            self._engine = create_engine(db_url)
        return self._engine
    
    def get_conn(self) -> Connection:
        return self.get_connection(self.conn_id)
    
    def query(self, query):
        return pd.read_sql_query(sql=query, con=self.engine)
    
    def table_exists(self):
        query = Template("""
        SELECT EXISTS(
        SELECT * FROM information_schema.tables 
        WHERE 
        table_schema = '{{ schema }}' AND 
        table_name = '{{ tablename }}' );
        """).render(
            {"schema": self.schema, 
             "tablename": self.tablename
            })
        return self.query(query).iloc[0][0]

    def get_last_updated_at(self):
        query = Template("""
        SELECT MAX({{ updated_at }})
        FROM {{ schema }}."{{ tablename }}"
        """).render({
        "schema": self.schema, 
        "tablename": self.tablename,
        "updated_at": "open_time"
        })
        return self.query(query).iloc[0][0]
    
    def list_schemas(self):
        query = """SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name not in ('pg_toast', 'pg_temp_1', 
        'pg_toast_temp_1', 'pg_catalog', 
        'information_schema', 'public')"""
        return self.query(query)
        
    def upload(self, df):
        self.df.to_sql(
            name=self.tablename,
            con=self.engine,
            schema=self.schema,
            if_exists='replace',
            chunksize=1000,
            index=False,
            index_label=None
            )
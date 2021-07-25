from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from jinja2 import Template
import pandas as pd


class DB_Manager(BaseHook):
    def __init__(self, conn_id="pythos_db"):
        self.conn_id = conn_id
        self._engine = None
        self.schema = "public"
        self.table_name = "ethusdt_4h"
        
    def get_conn(self):
        return self.get_connection(self.conn_id)

    @property
    def engine(self):
        conn = self.get_conn()
        if self._engine is None:
            db_url = f'postgresql+psycopg2://{conn.login}:{conn.password}@127.0.0.1:5432/cryptos'
            self._engine = create_engine(db_url)
        return self._engine

    def table_exists(self):
        query = Template("""
        SELECT EXISTS(
        SELECT * FROM information_schema.tables 
        WHERE 
        table_schema = '{{ schema }}' AND 
        table_name = '{{ table_name }}' );
        """).render(
            {"schema": self.schema, 
             "table_name": self.table_name
            })
        return pd.read_sql_query(
            sql=query, 
            con=self.engine,
        ).iloc[0][0]
        
    
    def upload(self, df):
        df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='replace',
            chunksize=1000,
            index=False,
            index_label=None
            )

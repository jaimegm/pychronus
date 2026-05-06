import io
from typing import Any, Iterator, Optional

import pandas as pd
import psycopg2
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from jinja2 import Template
from sqlalchemy import create_engine


# https://stackoverflow.com/questions/12593576/adapt-an-iterator-to-behave-like-a-file-like-object-in-python/12604375#12604375
class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ""

    def readable(self) -> bool:
        return True

    def _preread(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret) :]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._preread()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._preread(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return "".join(line)


class PostgresHook(BaseHook):
    def __init__(
        self,
        database: str,
        conn_id: str = "Alexandria",
    ):
        self.database = database
        self.conn_id = conn_id
        self._engine = None

    def get_conn(self) -> Connection:
        return self.get_connection(self.conn_id)

    @property
    def engine(self):
        conn = self.get_conn()
        if self._engine is None:
            db_url = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{self.database}"
            self._engine = create_engine(db_url)
        return self._engine

    @property
    def postgres_conn(self):
        if not self._postgres_conn:
            conn = self.get_conn()
            self._postgres_conn = psycopg2.connect(
                database=self.database,
                password=conn.password,
                user=conn.login,
                host=conn.host,
                port=conn.port,
            )
            self._postgres_conn.autocommit = True
        return self._postgres_conn

    @staticmethod
    def translate_dtype(dtype: str):
        return {
            "int64": "INTEGER",
            "float64": "FLOAT64",
            "bool": "BOOLEAN",
            "datetime64[ns]": "DATE",
        }[dtype]

    # def make_bigquery_schema(self, df: pd.DataFrame, lowercase: bool = False) -> List:
    #     """
    #     interprets schema from dataframe and returns bigquery schema as list
    #     :param df: source dataframe
    #     :param lowercase: boolean whether field names should be made lowercase
    #     :return: list of bigquery compliant fields in list
    #     """
    #     schema = []
    #     replacements = [
    #         (".", ""),
    #         ("&", ""),
    #         (" ", "_"),
    #         ("(", ""),
    #         ("ß", "ss"),
    #         ("Ä", "AE"),
    #         ("Ö", "OE"),
    #         ("Ü", "UE"),
    #         ("%", ""),
    #         ("É", "E"),
    #         ("È", "E"),
    #         ("ö", "OE"),
    #         (")", ""),
    #         ("#", "count"),
    #         ("!", ""),
    #         ("'", ""),
    #         ("/", ""),
    #         ("=", ""),
    #         ("-", "_"),
    #         ("_/t_", ""),
    #         ("/n", ""),
    #         ("\n", ""),
    #         ("?", ""),
    #         ("[", ""),
    #         ("]", ""),
    #         ("__", "_"),
    #     ]
    #     x = 1
    #     for name in df.columns:
    #         form = str(df.dtypes[name])
    #         cast = self.translate_dtype(form)
    #         if name is None or name == "":
    #             name = f"no_field_name{x}"
    #             x = x + 1
    #         for bad_char, good_char in replacements:
    #             name = name.replace(bad_char, good_char).strip()
    #         if name[0] in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
    #             name = "_" + name
    #         if lowercase:
    #             name = name.lower()
    #         line = bigquery.SchemaField(name, cast, mode="NULLABLE")
    #         schema.append(line)
    #
    #     logging.info("generated schema")
    #
    #     return schema
    # def initialize_staging_table(self) -> None:
    #     logging.info(f"Initilized Postgres Table: {self.staging_table}")
    #     with self.postgres_conn.cursor() as cursor:
    #         cursor.execute(f"""
    #             DROP TABLE IF EXISTS {self.staging_table};
    #             CREATE TABLE {self.staging_table} (
    #                 # id                  TEXT,
    #                 # created_at          TIMESTAMP,
    #                 # reader_count        INTEGER,
    #                 # label               TEXT
    #             );
    #         """)

    @staticmethod
    def clean_csv_value(value: Optional[Any]) -> str:
        if value is None:
            return r"\N"
        return str(value).replace("\n", "\\n")

    def query(self, query):
        return pd.read_sql_query(sql=query, con=self.engine)

    def table_exists(self, table_schema):
        self.log.info(f"Checking for {table_schema}")
        query = Template(
            """
        SELECT EXISTS(
        SELECT * FROM information_schema.tables
        WHERE
        table_schema = '{{ schema }}' AND
        table_name = '{{ tablename }}' );
        """
        ).render(
            {
                "schema": table_schema.split(".")[0],
                "tablename": table_schema.split(".")[-1],
            }
        )
        return self.query(query).iloc[0][0]

    def get_last_updated_at(self, table_schema, updated_at):
        self.log.info(f"Extracting Last Updated at: {updated_at}")
        query = Template(
            """
        SELECT MAX({{ updated_at }})
        FROM {{ schema }}."{{ tablename }}"
        """
        ).render(
            {
                "schema": table_schema.split(".")[0],
                "tablename": table_schema.split(".")[-1],
                "updated_at": updated_at,
            }
        )
        return self.query(query).iloc[0][0]

    def list_schemas(self):
        query = """SELECT schema_name FROM information_schema.schemata
        WHERE schema_name not in ('pg_toast', 'pg_temp_1',
        'pg_toast_temp_1', 'pg_catalog',
        'information_schema', 'public')"""
        return self.query(query)

    def list_tables(self, schema):
        query = Template(
            """SELECT * FROM information_schema.tables
        WHERE table_schema = '{{ schema }}'
        """
        ).render({"schema": schema})
        return self.query(query)

    def upload(self, df: pd.DataFrame):
        df.to_sql(
            name=self.tablename,
            con=self.engine,
            schema=self.schema,
            if_exists="append" if self.table_exists() else "replace",
            chunksize=100000,
            index=False,
            index_label=None,
        )

import logging
import os
from typing import Any, Dict, Union

import pandas as pd
from airflow.models import BaseOperator

from pychronus.hooks.postgres import PostgresHook
from pychronus.utils.pythos import generate_hash_uuid, get_sql_filepath, keyify
from pychronus.utils.vendors import known_vendors

EXCLUDE_COLUMNS = ["memo"]


class FinanceManagerOperator(BaseOperator):
    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super(FinanceManagerOperator, self).__init__(*args, **kwargs)
        self._postgres_hook = None

    @property
    def postgres_hook(self) -> PostgresHook:
        if self._postgres_hook is None:
            self._postgres_hook = PostgresHook(
                database="jaime",
            )
        return self._postgres_hook

    @staticmethod
    def vendor_check(description):
        if description:
            for key in known_vendors.keys():
                if key in str(description).lower():
                    return known_vendors.get(key)
        return False

    @staticmethod
    def determine_bank(filename):
        name = filename.lower()
        if "chase" in name:
            return "chase"
        elif "n26" in name:
            return "n26"
        elif "revolut" in name:
            return "revolut"
        else:
            raise ValueError("Unknown Bank")

    @staticmethod
    def build_transaction_date(df):
        date_cols = [col for col in df.columns if "date" in col.lower()]

        for col in date_cols:
            if "transaction" in col:
                return df
            elif "post" in col:
                df = df.rename({col: "transaction_date"}, axis=1)
                return df
            else:
                raise ValueError("Unknown Date Column")

    @staticmethod
    def determine_transaction_type(filename):
        cleaned_filename = filename.lower()
        if "chase0852" in cleaned_filename:
            return "credit card"
        elif "chase3406" in cleaned_filename:
            return "credit card"
        else:
            raise ValueError(
                "Card type not defined. Check `determine_transaction_type`"
                f"Check filename: {cleaned_filename}"
            )

    @staticmethod
    def clean_dataframe(df: pd.DataFrame):
        for col in df.columns:
            if col in EXCLUDE_COLUMNS:
                df = df.drop([col], axis=1)
        return df

    @staticmethod
    def read_finance_file(filename):
        encodings = ["utf-8", "latin1", "iso-8859-1", "cp1252"]
        for enc in encodings:
            try:
                df = pd.read_csv(filename, encoding=enc)
                return df
            except UnicodeDecodeError as e:
                print(f"Failed to read the file with {enc} encoding: {e}")

    def process_transaction_file(self):
        os.chdir("/Users/jaime/finances")
        files = os.listdir()
        for file in files:
            df = self.read_finance_file(file)
            df.columns = [keyify(column) for column in df.columns]
            logging.info(df.columns)
            df = self.build_transaction_date(df)
            df["vendor"] = df["description"].apply(lambda x: self.vendor_check(x))
            df["bank"] = self.determine_bank(file)
            df["type"] = self.determine_transaction_type(file)
            df["transaction_uuid"] = df.apply(
                lambda row: generate_hash_uuid(
                    str(row["transaction_date"])
                    + str(row["description"])
                    + str(row["bank"])
                ),
                axis=1,
            )
            df = self.clean_dataframe(df)
            df.to_sql(
                name="transactions",
                con=self.postgres_hook.engine,
                schema="raw",
                if_exists="replace",
                chunksize=100000,
                index=False,
                index_label=None,
            )
            self.postgres_hook.query(get_sql_filepath("insert_transaction_data.sql"))

    def execute(self, context: Dict[str, Any]) -> Union[None, bool]:
        self.process_transaction_file()
        return "Success!"

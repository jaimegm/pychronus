# import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from pychronus.hooks.dbmanager import DBManager
from pychronus.hooks.pytrader import PyTrader


class CryptoDataCollector(BaseOperator):
    @apply_defaults
    def __init__(self, pair: str, interval: str, *args, **kwargs):
        super(CryptoDataCollector, self).__init__(*args, **kwargs)
        self._dbmanager = None
        self._pytrader = None
        self.interval = interval
        self.pair = pair
        self.tablename = f"{self.pair.lower()}_{self.interval}"
        self.schema = interval

    @property
    def dbmanager(self) -> DBManager:
        if self._dbmanager is None:
            self._dbmanager = DBManager(tablename=self.tablename, schema=self.interval)
        return self._dbmanager

    @property
    def pytrader(self) -> PyTrader:
        if self._pytrader is None:
            self._pytrader = PyTrader(pair=self.pair, interval=self.interval)
        return self._pytrader

    def execute(self):
        if not self.dbmanager.table_exists():
            print(self.schema)
        elif self.dbmanager.table_exists():
            self.dbmanager.upload()

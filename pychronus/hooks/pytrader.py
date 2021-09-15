import json
import logging
from datetime import datetime

import pandas as pd
from binance.client import Client

from pychronus.hooks.dbmanager import DBManager

credential_path = "/root/creds/binance.json"
archive_location = "/Users/jaime/code/projects/crypto"

interested_cryptos = [
    "USDT",
    "EUR",
    "USDC",
    "BUSD",
    "BTC",
    "ETH",
    "DOT",
    "BNB",
    "MATIC",
    "UNI",
    "YFI",
    "ADA",
    "EGLD",
]


class PyTrader:
    def __init__(self, pair):
        self._dbmanager = None
        self._client = None
        self.creds = None
        self.pair = pair
        self.fiats = ["USD", "EUR", "USDT", "USDC"]

    @property
    def client(self):
        if self._client is None:
            logging.info("Building Client...")
            key = json.loads(open(credential_path, "r").read())
            self._client = Client(key["client_id"], key["secret"])
        return self._client

    @property
    def dbmanager(self):
        if self._dbmanager is None:
            self._dbmanager = DBManager(tablename="1h", schema=self.pair.lower())
        return self._dbmanager

    def get_pairs(self):
        coins = self.client.get_exchange_info()
        coin_df = pd.DataFrame(coins["symbols"])
        # Only Spot Trading Pairs
        coin_df = coin_df[coin_df["isSpotTradingAllowed"] == True]
        # Important Fields
        coin_df = coin_df[["status", "symbol", "baseAsset", "quoteAsset"]]
        # Rename Fields
        col_names = {"symbol": "pair", "baseAsset": "base", "quoteAsset": "target"}
        coin_df = coin_df.rename(columns=col_names)
        # Exclude Pairs I dont use
        coin_df = coin_df[coin_df["base"].isin(interested_cryptos)]
        coin_df = coin_df[coin_df["target"].isin(interested_cryptos)]
        return coin_df

    def determine_interval(self, interval):
        try:
            return {
                "1h": self.client.KLINE_INTERVAL_1HOUR,
                "2h": self.client.KLINE_INTERVAL_2HOUR,
                "4h": self.client.KLINE_INTERVAL_4HOUR,
                "5m": self.client.KLINE_INTERVAL_5MINUTE,
                "15m": self.client.KLINE_INTERVAL_15MINUTE,
                "30m": self.client.KLINE_INTERVAL_30MINUTE,
                "1m": self.client.KLINE_INTERVAL_1MINUTE,
            }[interval]
        except KeyError:
            raise KeyError(f"Check Interval Assignment: {interval}")

    def generate_timeframe(self):
        start = self.dbmanager.get_last_updated_at().strftime("%d %b, %Y")
        end = datetime.utcnow().strftime("%d %b, %Y")
        return f"{start}::{end}"

    def get_candlesticks(self, timeframe, interval="1h"):
        # Ex 1 Apr, 2017::1 Feb, 2021
        intrvs = timeframe.split("::")
        logging.info(f"Extracting {self.pair} Candlesticks: {timeframe}")
        intrvl = self.determine_interval(interval)
        klines = self.client.get_historical_klines(
            self.pair, intrvl, intrvs[0], intrvs[-1]
        )
        logging.info("Converting response to DataFrame")
        col_names = [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_vol",
            "trades",
            "base_asset_vol",
            "quote_asset_vol",
            "ign",
        ]
        df = pd.DataFrame(klines, columns=col_names)
        df["open_time"] = pd.to_datetime(df["open_time"].astype(int), unit="ms")
        df["close_time"] = pd.to_datetime(df["close_time"].astype(int), unit="ms")
        df = df.drop(columns=["ign"])
        df["pair"] = self.pair
        df["interval"] = interval
        return df

    def get_spot(self):
        spot_info = self.client.get_account()["balances"]
        df = pd.DataFrame(spot_info)
        df["free"].astype(str)
        df = df[df["free"] != "0.00000000"]
        df = df[df["free"] != "0.00"]
        df = df.reset_index(drop=True)
        return df

    def get_trades(self):
        trades = self.client.get_my_trades(symbol=self.pair)
        df = pd.DataFrame(trades)
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        return df

    def get_all_orders(self):
        orders = self.client.get_all_orders(symbol=self.pair)
        df = pd.DataFrame(orders)
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df["updateTime"] = pd.to_datetime(df["updateTime"], unit="ms")
        return df

    def get_deposits(self):
        deposits = self.client.get_deposit_history()["depositList"]
        df = pd.DataFrame(deposits)
        df["insertTime"] = pd.to_datetime(df["insertTime"], unit="ms")
        return df

    def get_withdraws(self):
        withdraws = self.client.get_withdraw_history()
        df = pd.DataFrame(withdraws["withdrawList"])
        return df

    def get_address(self, token):
        address = self.client.get_deposit_address(asset=token)
        return address

    def get_open_orders(self):
        orders = self.client.get_open_orders(symbol=self.pair)
        df = pd.DataFrame(orders)
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df["updateTime"] = pd.to_datetime(df["updateTime"], unit="ms")
        return df

    def check_order(self, order_id):
        order_status = self.client.get_order(symbol=self.pair, orderId=order_id)
        return order_status

    def cancel_order(self, order_id):
        order = self.client.cancel_order(symbol=self.pair, orderId=order_id)
        return order

    def test_order(self, side_buy, order_type_limit, time_in_force):
        return self.client.create_test_order(
            symbol=self.pair,
            side=side_buy,
            type=order_type_limit,
            timeInForce=time_in_force,
            quantity=100,
            price="0.00001",
        )

    def create_oco_order(self, side_sell, time_in_force):
        order = self.client.create_oco_order(
            symbol=self.pair,
            side=side_sell,
            stopLimitTimeInForce=time_in_force,
            quantity=100,
            stopPrice="0.00001",
            price="0.00002",
        )
        return order

    def create_oco_buy(self, side_sell, time_in_force):
        """
        {
            "symbol": self.pair,
            # "quantity": quantity,
            # "price": price,
            # 'limitIcebergQty': limit_maker 'Used to make the LIMIT_MAKER leg an iceberg order.',
            # 'stopPrice': stop_price,
            # 'stopIcebergQty': stop_ice_qty #'Used with STOP_LOSS_LIMIT leg to make an iceberg order.',
            "stopLimitPrice": "If provided, stopLimitTimeInForce is required.",
            "stopLimitTimeInForce": "GTC",  # 'Valid values are GTC/FOK/IOC.'
            "newOrderRespType": "JSON",
            "recvWindow": "the number of milliseconds the request is valid for",
        }
        """
        order = self.client.create_oco_order(
            symbol=self.pair,
            side=side_sell,
            stopLimitTimeInForce=time_in_force,
            quantity=100,
            stopPrice="0.00001",
            price="0.00002",
        )

        return order

    def create_oco_sell(self, side_sell, time_in_force):
        """
        {
            "limitIcebergQty": "Used to make the LIMIT_MAKER leg an iceberg order.",
            "stopPrice": "required",
            "stopLimitPrice": "If provided, stopLimitTimeInForce is required.",
            "stopIcebergQty": "Used with STOP_LOSS_LIMIT leg to make an iceberg order.",
            "stopLimitTimeInForce": "Valid values are GTC/FOK/IOC.",
            "newOrderRespType": "JSON",
            "recvWindow": "the number of milliseconds the request is valid for",
        }
        """
        order = self.client.create_oco_order(
            symbol=self.pair,
            side=side_sell,
            stopLimitTimeInForce=time_in_force,
            quantity=100,
            stopPrice="0.00001",
            price="0.00002",
        )
        return order

    def get_lending_products(self):
        data = self.client.get_lending_product_list()
        return pd.DataFrame(data)

    def archive_data(self, df, timeframe):
        naming_convention = self.create_filename(timeframe)
        filename = archive_location + f"/{naming_convention}"
        logging.info(f"Storing: {filename}")
        df.to_csv(filename, header=True, index=False, encoding="utf-8")

    def create_filename(self, timeframe):
        start = timeframe.split("::")[0]
        end = timeframe.split("::")[-1]
        start = self.parse_date(start, "%d %b, %Y")
        end = self.parse_date(end, "%d %b, %Y")
        return f"{self.pair}_{start}_to_{end}.csv"

    def upload(self, df):
        table_name = f"{self.pair}_{self.pair}"
        df.to_sql(
            table_name,
            con=self.dbmanager.engine,
            schema=self.schema,
            if_exists="replace",
            chunksize=1000,
            index=False,
            index_label=None,
        )

    @staticmethod
    def parse_date(date_str, input_format, output_format="%Y_%m_%d"):
        if date_str in [None, "", "nan"]:
            return None
        date_obj = datetime.strptime(date_str, input_format)
        return date_obj.date().strftime(output_format)

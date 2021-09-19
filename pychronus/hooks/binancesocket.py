import json
import logging
import random
from datetime import datetime

from binance import ThreadedWebsocketManager
from binance.client import Client
from twisted.internet import reactor

from pychronus.utils.misc import goodbyes, hellos, managers


class BinanceSocket:
    def __init__(self, pair):
        self.thread_state = {"error": False}
        self.credential_path = "/root/creds/binance.json"
        self.pair = pair
        self._manager = None
        self._client = None
        self.streams = []
        self.data = []
        self.messages = {"created": None, "messages": self.data}

    @property
    def client(self):
        if self._client is None:
            logging.info("Building Client...")
            key = json.loads(open(self.credential_path, "r").read())
            self._client = Client(key["client_id"], key["secret"])
        return self._client

    @property
    def manager(self):
        if self._manager is None:
            logging.info("Building Manager...")
            self._manager = ThreadedWebsocketManager()
            manager = self.rando(managers)
            self._manager.setName(manager)
            logging.info(f"{self.rando(hellos)}, It's {manager}")
        return self._manager

    def message_manager(self):
        # generate new orb
        if self.messages is None or self.messages.get("created") - datetime.now() <= 30:
            self.data = []
            self.messages = {"created": datetime.now(), "messages": self.data}
            return self.messages
        # Re-use
        else:
            return self.messages

    def start(self):
        self.manager.start()

    def stop(self):
        self.manager.stop()

    def message_processor(self, msg):
        """define how to process incoming WebSocket messages"""
        if msg["e"] != "error":
            self.data.append(
                {"best_bid": msg["b"], "best_ask": msg["a"], "close?": msg["c"]}
            )
            self.thread_state["error"] = False
        else:
            self.thread_state["error"] = True

    def socket_stability(self):
        pass
        # if self._manager.is_alive() == False:

    def start_ticker_stream(self):
        stream_name = self.manager.start_symbol_ticker_socket(
            callback=self.messenger, symbol=self.pair
        )
        self.streams.append(stream_name)

    def start_info_socket(self, socket_type):
        if socket_type == "kline":
            self.manager.start_kline_socket(
                symbol=self.pair, callback=self.message_processor
            )
        elif socket_type == "depth":
            self.manager.start_depth_socket(
                symbol=self.pair, callback=self.message_processor
            )
        elif socket_type == "multiplex":
            self.manager.start_multiplex_socket(
                symbol=self.pair, callback=self.message_processor
            )
        elif socket_type == "agg-trade":
            self.manager.start_aggtrade_socket(
                symbol=self.pair, callback=self.message_processor
            )
        elif socket_type == "trade":
            self.manager.start_trade_socket(
                symbol=self.pair, callback=self.message_processor
            )
        elif socket_type == "ticker":
            self.manager.start_ticker_socket(callback=self.message_processor)
        elif socket_type == "mini-ticket":
            # this socket can take an update interval parameter
            # set as 5000 to receive updates every 5 seconds
            self.manager.start_miniticker_socket(self.message_processor, 5000)

    def start_trade_socket(self):
        self.manager.start_user_socket(self.message_processor)

    def kill_switch(self):
        self.manager.close()
        logging.info(f"{self.rando(goodbyes)} ~ {self.manager.getName()}")
        reactor.stop()

    @staticmethod
    def rando(array):
        if type(array) == dict:
            rand = array[str(random.randrange(0, len(array)))]
        elif type(array) == list:
            rand = array[random.randrange(0, len(array))]
        return rand
